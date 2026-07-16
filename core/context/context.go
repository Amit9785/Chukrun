package context

import (
	stdcontext "context"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// PriorityClass defines execution priorities
type PriorityClass string

const (
	PriorityClassCritical   PriorityClass = "Critical"
	PriorityClassHigh       PriorityClass = "High"
	PriorityClassNormal     PriorityClass = "Normal"
	PriorityClassLow        PriorityClass = "Low"
	PriorityClassBackground PriorityClass = "Background"
)

type contextKey string

const (
	keyRuntime contextKey = "runtime"
	keySession contextKey = "session"
	keyUser    contextKey = "user"
	keyRequest contextKey = "request"
)

var (
	compatTraceID     any = "trace_id"
	compatSessionID   any = "session_id"
	compatUserID      any = "user_id"
	compatExecutionID any = "execution_id"
)

type CostBudget struct {
	Limit     float64
	spentBits *uint64 // pointer to uint64 representing float64 bits, shared across derived contexts
	Currency  string
}

func NewCostBudget(limit float64, currency string) *CostBudget {
	var spent uint64
	return &CostBudget{
		Limit:     limit,
		spentBits: &spent,
		Currency:  currency,
	}
}

func (cb *CostBudget) Spent() float64 {
	if cb.spentBits == nil {
		return 0
	}
	return math.Float64frombits(atomic.LoadUint64(cb.spentBits))
}

func (cb *CostBudget) AddSpent(amount float64) {
	if cb.spentBits == nil {
		return
	}
	for {
		oldBits := atomic.LoadUint64(cb.spentBits)
		oldVal := math.Float64frombits(oldBits)
		newVal := oldVal + amount
		newBits := math.Float64bits(newVal)
		if atomic.CompareAndSwapUint64(cb.spentBits, oldBits, newBits) {
			break
		}
	}
}

func (cb *CostBudget) TryReserve(amount float64) bool {
	if cb.spentBits == nil {
		return true
	}
	if cb.Limit <= 0 {
		return true
	}
	return cb.Spent()+amount <= cb.Limit
}

type RuntimeLayer struct {
	InstanceID string
	Version    string
}

type SessionLayer struct {
	SessionID string
	UserID    string
	State     SessionState
}

type UserLayer struct {
	UserID string
	OrgID  string
	Claims map[string]string
}

type RequestLayer struct {
	ExecutionID       string
	ParentExecutionID string
	TraceID           string
	Deadline          time.Time
	Priority          PriorityClass
	CostBudget        *CostBudget
	Attempt           int
	Metadata          map[string]string
}

// Manager and contextManager kept for compatibility in transition if needed
type Manager interface {
	NewRootContext() stdcontext.Context
}

type contextManager struct {
	instanceID string
	version    string
}

func NewManager(instanceID, version string) Manager {
	return &contextManager{
		instanceID: instanceID,
		version:    version,
	}
}

func (m *contextManager) NewRootContext() stdcontext.Context {
	return NewRootContext(m.instanceID, m.version)
}

func NewRootContext(instanceID, version string) stdcontext.Context {
	rl := &RuntimeLayer{
		InstanceID: instanceID,
		Version:    version,
	}
	return stdcontext.WithValue(stdcontext.Background(), keyRuntime, rl)
}

// Derivations
func WithSession(ctx stdcontext.Context, sessionID, userID string) stdcontext.Context {
	sl := &SessionLayer{
		SessionID: sessionID,
		UserID:    userID,
		State:     GetSessionStore().GetOrCreate(sessionID),
	}
	ctx = stdcontext.WithValue(ctx, keySession, sl)
	ctx = stdcontext.WithValue(ctx, compatSessionID, sessionID)
	if userID != "" {
		ctx = stdcontext.WithValue(ctx, compatUserID, userID)
	}
	return ctx
}

func WithUser(ctx stdcontext.Context, userID, orgID string, claims map[string]string) stdcontext.Context {
	filteredClaims := make(map[string]string)
	for k, v := range claims {
		if k != "token" && k != "password" && k != "secret" && k != "jwt" {
			filteredClaims[k] = v
		}
	}
	ul := &UserLayer{
		UserID: userID,
		OrgID:  orgID,
		Claims: filteredClaims,
	}
	ctx = stdcontext.WithValue(ctx, keyUser, ul)
	ctx = stdcontext.WithValue(ctx, compatUserID, userID)
	return ctx
}

func WithExecution(ctx stdcontext.Context, executionID string, deadline time.Duration, priority PriorityClass) stdcontext.Context {
	var requestedDeadline time.Time
	var cancel stdcontext.CancelFunc

	if deadline > 0 {
		requestedDeadline = time.Now().Add(deadline)
		ctx, cancel = stdcontext.WithDeadline(ctx, requestedDeadline)
	} else {
		ctx, cancel = stdcontext.WithCancel(ctx)
	}
	_ = cancel

	traceID := GetTraceID(ctx)
	if traceID == "" {
		traceID = fmt.Sprintf("tr-%s", executionID)
	}

	req := &RequestLayer{
		ExecutionID:       executionID,
		ParentExecutionID: GetExecutionID(ctx),
		TraceID:           traceID,
		Deadline:          requestedDeadline,
		Priority:          priority,
		Attempt:           1,
		Metadata:          make(map[string]string),
	}

	if parentReq := getRequestLayer(ctx); parentReq != nil {
		req.CostBudget = parentReq.CostBudget
		for k, v := range parentReq.Metadata {
			req.Metadata[k] = v
		}
	}

	ctx = stdcontext.WithValue(ctx, keyRequest, req)
	ctx = stdcontext.WithValue(ctx, compatTraceID, traceID)
	ctx = stdcontext.WithValue(ctx, compatExecutionID, executionID)
	return ctx
}

func WithChildExecution(ctx stdcontext.Context, childExecutionID string, timeoutOverride *time.Duration) stdcontext.Context {
	var childDeadline time.Time
	var hasDeadline bool

	if timeoutOverride != nil {
		childDeadline = time.Now().Add(*timeoutOverride)
		hasDeadline = true
		if pDeadline, ok := ctx.Deadline(); ok {
			if pDeadline.Before(childDeadline) {
				childDeadline = pDeadline
			}
		}
	} else {
		childDeadline, hasDeadline = ctx.Deadline()
	}

	var cancel stdcontext.CancelFunc
	if hasDeadline {
		ctx, cancel = stdcontext.WithDeadline(ctx, childDeadline)
	} else {
		ctx, cancel = stdcontext.WithCancel(ctx)
	}
	_ = cancel

	parentExecID := GetExecutionID(ctx)

	req := &RequestLayer{
		ExecutionID:       childExecutionID,
		ParentExecutionID: parentExecID,
		TraceID:           GetTraceID(ctx),
		Deadline:          childDeadline,
		Attempt:           1,
		Metadata:          make(map[string]string),
	}

	if parentReq := getRequestLayer(ctx); parentReq != nil {
		req.Priority = parentReq.Priority
		req.CostBudget = parentReq.CostBudget
		for k, v := range parentReq.Metadata {
			req.Metadata[k] = v
		}
	}

	ctx = stdcontext.WithValue(ctx, keyRequest, req)
	ctx = stdcontext.WithValue(ctx, compatExecutionID, childExecutionID)
	return ctx
}

func WithAttempt(ctx stdcontext.Context, attemptNumber int) stdcontext.Context {
	parentReq := getRequestLayer(ctx)
	if parentReq == nil {
		return ctx
	}
	req := &RequestLayer{
		ExecutionID:       parentReq.ExecutionID,
		ParentExecutionID: parentReq.ParentExecutionID,
		TraceID:           parentReq.TraceID,
		Deadline:          parentReq.Deadline,
		Priority:          parentReq.Priority,
		CostBudget:        parentReq.CostBudget,
		Attempt:           attemptNumber,
		Metadata:          make(map[string]string),
	}
	for k, v := range parentReq.Metadata {
		req.Metadata[k] = v
	}
	return stdcontext.WithValue(ctx, keyRequest, req)
}

func WithMetadata(ctx stdcontext.Context, key, value string) (stdcontext.Context, error) {
	parentReq := getRequestLayer(ctx)
	currentSize := 0
	if parentReq != nil && parentReq.Metadata != nil {
		for k, v := range parentReq.Metadata {
			currentSize += len(k) + len(v)
		}
	}

	newEntrySize := len(key) + len(value)
	if parentReq != nil && parentReq.Metadata != nil {
		if val, exists := parentReq.Metadata[key]; exists {
			currentSize -= len(key) + len(val)
		}
	}

	if currentSize+newEntrySize > 4096 {
		return ctx, fmt.Errorf("metadata size limit exceeded")
	}

	req := &RequestLayer{
		Metadata: make(map[string]string),
	}
	if parentReq != nil {
		req.ExecutionID = parentReq.ExecutionID
		req.ParentExecutionID = parentReq.ParentExecutionID
		req.TraceID = parentReq.TraceID
		req.Deadline = parentReq.Deadline
		req.Priority = parentReq.Priority
		req.CostBudget = parentReq.CostBudget
		req.Attempt = parentReq.Attempt
		for k, v := range parentReq.Metadata {
			req.Metadata[k] = v
		}
	}
	req.Metadata[key] = value

	return stdcontext.WithValue(ctx, keyRequest, req), nil
}

func WithCostBudget(ctx stdcontext.Context, budget CostBudget) stdcontext.Context {
	parentReq := getRequestLayer(ctx)
	req := &RequestLayer{
		Metadata: make(map[string]string),
	}
	if parentReq != nil {
		req.ExecutionID = parentReq.ExecutionID
		req.ParentExecutionID = parentReq.ParentExecutionID
		req.TraceID = parentReq.TraceID
		req.Deadline = parentReq.Deadline
		req.Priority = parentReq.Priority
		req.Attempt = parentReq.Attempt
		for k, v := range parentReq.Metadata {
			req.Metadata[k] = v
		}
	}
	req.CostBudget = &budget
	return stdcontext.WithValue(ctx, keyRequest, req)
}

type sensitiveKeysKey struct{}

func WithSensitiveKey(ctx stdcontext.Context, key string) stdcontext.Context {
	keys, _ := ctx.Value(sensitiveKeysKey{}).(map[string]bool)
	newKeys := make(map[string]bool)
	for k, v := range keys {
		newKeys[k] = v
	}
	newKeys[key] = true
	return stdcontext.WithValue(ctx, sensitiveKeysKey{}, newKeys)
}

func IsSensitiveKey(ctx stdcontext.Context, key string) bool {
	if ctx == nil {
		return false
	}
	keys, ok := ctx.Value(sensitiveKeysKey{}).(map[string]bool)
	if !ok {
		return false
	}
	return keys[key]
}

func WithSensitiveVariable(ctx stdcontext.Context, key string, value any) stdcontext.Context {
	valStr := fmt.Sprintf("%v", value)
	ctx, _ = WithMetadata(ctx, key, valStr)
	return WithSensitiveKey(ctx, key)
}


// Internal Accessor
func getRequestLayer(ctx stdcontext.Context) *RequestLayer {
	if ctx == nil {
		return nil
	}
	if val := ctx.Value(keyRequest); val != nil {
		if req, ok := val.(*RequestLayer); ok {
			return req
		}
	}
	return nil
}

// Public Accessors
func GetRuntime(ctx stdcontext.Context) *RuntimeLayer {
	if ctx == nil {
		return nil
	}
	if val := ctx.Value(keyRuntime); val != nil {
		if rl, ok := val.(*RuntimeLayer); ok {
			return rl
		}
	}
	return nil
}

func GetSessionID(ctx stdcontext.Context) string {
	if ctx == nil {
		return ""
	}
	if val := ctx.Value(keySession); val != nil {
		if sl, ok := val.(*SessionLayer); ok {
			return sl.SessionID
		}
	}
	if val, ok := ctx.Value("session_id").(string); ok {
		return val
	}
	return ""
}

func GetUserLayer(ctx stdcontext.Context) *UserLayer {
	if ctx == nil {
		return nil
	}
	if val := ctx.Value(keyUser); val != nil {
		if ul, ok := val.(*UserLayer); ok {
			return ul
		}
	}
	return nil
}

func GetUserID(ctx stdcontext.Context) string {
	if ctx == nil {
		return ""
	}
	if ul := GetUserLayer(ctx); ul != nil {
		return ul.UserID
	}
	if val := ctx.Value(keySession); val != nil {
		if sl, ok := val.(*SessionLayer); ok {
			return sl.UserID
		}
	}
	if val, ok := ctx.Value("user_id").(string); ok {
		return val
	}
	return ""
}

func GetTraceID(ctx stdcontext.Context) string {
	if ctx == nil {
		return ""
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.TraceID
	}
	if val, ok := ctx.Value("trace_id").(string); ok {
		return val
	}
	return ""
}

func GetCostBudget(ctx stdcontext.Context) *CostBudget {
	if ctx == nil {
		return nil
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.CostBudget
	}
	return nil
}

func GetMetadata(ctx stdcontext.Context, key string) (string, bool) {
	if ctx == nil {
		return "", false
	}
	if req := getRequestLayer(ctx); req != nil && req.Metadata != nil {
		val, ok := req.Metadata[key]
		return val, ok
	}
	return "", false
}

func GetAttemptNumber(ctx stdcontext.Context) int {
	if ctx == nil {
		return 1
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.Attempt
	}
	return 1
}

func GetExecutionID(ctx stdcontext.Context) string {
	if ctx == nil {
		return ""
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.ExecutionID
	}
	if val, ok := ctx.Value("execution_id").(string); ok {
		return val
	}
	return ""
}

func GetParentExecutionID(ctx stdcontext.Context) string {
	if ctx == nil {
		return ""
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.ParentExecutionID
	}
	return ""
}

func GetPriority(ctx stdcontext.Context) PriorityClass {
	if ctx == nil {
		return PriorityClassNormal
	}
	if req := getRequestLayer(ctx); req != nil {
		return req.Priority
	}
	return PriorityClassNormal
}

func GetSession(ctx stdcontext.Context) SessionState {
	if ctx == nil {
		return nil
	}
	if val := ctx.Value(keySession); val != nil {
		if sl, ok := val.(*SessionLayer); ok {
			return sl.State
		}
	}
	return nil
}

// Merging API
type ConflictStrategy int

const (
	OverlayWins ConflictStrategy = iota
	BaseWins
	ErrorOnConflict
)

type MergeRules struct {
	OnConflict ConflictStrategy
}

var ErrCrossRuntimeMerge = fmt.Errorf("cross-runtime merge rejected")

func Merge(base, overlay stdcontext.Context, rules MergeRules) (stdcontext.Context, error) {
	baseRL := GetRuntime(base)
	overlayRL := GetRuntime(overlay)
	if baseRL != nil && overlayRL != nil && baseRL.InstanceID != overlayRL.InstanceID {
		return nil, ErrCrossRuntimeMerge
	}

	var err error
	mergedCtx := base

	mergedCtx, err = mergeSession(mergedCtx, base, overlay, rules)
	if err != nil {
		return nil, err
	}

	mergedCtx, err = mergeUser(mergedCtx, base, overlay, rules)
	if err != nil {
		return nil, err
	}

	mergedCtx, err = mergeRequest(mergedCtx, base, overlay, rules)
	if err != nil {
		return nil, err
	}

	return mergedCtx, nil
}

func mergeSession(mergedCtx stdcontext.Context, base, overlay stdcontext.Context, rules MergeRules) (stdcontext.Context, error) {
	baseSL := base.Value(keySession)
	overlaySL := overlay.Value(keySession)
	if overlaySL == nil {
		return mergedCtx, nil
	}

	if baseSL == nil {
		mergedCtx = stdcontext.WithValue(mergedCtx, keySession, overlaySL)
		if sl, ok := overlaySL.(*SessionLayer); ok {
			mergedCtx = stdcontext.WithValue(mergedCtx, compatSessionID, sl.SessionID)
			mergedCtx = stdcontext.WithValue(mergedCtx, compatUserID, sl.UserID)
		}
		return mergedCtx, nil
	}

	baseState := GetSession(base)
	overlayState := GetSession(overlay)
	if baseState == nil || overlayState == nil {
		return mergedCtx, nil
	}

	_, isBaseSession := baseState.(*sessionState)
	oState, isOverlaySession := overlayState.(*sessionState)
	if !isBaseSession || !isOverlaySession {
		return mergedCtx, nil
	}

	if err := mergeSessionKeys(baseState, overlayState, oState, rules); err != nil {
		return nil, err
	}

	return mergedCtx, nil
}

func mergeSessionKeys(baseState, overlayState SessionState, oState *sessionState, rules MergeRules) error {
	oState.mu.RLock()
	keys := make([]string, 0, len(oState.vars))
	for k := range oState.vars {
		keys = append(keys, k)
	}
	oState.mu.RUnlock()

	for _, k := range keys {
		oVal, _ := overlayState.Get(k)
		_, exists := baseState.Get(k)
		if exists {
			switch rules.OnConflict {
			case ErrorOnConflict:
				return fmt.Errorf("conflict on session key: %s", k)
			case OverlayWins:
				baseState.Set(k, oVal)
			case BaseWins:
				// Do nothing
			}
		} else {
			baseState.Set(k, oVal)
		}
	}
	return nil
}

func mergeUser(mergedCtx stdcontext.Context, base, overlay stdcontext.Context, rules MergeRules) (stdcontext.Context, error) {
	baseUL := base.Value(keyUser)
	overlayUL := overlay.Value(keyUser)
	if overlayUL == nil {
		return mergedCtx, nil
	}

	if baseUL == nil {
		mergedCtx = stdcontext.WithValue(mergedCtx, keyUser, overlayUL)
		if ul, ok := overlayUL.(*UserLayer); ok {
			mergedCtx = stdcontext.WithValue(mergedCtx, compatUserID, ul.UserID)
		}
		return mergedCtx, nil
	}

	bUl := baseUL.(*UserLayer)
	oUl := overlayUL.(*UserLayer)
	var mergedUser *UserLayer

	switch rules.OnConflict {
	case ErrorOnConflict:
		return nil, fmt.Errorf("conflict on user layer")
	case OverlayWins:
		mergedUser = oUl
	case BaseWins:
		mergedUser = bUl
	}

	mergedCtx = stdcontext.WithValue(mergedCtx, keyUser, mergedUser)
	mergedCtx = stdcontext.WithValue(mergedCtx, compatUserID, mergedUser.UserID)
	return mergedCtx, nil
}

func mergeRequest(mergedCtx stdcontext.Context, base, overlay stdcontext.Context, rules MergeRules) (stdcontext.Context, error) {
	baseReq := getRequestLayer(base)
	overlayReq := getRequestLayer(overlay)
	if overlayReq == nil {
		return mergedCtx, nil
	}

	if baseReq == nil {
		mergedCtx = stdcontext.WithValue(mergedCtx, keyRequest, overlayReq)
		mergedCtx = stdcontext.WithValue(mergedCtx, compatTraceID, overlayReq.TraceID)
		mergedCtx = stdcontext.WithValue(mergedCtx, compatExecutionID, overlayReq.ExecutionID)
		return mergedCtx, nil
	}

	mergedReq := &RequestLayer{
		ExecutionID:       baseReq.ExecutionID,
		ParentExecutionID: baseReq.ParentExecutionID,
		TraceID:           baseReq.TraceID,
		Deadline:          baseReq.Deadline,
		Priority:          baseReq.Priority,
		CostBudget:        baseReq.CostBudget,
		Attempt:           baseReq.Attempt,
		Metadata:          make(map[string]string),
	}

	if err := mergeRequestFields(mergedReq, baseReq, overlayReq, rules); err != nil {
		return nil, err
	}

	if err := mergeRequestMetadata(mergedReq, baseReq, overlayReq, rules); err != nil {
		return nil, err
	}

	mergedCtx = stdcontext.WithValue(mergedCtx, keyRequest, mergedReq)
	mergedCtx = stdcontext.WithValue(mergedCtx, compatTraceID, mergedReq.TraceID)
	mergedCtx = stdcontext.WithValue(mergedCtx, compatExecutionID, mergedReq.ExecutionID)
	return mergedCtx, nil
}

func mergeRequestFields(mergedReq *RequestLayer, baseReq, overlayReq *RequestLayer, rules MergeRules) error {
	switch rules.OnConflict {
	case ErrorOnConflict:
		if overlayReq.ExecutionID != "" && overlayReq.ExecutionID != baseReq.ExecutionID {
			return fmt.Errorf("conflict on execution_id")
		}
	case OverlayWins:
		applyOverlayFields(mergedReq, overlayReq)
	case BaseWins:
		// Do nothing
	}
	return nil
}

func applyOverlayFields(mergedReq *RequestLayer, overlayReq *RequestLayer) {
	if overlayReq.ExecutionID != "" {
		mergedReq.ExecutionID = overlayReq.ExecutionID
	}
	if overlayReq.ParentExecutionID != "" {
		mergedReq.ParentExecutionID = overlayReq.ParentExecutionID
	}
	if overlayReq.TraceID != "" {
		mergedReq.TraceID = overlayReq.TraceID
	}
	if !overlayReq.Deadline.IsZero() {
		mergedReq.Deadline = overlayReq.Deadline
	}
	if overlayReq.Priority != "" {
		mergedReq.Priority = overlayReq.Priority
	}
	if overlayReq.CostBudget != nil {
		mergedReq.CostBudget = overlayReq.CostBudget
	}
	if overlayReq.Attempt > 0 {
		mergedReq.Attempt = overlayReq.Attempt
	}
}

func mergeRequestMetadata(mergedReq *RequestLayer, baseReq, overlayReq *RequestLayer, rules MergeRules) error {
	for k, v := range baseReq.Metadata {
		mergedReq.Metadata[k] = v
	}

	for k, v := range overlayReq.Metadata {
		if _, exists := mergedReq.Metadata[k]; exists {
			switch rules.OnConflict {
			case ErrorOnConflict:
				return fmt.Errorf("conflict on metadata key: %s", k)
			case OverlayWins:
				mergedReq.Metadata[k] = v
			case BaseWins:
				// Do nothing
			}
		} else {
			mergedReq.Metadata[k] = v
		}
	}
	return nil
}
