package context

import (
	stdcontext "context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"chukrun/runtime/kernel"
)

type contextKey string

const keyCustomContext contextKey = "custom_context"

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
	Priority          kernel.PriorityClass
	CostBudget        *CostBudget
	Attempt           int
	Metadata          map[string]string
}

type Context struct {
	std      stdcontext.Context
	runtime  *RuntimeLayer
	session  *SessionLayer
	user     *UserLayer
	request  *RequestLayer
	parent   *Context
}

func newContext(std stdcontext.Context, runtime *RuntimeLayer, session *SessionLayer, user *UserLayer, request *RequestLayer, parent *Context) *Context {
	c := &Context{
		runtime: runtime,
		session: session,
		user:    user,
		request: request,
		parent:  parent,
	}
	c.std = stdcontext.WithValue(std, keyCustomContext, c)
	return c
}

// FromStdContext extracts the custom Context from a standard context.Context
func FromStdContext(std stdcontext.Context) (*Context, bool) {
	if std == nil {
		return nil, false
	}
	if c, ok := std.Value(keyCustomContext).(*Context); ok {
		return c, true
	}
	return nil, false
}

// standard context.Context interface implementation
func (c *Context) Deadline() (time.Time, bool) {
	return c.std.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.std.Done()
}

func (c *Context) Err() error {
	return c.std.Err()
}

func (c *Context) Value(key any) any {
	if key == keyCustomContext {
		return c
	}
	return c.std.Value(key)
}

// Read accessors
func (c *Context) ExecutionID() string {
	if c.request != nil {
		return c.request.ExecutionID
	}
	return ""
}

func (c *Context) SessionID() string {
	if c.session != nil {
		return c.session.SessionID
	}
	return ""
}

func (c *Context) UserID() string {
	if c.user != nil {
		return c.user.UserID
	}
	if c.session != nil {
		return c.session.UserID
	}
	return ""
}

func (c *Context) TraceID() string {
	if c.request != nil {
		return c.request.TraceID
	}
	return ""
}

func (c *Context) CostBudgetRemaining() *CostBudget {
	if c.request != nil {
		return c.request.CostBudget
	}
	return nil
}

func (c *Context) Metadata(key string) (string, bool) {
	if c.request != nil && c.request.Metadata != nil {
		val, ok := c.request.Metadata[key]
		return val, ok
	}
	return "", false
}

func (c *Context) StdContext() stdcontext.Context {
	return c.std
}

func (c *Context) AttemptNumber() int {
	if c.request != nil {
		return c.request.Attempt
	}
	return 1
}

// Manager implementation
type Manager interface {
	NewRootContext() *Context
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

func (m *contextManager) NewRootContext() *Context {
	rl := &RuntimeLayer{
		InstanceID: m.instanceID,
		Version:    m.version,
	}
	return newContext(stdcontext.Background(), rl, nil, nil, nil, nil)
}

// Derivations
func (c *Context) WithSession(sessionID, userID string) *Context {
	sl := &SessionLayer{
		SessionID: sessionID,
		UserID:    userID,
		State:     GetSessionStore().GetOrCreate(sessionID),
	}
	return newContext(c.std, c.runtime, sl, c.user, c.request, c)
}

func (c *Context) WithUser(userID, orgID string, claims map[string]string) *Context {
	filteredClaims := make(map[string]string)
	// Filter out sensitive auth elements per Section 26.1 (only propagate safe claims)
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
	return newContext(c.std, c.runtime, c.session, ul, c.request, c)
}

func (c *Context) WithExecution(executionID string, deadline time.Duration, priority kernel.PriorityClass) *Context {
	var requestedDeadline time.Time
	var std stdcontext.Context
	var cancel stdcontext.CancelFunc

	if deadline > 0 {
		requestedDeadline = time.Now().Add(deadline)
		std, cancel = stdcontext.WithDeadline(c.std, requestedDeadline)
	} else {
		std, cancel = stdcontext.WithCancel(c.std)
	}
	_ = cancel

	traceID := c.TraceID()
	if traceID == "" {
		traceID = fmt.Sprintf("tr-%s", executionID)
	}

	req := &RequestLayer{
		ExecutionID: executionID,
		TraceID:     traceID,
		Deadline:    requestedDeadline,
		Priority:    priority,
		Attempt:     1,
		Metadata:    make(map[string]string),
	}

	return newContext(std, c.runtime, c.session, c.user, req, c)
}

func (c *Context) WithChildExecution(childExecutionID string, timeoutOverride *time.Duration) *Context {
	var childDeadline time.Time
	var hasDeadline bool

	if timeoutOverride != nil {
		childDeadline = time.Now().Add(*timeoutOverride)
		hasDeadline = true
		if pDeadline, ok := c.Deadline(); ok {
			if pDeadline.Before(childDeadline) {
				childDeadline = pDeadline // clamp: child cannot outlive parent
			}
		}
	} else {
		childDeadline, hasDeadline = c.Deadline()
	}

	var std stdcontext.Context
	var cancel stdcontext.CancelFunc
	if hasDeadline {
		std, cancel = stdcontext.WithDeadline(c.std, childDeadline)
	} else {
		std, cancel = stdcontext.WithCancel(c.std)
	}
	_ = cancel

	var parentExecID string
	if c.request != nil {
		parentExecID = c.request.ExecutionID
	}

	req := &RequestLayer{
		ExecutionID:       childExecutionID,
		ParentExecutionID: parentExecID,
		TraceID:           c.TraceID(),
		Deadline:          childDeadline,
		Attempt:           1,
		Metadata:          make(map[string]string),
	}
	if c.request != nil {
		req.Priority = c.request.Priority
		req.CostBudget = c.request.CostBudget
		for k, v := range c.request.Metadata {
			req.Metadata[k] = v
		}
	}

	return newContext(std, c.runtime, c.session, c.user, req, c)
}

func (c *Context) WithAttempt(attemptNumber int) *Context {
	if c.request == nil {
		return c
	}
	req := &RequestLayer{
		ExecutionID:       c.request.ExecutionID,
		ParentExecutionID: c.request.ParentExecutionID,
		TraceID:           c.request.TraceID,
		Deadline:          c.request.Deadline,
		Priority:          c.request.Priority,
		CostBudget:        c.request.CostBudget,
		Attempt:           attemptNumber,
		Metadata:          make(map[string]string),
	}
	for k, v := range c.request.Metadata {
		req.Metadata[k] = v
	}
	return newContext(c.std, c.runtime, c.session, c.user, req, c)
}

func (c *Context) WithMetadata(key, value string) (*Context, error) {
	currentSize := 0
	if c.request != nil && c.request.Metadata != nil {
		for k, v := range c.request.Metadata {
			currentSize += len(k) + len(v)
		}
	}

	newEntrySize := len(key) + len(value)
	if c.request != nil && c.request.Metadata != nil {
		if val, exists := c.request.Metadata[key]; exists {
			currentSize -= len(key) + len(val)
		}
	}

	// Default size limit of 4096 bytes per context
	if currentSize+newEntrySize > 4096 {
		return c, fmt.Errorf("metadata size limit exceeded")
	}

	req := &RequestLayer{
		Metadata: make(map[string]string),
	}
	if c.request != nil {
		req.ExecutionID = c.request.ExecutionID
		req.ParentExecutionID = c.request.ParentExecutionID
		req.TraceID = c.request.TraceID
		req.Deadline = c.request.Deadline
		req.Priority = c.request.Priority
		req.CostBudget = c.request.CostBudget
		req.Attempt = c.request.Attempt
		for k, v := range c.request.Metadata {
			req.Metadata[k] = v
		}
	}
	req.Metadata[key] = value

	return newContext(c.std, c.runtime, c.session, c.user, req, c), nil
}

func (c *Context) WithCostBudget(budget CostBudget) *Context {
	if c.request == nil {
		return c
	}
	req := &RequestLayer{
		ExecutionID:       c.request.ExecutionID,
		ParentExecutionID: c.request.ParentExecutionID,
		TraceID:           c.request.TraceID,
		Deadline:          c.request.Deadline,
		Priority:          c.request.Priority,
		CostBudget:        &budget,
		Attempt:           c.request.Attempt,
		Metadata:          make(map[string]string),
	}
	for k, v := range c.request.Metadata {
		req.Metadata[k] = v
	}
	return newContext(c.std, c.runtime, c.session, c.user, req, c)
}

func (c *Context) Session() SessionState {
	if c.session != nil {
		return c.session.State
	}
	return nil
}
