package telemetry

import (
	stdcontext "context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	mathrand "math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rtcontext "chukrun/core/context"
)

type Label struct {
	Key   string
	Value string
}

type Counter interface {
	Inc(ctx stdcontext.Context, labels ...Label)
	Add(ctx stdcontext.Context, value float64, labels ...Label)
}

type Histogram interface {
	Observe(ctx stdcontext.Context, value float64, labels ...Label)
}

type Gauge interface {
	Set(ctx stdcontext.Context, value float64, labels ...Label)
}

type Span interface {
	End()
	SetAttribute(key string, value any)
}

type TokenUsage struct {
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	Provider         string
}

type CostEstimate struct {
	AmountUSD float64
	Provider  string
}

type Telemetry interface {
	Counter(name string) Counter
	Histogram(name string) Histogram
	Gauge(name string) Gauge
	StartSpan(ctx stdcontext.Context, name string) (stdcontext.Context, Span)
	RecordTokenUsage(ctx stdcontext.Context, usage TokenUsage)
	RecordCost(ctx stdcontext.Context, cost CostEstimate)
}

type MetricType int

const (
	MetricTypeCounter MetricType = iota
	MetricTypeHistogram
	MetricTypeGauge
)

func (t MetricType) String() string {
	switch t {
	case MetricTypeCounter:
		return "COUNTER"
	case MetricTypeHistogram:
		return "HISTOGRAM"
	case MetricTypeGauge:
		return "GAUGE"
	default:
		return "COUNTER"
	}
}

type MetricValue struct {
	Name        string
	Type        MetricType
	Value       float64
	Labels      map[string]string
	Time        time.Time
	TraceID     string
	SessionID   string
	ExecutionID string
	Attempt     int
	UserID      string
}

type SpanContext struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
}

type spanContextKey struct{}

func formatTraceID(raw string) string {
	raw = strings.TrimPrefix(raw, "tr-")
	raw = strings.ReplaceAll(raw, "-", "")
	if len(raw) > 32 {
		return raw[:32]
	}
	if len(raw) < 32 {
		return raw + strings.Repeat("0", 32-len(raw))
	}
	return raw
}

func generateID(length int) string {
	bytes := make([]byte, length)
	_, _ = cryptorand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func InjectW3C(ctx stdcontext.Context) string {
	sc, ok := ctx.Value(spanContextKey{}).(SpanContext)
	if !ok {
		tid := rtcontext.GetTraceID(ctx)
		if tid != "" {
			return fmt.Sprintf("00-%s-0000000000000000-01", formatTraceID(tid))
		}
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", formatTraceID(sc.TraceID), sc.SpanID, sc.TraceFlags)
}

func ExtractW3C(traceparent string) (SpanContext, bool) {
	if traceparent == "" {
		return SpanContext{}, false
	}
	parts := strings.Split(traceparent, "-")
	if len(parts) < 4 {
		return SpanContext{}, false
	}
	flagsVal := byte(0)
	if len(parts[3]) == 2 {
		if fBytes, err := hex.DecodeString(parts[3]); err == nil && len(fBytes) > 0 {
			flagsVal = fBytes[0]
		}
	}
	return SpanContext{
		TraceID:    parts[1],
		SpanID:     parts[2],
		TraceFlags: flagsVal,
	}, true
}

func EstimateStreamingTokenUsage(promptText, completionText string) TokenUsage {
	promptTokens := int64(len(promptText) / 4)
	if promptTokens == 0 && len(promptText) > 0 {
		promptTokens = 1
	}
	completionTokens := int64(len(completionText) / 4)
	if completionTokens == 0 && len(completionText) > 0 {
		completionTokens = 1
	}
	return TokenUsage{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      promptTokens + completionTokens,
	}
}

var (
	globalDebugMode  int32  = 0
	globalSampleRate uint64 = math.Float64bits(0.10)

	priorityOverridesMu sync.RWMutex
	priorityOverrides   map[string]float64 = map[string]float64{
		"critical": 1.0,
		"high":     0.5,
	}
)

func SetDebugMode(enabled bool) {
	var val int32
	if enabled {
		val = 1
	}
	atomic.StoreInt32(&globalDebugMode, val)
}

func IsDebugModeEnabled() bool {
	return atomic.LoadInt32(&globalDebugMode) == 1
}

func SetGlobalSamplingRate(rate float64) {
	atomic.StoreUint64(&globalSampleRate, math.Float64bits(rate))
}

func GetGlobalSamplingRate() float64 {
	return math.Float64frombits(atomic.LoadUint64(&globalSampleRate))
}

func SetPriorityOverride(priority string, rate float64) {
	priorityOverridesMu.Lock()
	defer priorityOverridesMu.Unlock()
	priorityOverrides[strings.ToLower(priority)] = rate
}

func GetPriorityOverride(priority string) (float64, bool) {
	priorityOverridesMu.RLock()
	defer priorityOverridesMu.RUnlock()
	val, ok := priorityOverrides[strings.ToLower(priority)]
	return val, ok
}

type MetricsExporter interface {
	Export(metrics []MetricValue) error
}

type MetricsExporterRegistry struct {
	mu        sync.RWMutex
	exporters []MetricsExporter
}

var globalExporterRegistry = &MetricsExporterRegistry{}

func RegisterMetricsExporter(exporter MetricsExporter) error {
	globalExporterRegistry.mu.Lock()
	defer globalExporterRegistry.mu.Unlock()
	globalExporterRegistry.exporters = append(globalExporterRegistry.exporters, exporter)
	return nil
}

type platformCounter struct {
	name string
	tel  *InMemoryTelemetry
}

func (c *platformCounter) Inc(ctx stdcontext.Context, labels ...Label) {
	c.Add(ctx, 1.0, labels...)
}

func (c *platformCounter) Add(ctx stdcontext.Context, value float64, labels ...Label) {
	c.tel.recordMetric(ctx, c.name, MetricTypeCounter, value, labels...)
}

type platformHistogram struct {
	name string
	tel  *InMemoryTelemetry
}

func (h *platformHistogram) Observe(ctx stdcontext.Context, value float64, labels ...Label) {
	h.tel.recordMetric(ctx, h.name, MetricTypeHistogram, value, labels...)
}

type platformGauge struct {
	name string
	tel  *InMemoryTelemetry
}

func (g *platformGauge) Set(ctx stdcontext.Context, value float64, labels ...Label) {
	g.tel.recordMetric(ctx, g.name, MetricTypeGauge, value, labels...)
}

type platformSpan struct {
	name       string
	ctx        stdcontext.Context
	startTime  time.Time
	attributes map[string]any
	sampled    bool
	tel        *InMemoryTelemetry
}

func (s *platformSpan) End() {
	if !s.sampled {
		return
	}
	duration := time.Since(s.startTime)
	s.tel.recordSpanEnd(s.ctx, s.name, duration, s.attributes)
}

func (s *platformSpan) SetAttribute(key string, value any) {
	keyLower := strings.ToLower(key)
	isSens := keyLower == "api_key" || keyLower == "secret" || keyLower == "password" || keyLower == "authorization" ||
		strings.Contains(keyLower, "key") || strings.Contains(keyLower, "secret") ||
		rtcontext.IsSensitiveKey(s.ctx, key)

	if isSens {
		s.attributes[key] = "[REDACTED]"
	} else {
		s.attributes[key] = value
	}
}

type InMemoryTelemetry struct {
	mu            sync.RWMutex
	metrics       []MetricValue
	finishedSpans []map[string]any
}

func NewInMemoryTelemetry() *InMemoryTelemetry {
	return &InMemoryTelemetry{
		metrics:       make([]MetricValue, 0),
		finishedSpans: make([]map[string]any, 0),
	}
}

func (t *InMemoryTelemetry) Counter(name string) Counter {
	return &platformCounter{name: name, tel: t}
}

func (t *InMemoryTelemetry) Histogram(name string) Histogram {
	return &platformHistogram{name: name, tel: t}
}

func (t *InMemoryTelemetry) Gauge(name string) Gauge {
	return &platformGauge{name: name, tel: t}
}

func (t *InMemoryTelemetry) StartSpan(ctx stdcontext.Context, name string) (stdcontext.Context, Span) {
	parentSC, hasParent := ctx.Value(spanContextKey{}).(SpanContext)
	traceID := t.resolveTraceID(ctx, parentSC, hasParent)
	spanID := generateID(8)

	sc := SpanContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: 01,
	}

	sampled := t.shouldSampleTrace(ctx)
	if !sampled {
		sc.TraceFlags = 00
	}

	derivedCtx := stdcontext.WithValue(ctx, spanContextKey{}, sc)

	s := &platformSpan{
		name:       name,
		ctx:        derivedCtx,
		startTime:  time.Now(),
		attributes: make(map[string]any),
		sampled:    sampled,
		tel:        t,
	}

	t.enrichSpanAttributes(ctx, s, sc, parentSC, hasParent)

	return derivedCtx, s
}

func (t *InMemoryTelemetry) resolveTraceID(ctx stdcontext.Context, parentSC SpanContext, hasParent bool) string {
	if hasParent {
		return parentSC.TraceID
	}
	if req := rtcontext.GetTraceID(ctx); req != "" {
		return formatTraceID(req)
	}
	return generateID(16)
}

func (t *InMemoryTelemetry) shouldSampleTrace(ctx stdcontext.Context) bool {
	if IsDebugModeEnabled() {
		return true
	}
	rate := GetGlobalSamplingRate()
	priority := rtcontext.GetPriority(ctx)
	priorityStr := strings.ToLower(string(priority))

	priorityOverridesMu.RLock()
	r, ok := priorityOverrides[priorityStr]
	priorityOverridesMu.RUnlock()
	if ok {
		rate = r
	}

	if rate >= 1.0 {
		return true
	}
	if rate <= 0.0 {
		return false
	}
	return mathrand.Float64() < rate
}

func (t *InMemoryTelemetry) enrichSpanAttributes(ctx stdcontext.Context, s Span, sc SpanContext, parentSC SpanContext, hasParent bool) {
	s.SetAttribute("trace_id", sc.TraceID)
	s.SetAttribute("span_id", sc.SpanID)
	if hasParent {
		s.SetAttribute("parent_span_id", parentSC.SpanID)
	}
	if sid := rtcontext.GetSessionID(ctx); sid != "" {
		s.SetAttribute("session_id", sid)
	}
	if eid := rtcontext.GetExecutionID(ctx); eid != "" {
		s.SetAttribute("execution_id", eid)
	}
	s.SetAttribute("attempt_number", rtcontext.GetAttemptNumber(ctx))
	if uid := rtcontext.GetUserID(ctx); uid != "" {
		s.SetAttribute("user_id", uid)
	}
	if ul := rtcontext.GetUserLayer(ctx); ul != nil && ul.OrgID != "" {
		s.SetAttribute("tenant_id", ul.OrgID)
	}
}

func (t *InMemoryTelemetry) RecordTokenUsage(ctx stdcontext.Context, usage TokenUsage) {
	provider := usage.Provider
	if provider == "" {
		provider = "unknown"
	}
	t.Counter("token_usage_total").Add(ctx, float64(usage.PromptTokens), Label{Key: "provider", Value: provider}, Label{Key: "token_type", Value: "prompt"})
	t.Counter("token_usage_total").Add(ctx, float64(usage.CompletionTokens), Label{Key: "provider", Value: provider}, Label{Key: "token_type", Value: "completion"})
	t.Counter("token_usage_total").Add(ctx, float64(usage.TotalTokens), Label{Key: "provider", Value: provider}, Label{Key: "token_type", Value: "total"})
}

func (t *InMemoryTelemetry) RecordCost(ctx stdcontext.Context, cost CostEstimate) {
	provider := cost.Provider
	if provider == "" {
		provider = "unknown"
	}
	t.Counter("cost_recorded_total_usd").Add(ctx, cost.AmountUSD, Label{Key: "provider", Value: provider})

	if budget := rtcontext.GetCostBudget(ctx); budget != nil {
		budget.AddSpent(cost.AmountUSD)
	}
}

func (t *InMemoryTelemetry) recordMetric(ctx stdcontext.Context, name string, mtype MetricType, value float64, labels ...Label) {
	labelMap := make(map[string]string)
	for _, l := range labels {
		keyLower := strings.ToLower(l.Key)
		isSens := keyLower == "api_key" || keyLower == "secret" || keyLower == "password" || keyLower == "authorization" ||
			strings.Contains(keyLower, "key") || strings.Contains(keyLower, "secret") ||
			rtcontext.IsSensitiveKey(ctx, l.Key)

		if isSens {
			labelMap[l.Key] = "[REDACTED]"
		} else {
			labelMap[l.Key] = l.Value
		}
	}

	mv := MetricValue{
		Name:        name,
		Type:        mtype,
		Value:       value,
		Labels:      labelMap,
		Time:        time.Now(),
		TraceID:     rtcontext.GetTraceID(ctx),
		SessionID:   rtcontext.GetSessionID(ctx),
		ExecutionID: rtcontext.GetExecutionID(ctx),
		Attempt:     rtcontext.GetAttemptNumber(ctx),
		UserID:      rtcontext.GetUserID(ctx),
	}

	t.mu.Lock()
	t.metrics = append(t.metrics, mv)
	t.mu.Unlock()

	globalExporterRegistry.mu.RLock()
	for _, exporter := range globalExporterRegistry.exporters {
		_ = exporter.Export([]MetricValue{mv})
	}
	globalExporterRegistry.mu.RUnlock()
}

func (t *InMemoryTelemetry) recordSpanEnd(_ stdcontext.Context, name string, duration time.Duration, attrs map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishedSpans = append(t.finishedSpans, map[string]any{
		"name":       name,
		"duration":   duration,
		"attributes": attrs,
		"time":       time.Now(),
	})
}

func (t *InMemoryTelemetry) GetMetrics() []MetricValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	copied := make([]MetricValue, len(t.metrics))
	copy(copied, t.metrics)
	return copied
}

func (t *InMemoryTelemetry) ClearMetrics() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = make([]MetricValue, 0)
	t.finishedSpans = make([]map[string]any, 0)
}

func (t *InMemoryTelemetry) GetFinishedSpans() []map[string]any {
	t.mu.RLock()
	defer t.mu.RUnlock()
	copied := make([]map[string]any, len(t.finishedSpans))
	copy(copied, t.finishedSpans)
	return copied
}
