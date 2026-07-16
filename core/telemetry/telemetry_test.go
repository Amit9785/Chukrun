package telemetry

import (
	stdcontext "context"
	"strings"
	"sync"
	"testing"
	"time"

	rtcontext "chukrun/core/context"
)

func TestTelemetryMetricsEnrichment(t *testing.T) {
	tel := NewInMemoryTelemetry()

	ctx := stdcontext.Background()
	ctx = rtcontext.WithSession(ctx, "sess-test", "user-test")
	ctx = rtcontext.WithExecution(ctx, "exec-test", 5*time.Second, rtcontext.PriorityClassCritical)

	counter := tel.Counter("test_counter")
	counter.Inc(ctx, Label{Key: "env", Value: "test"})

	histogram := tel.Histogram("test_histogram")
	histogram.Observe(ctx, 4.5, Label{Key: "op", Value: "exec"})

	gauge := tel.Gauge("test_gauge")
	gauge.Set(ctx, 42.0, Label{Key: "tier", Value: "free"})

	metrics := tel.GetMetrics()
	if len(metrics) < 3 {
		t.Fatalf("expected at least 3 metrics, got: %d", len(metrics))
	}

	if metrics[0].Name != "test_counter" || metrics[0].Value != 1.0 || metrics[0].TraceID == "" {
		t.Errorf("unexpected counter metric: %+v", metrics[0])
	}
	if metrics[1].Name != "test_histogram" || metrics[1].Value != 4.5 || metrics[1].SessionID != "sess-test" {
		t.Errorf("unexpected histogram metric: %+v", metrics[1])
	}
	if metrics[2].Name != "test_gauge" || metrics[2].Value != 42.0 || metrics[2].ExecutionID != "exec-test" {
		t.Errorf("unexpected gauge metric: %+v", metrics[2])
	}
}

func TestTelemetryTokenUsageAndCost(t *testing.T) {
	tel := NewInMemoryTelemetry()
	ctx := stdcontext.Background()

	usage := EstimateStreamingTokenUsage("Hello world", "This is a completed response chunk.")
	if usage.PromptTokens != 2 || usage.CompletionTokens != 8 {
		t.Errorf("unexpected heuristic token usage: %+v", usage)
	}

	budget := rtcontext.NewCostBudget(10.0, "USD")
	budgetCtx := rtcontext.WithCostBudget(ctx, *budget)

	tel.RecordCost(budgetCtx, CostEstimate{AmountUSD: 1.5, Provider: "openai"})
	if budget.Spent() != 1.5 {
		t.Errorf("expected budget spent to be updated to 1.5, got %f", budget.Spent())
	}
}

func TestTelemetryTraceContextPropagation(t *testing.T) {
	SetGlobalSamplingRate(1.0)
	tel := NewInMemoryTelemetry()
	ctx := stdcontext.Background()

	spanCtx, span := tel.StartSpan(ctx, "root-span")

	w3cHeader := InjectW3C(spanCtx)
	if !strings.HasPrefix(w3cHeader, "00-") {
		t.Errorf("expected W3C header format starting with 00, got: %q", w3cHeader)
	}

	extractedSC, ok := ExtractW3C(w3cHeader)
	if !ok || extractedSC.TraceID == "" || extractedSC.SpanID == "" {
		t.Errorf("failed to extract valid W3C span context, got extracted SC: %+v", extractedSC)
	}

	childCtx, childSpan := tel.StartSpan(spanCtx, "child-span")
	childSC, childOk := childCtx.Value(spanContextKey{}).(SpanContext)
	parentSC, parentOk := spanCtx.Value(spanContextKey{}).(SpanContext)
	if !childOk || !parentOk {
		t.Fatal("failed to find span context in derived contexts")
	}

	if childSC.TraceID != parentSC.TraceID {
		t.Errorf("child trace ID %q did not match parent trace ID %q", childSC.TraceID, parentSC.TraceID)
	}

	childSpan.End()
	span.End()

	finished := tel.GetFinishedSpans()
	if len(finished) < 2 {
		t.Errorf("expected 2 finished spans, got %d", len(finished))
	}
}

type MockExporter struct {
	mu      sync.Mutex
	metrics []MetricValue
}

func (m *MockExporter) Export(metrics []MetricValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = append(m.metrics, metrics...)
	return nil
}

func TestTelemetryMetricExporter(t *testing.T) {
	exporter := &MockExporter{}
	RegisterMetricsExporter(exporter)
	defer func() {
		globalExporterRegistry.mu.Lock()
		globalExporterRegistry.exporters = nil
		globalExporterRegistry.mu.Unlock()
	}()

	tel := NewInMemoryTelemetry()
	tel.Counter("exported_counter").Inc(stdcontext.Background())

	exporter.mu.Lock()
	l := len(exporter.metrics)
	exporter.mu.Unlock()
	if l == 0 {
		t.Error("expected metric to be exported")
	}
}

func TestTelemetryPrioritySampling(t *testing.T) {
	SetGlobalSamplingRate(0.0)
	SetPriorityOverride("critical", 1.0)
	defer SetPriorityOverride("critical", 1.0)

	tel := NewInMemoryTelemetry()
	ctx := rtcontext.WithExecution(stdcontext.Background(), "exec-crit", 5*time.Second, rtcontext.PriorityClassCritical)

	_, span := tel.StartSpan(ctx, "critical-span")
	if !span.(*platformSpan).sampled {
		t.Error("expected critical span to be sampled due to override")
	}
}

func TestTelemetrySpanRedaction(t *testing.T) {
	SetGlobalSamplingRate(1.0)
	tel := NewInMemoryTelemetry()
	ctx := stdcontext.Background()

	ctx = rtcontext.WithSensitiveVariable(ctx, "secret_var", "secret-value")

	_, span := tel.StartSpan(ctx, "span-redact")
	span.SetAttribute("secret_var", "secret-value")
	span.SetAttribute("api_key", "my-key")
	span.SetAttribute("normal_attr", "normal-value")
	span.End()

	spans := tel.GetFinishedSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	attrs := spans[0]["attributes"].(map[string]any)
	if attrs["secret_var"] != "[REDACTED]" {
		t.Errorf("expected secret_var to be redacted, got %v", attrs["secret_var"])
	}
	if attrs["api_key"] != "[REDACTED]" {
		t.Errorf("expected api_key to be redacted, got %v", attrs["api_key"])
	}
	if attrs["normal_attr"] != "normal-value" {
		t.Errorf("expected normal_attr to be normal-value, got %v", attrs["normal_attr"])
	}
}

func TestTelemetryInvalidW3C(t *testing.T) {
	_, ok := ExtractW3C("invalid-header")
	if ok {
		t.Error("expected false for invalid-header")
	}
	_, ok = ExtractW3C("00-traceid-spanid")
	if ok {
		t.Error("expected false for incorrect parts")
	}
}

func TestTelemetryExtraUtilities(t *testing.T) {
	SetDebugMode(true)
	if !IsDebugModeEnabled() {
		t.Error("expected debug mode to be enabled")
	}
	SetDebugMode(false)
	if IsDebugModeEnabled() {
		t.Error("expected debug mode to be disabled")
	}

	rate, ok := GetPriorityOverride("critical")
	if !ok || rate != 1.0 {
		t.Errorf("expected 1.0 for critical override, got %f", rate)
	}

	tel := NewInMemoryTelemetry()
	tel.Counter("c").Inc(stdcontext.Background())
	tel.ClearMetrics()
	if len(tel.GetMetrics()) != 0 {
		t.Error("expected metrics to be cleared")
	}

	SetGlobalSamplingRate(0.0)
	_, span := tel.StartSpan(stdcontext.Background(), "unsampled")
	span.SetAttribute("k", "v")
	span.End()

	_, ok = ExtractW3C("")
	if ok {
		t.Error("expected false for empty W3C header")
	}
}
