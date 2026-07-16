package observability

import (
	stdcontext "context"
	"strings"
	"testing"
	"time"

	rtcontext "chukrun/runtime/context"
	"chukrun/runtime/kernel"
)

func TestTelemetryMetricsEnrichment(t *testing.T) {
	tel := NewInMemoryTelemetry()

	ctx := stdcontext.Background()
	ctx = rtcontext.WithSession(ctx, "sess-test", "user-test")
	ctx = rtcontext.WithExecution(ctx, "exec-test", 5*time.Second, kernel.PriorityClassCritical)

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
