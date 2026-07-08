package observability

import (
	"context"
	"testing"
)

func TestInMemoryTelemetryOperations(t *testing.T) {
	tel := NewInMemoryTelemetry()

	labels := map[string]string{"env": "test"}

	tel.IncrementCounter("test_counter", labels)
	tel.ObserveHistogram("test_latency", 0.045, labels)
	tel.SetGauge("test_concurrency", 3.0, labels)

	metrics := tel.GetMetrics()
	if len(metrics) != 3 {
		t.Fatalf("expected 3 metrics, got: %d", len(metrics))
	}

	if metrics[0].Name != "test_counter" || metrics[0].Type != MetricTypeCounter || metrics[0].Value != 1 {
		t.Errorf("unexpected counter metric: %+v", metrics[0])
	}
	if metrics[1].Name != "test_latency" || metrics[1].Type != MetricTypeHistogram || metrics[1].Value != 0.045 {
		t.Errorf("unexpected histogram metric: %+v", metrics[1])
	}
	if metrics[2].Name != "test_concurrency" || metrics[2].Type != MetricTypeGauge || metrics[2].Value != 3.0 {
		t.Errorf("unexpected gauge metric: %+v", metrics[2])
	}

	ctx, endSpan := tel.StartSpan(context.Background(), "span-1")
	if ctx == nil {
		t.Error("expected non-nil context from StartSpan")
	}
	endSpan()

	tel.ClearMetrics()
	if len(tel.GetMetrics()) != 0 {
		t.Error("expected telemetry metrics list to be empty after clearing")
	}
}
