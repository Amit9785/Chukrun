package observability

import (
	"context"
	"sync"
	"time"
)

type MetricType int

const (
	MetricTypeCounter MetricType = iota
	MetricTypeHistogram
	MetricTypeGauge
)

// MetricValue represents a single metric observation
type MetricValue struct {
	Name   string
	Type   MetricType
	Value  float64
	Labels map[string]string
	Time   time.Time
}

// Telemetry provides metrics collection and tracing hooks
type Telemetry interface {
	IncrementCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
	StartSpan(ctx context.Context, name string) (context.Context, func())
	GetMetrics() []MetricValue
	ClearMetrics()
}

// InMemoryTelemetry is a simple thread-safe in-memory metrics exporter
type InMemoryTelemetry struct {
	mu      sync.RWMutex
	metrics []MetricValue
}

func NewInMemoryTelemetry() *InMemoryTelemetry {
	return &InMemoryTelemetry{
		metrics: make([]MetricValue, 0),
	}
}

func (t *InMemoryTelemetry) IncrementCounter(name string, labels map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = append(t.metrics, MetricValue{
		Name:   name,
		Type:   MetricTypeCounter,
		Value:  1,
		Labels: labels,
		Time:   time.Now(),
	})
}

func (t *InMemoryTelemetry) ObserveHistogram(name string, value float64, labels map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = append(t.metrics, MetricValue{
		Name:   name,
		Type:   MetricTypeHistogram,
		Value:  value,
		Labels: labels,
		Time:   time.Now(),
	})
}

func (t *InMemoryTelemetry) SetGauge(name string, value float64, labels map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = append(t.metrics, MetricValue{
		Name:   name,
		Type:   MetricTypeGauge,
		Value:  value,
		Labels: labels,
		Time:   time.Now(),
	})
}

func (t *InMemoryTelemetry) StartSpan(ctx context.Context, name string) (context.Context, func()) {
	// A simple trace span mock returning the parent context and an empty end function
	return ctx, func() {
		// Mock span end, no action needed for in-memory implementation
	}
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
}
