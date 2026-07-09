package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
	"chukrun/runtime/provider"
)

type testMockProvider struct {
	name string
}

func (m *testMockProvider) Execute(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
	if request.Metadata != nil && request.Metadata["fail"] == "true" {
		return nil, errors.New("mock execution failed")
	}
	return &kernel.ExecutionResult{
		ID:     request.ID,
		Status: kernel.StatusSucceeded,
	}, nil
}

func (m *testMockProvider) Stream(ctx context.Context, request *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error) {
	ch := make(chan kernel.StreamChunk, 2)
	go func() {
		ch <- kernel.StreamChunk{Content: "chunk 1"}
		ch <- kernel.StreamChunk{Content: "chunk 2"}
		close(ch)
	}()
	return ch, nil
}

func (m *testMockProvider) Name() string {
	return m.name
}

func (m *testMockProvider) Capabilities() provider.ProviderCapabilities {
	return provider.ProviderCapabilities{}
}

func TestExecutionManagerSubmitAndWait(t *testing.T) {
	reg := provider.NewRegistry()
	p1 := &testMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := NewPipeline(nil)
	ev := kernel.NewInProcessEventBus()
	logger := observability.NewJSONLogger("info")
	tel := observability.NewInMemoryTelemetry()
	lifecycle := kernel.NewLifecycleManager()

	_ = lifecycle.Transition(kernel.StateBooting)
	_ = lifecycle.Transition(kernel.StateReady)

	concurrency := kernel.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycle, concurrency)
	defer em.Shutdown()

	req := &kernel.ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    kernel.PriorityClassHigh,
	}

	exec, err := em.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to submit execution: %v", err)
	}

	if exec.ID == "" {
		t.Error("expected non-empty execution ID")
	}

	// Wait for execution to finish
	res, err := em.Wait(context.Background(), exec.ID)
	if err != nil {
		t.Fatalf("failed to wait for execution: %v", err)
	}

	if res.Status != kernel.StatusSucceeded {
		t.Errorf("expected status succeeded, got %s", res.Status)
	}
}

func TestExecutionManagerCancel(t *testing.T) {
	reg := provider.NewRegistry()
	p1 := &testMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := NewPipeline(nil)
	ev := kernel.NewInProcessEventBus()
	logger := observability.NewJSONLogger("info")
	tel := observability.NewInMemoryTelemetry()
	lifecycle := kernel.NewLifecycleManager()

	_ = lifecycle.Transition(kernel.StateBooting)
	_ = lifecycle.Transition(kernel.StateReady)

	concurrency := kernel.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycle, concurrency)
	defer em.Shutdown()

	req := &kernel.ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    kernel.PriorityClassNormal,
	}

	exec, err := em.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to submit execution: %v", err)
	}

	err = em.Cancel(context.Background(), exec.ID)
	if err != nil {
		t.Fatalf("failed to cancel execution: %v", err)
	}

	_, _ = em.Wait(context.Background(), exec.ID)
}

func TestExecutionManagerSubmitBatchAndGet(t *testing.T) {
	reg := provider.NewRegistry()
	p1 := &testMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := NewPipeline(nil)
	ev := kernel.NewInProcessEventBus()
	logger := observability.NewJSONLogger("info")
	tel := observability.NewInMemoryTelemetry()
	lifecycle := kernel.NewLifecycleManager()

	_ = lifecycle.Transition(kernel.StateBooting)
	_ = lifecycle.Transition(kernel.StateReady)

	concurrency := kernel.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycle, concurrency)
	defer em.Shutdown()

	reqs := []*kernel.ExecutionRequest{
		{ProviderRef: "openai-primary"},
		{ProviderRef: "openai-primary"},
	}

	batch, err := em.SubmitBatch(context.Background(), reqs)
	if err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}

	if len(batch.Executions) != 2 {
		t.Errorf("expected 2 executions in batch, got %d", len(batch.Executions))
	}

	exec, err := em.Get(context.Background(), batch.Executions[0].ID)
	if err != nil {
		t.Fatalf("failed to Get execution: %v", err)
	}
	if exec.ID != batch.Executions[0].ID {
		t.Errorf("expected execution ID %s, got %s", batch.Executions[0].ID, exec.ID)
	}

	_, _ = em.Wait(context.Background(), batch.Executions[0].ID)
	_, _ = em.Wait(context.Background(), batch.Executions[1].ID)
}

func TestExecutionManagerStream(t *testing.T) {
	reg := provider.NewRegistry()
	p1 := &testMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := NewPipeline(nil)
	ev := kernel.NewInProcessEventBus()
	logger := observability.NewJSONLogger("info")
	tel := observability.NewInMemoryTelemetry()
	lifecycle := kernel.NewLifecycleManager()

	_ = lifecycle.Transition(kernel.StateBooting)
	_ = lifecycle.Transition(kernel.StateReady)

	concurrency := kernel.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycle, concurrency)
	defer em.Shutdown()

	req := &kernel.ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    kernel.PriorityClassNormal,
	}

	ch, err := em.Stream(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to call Stream: %v", err)
	}

	var chunks []string
	for chunk := range ch {
		chunks = append(chunks, chunk.Content)
	}

	if len(chunks) != 2 {
		t.Errorf("expected 2 stream chunks, got %d", len(chunks))
	}
}

func TestExecutionManagerRetry(t *testing.T) {
	reg := provider.NewRegistry()
	p1 := &testMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := NewPipeline(nil)
	ev := kernel.NewInProcessEventBus()
	logger := observability.NewJSONLogger("info")
	tel := observability.NewInMemoryTelemetry()
	lifecycle := kernel.NewLifecycleManager()

	_ = lifecycle.Transition(kernel.StateBooting)
	_ = lifecycle.Transition(kernel.StateReady)

	concurrency := kernel.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycle, concurrency)
	defer em.Shutdown()

	req := &kernel.ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    kernel.PriorityClassNormal,
		Metadata: map[string]string{
			"fail": "true",
		},
		RetryPolicy: &kernel.RetryPolicy{
			MaxAttempts:     2,
			BackoffStrategy: kernel.BackoffConstant,
			BaseDelay:       5 * time.Millisecond,
		},
	}

	exec, err := em.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to submit execution: %v", err)
	}

	res, err := em.Wait(context.Background(), exec.ID)
	if err != nil {
		t.Fatalf("failed to wait for execution: %v", err)
	}

	if res.Status != kernel.StatusFailed {
		t.Errorf("expected status failed, got %s", res.Status)
	}

	_ = em.ActiveCount()
}
