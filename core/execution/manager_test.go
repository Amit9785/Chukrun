package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"chukrun/core/config"
	rtcontext "chukrun/core/context"
	"chukrun/core/events"
	"chukrun/core/lifecycle"
	"chukrun/core/telemetry"
)

type managerMockProvider struct {
	name string
}

func (m *managerMockProvider) Execute(ctx context.Context, request *ExecutionRequest) (*ExecutionResult, error) {
	if request.Metadata != nil && request.Metadata["fail"] == "true" {
		return nil, errors.New("mock execution failed")
	}
	return &ExecutionResult{
		ID:     request.ID,
		Status: StatusSucceeded,
	}, nil
}

func (m *managerMockProvider) Stream(ctx context.Context, request *ExecutionRequest) (<-chan StreamChunk, error) {
	ch := make(chan StreamChunk, 2)
	go func() {
		ch <- StreamChunk{Content: "chunk 1"}
		ch <- StreamChunk{Content: "chunk 2"}
		close(ch)
	}()
	return ch, nil
}

func (m *managerMockProvider) Name() string {
	return m.name
}

func (m *managerMockProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{}
}

func TestExecutionManagerSubmitAndWait(t *testing.T) {
	reg := NewRegistry()
	p1 := &managerMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	// Since pipeline wrapper is needed:
	pipeline := PipelineWrapper(nil)
	ev := events.NewInProcessEventBus()
	logger := telemetry.NewJSONLogger("info")
	tel := telemetry.NewInMemoryTelemetry()
	lifecycleMgr := lifecycle.NewLifecycleManager()

	_ = lifecycleMgr.Transition(lifecycle.StateBooting)
	_ = lifecycleMgr.Transition(lifecycle.StateReady)

	concurrency := config.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycleMgr, concurrency)
	defer em.Shutdown()

	req := &ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    rtcontext.PriorityClassHigh,
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

	if res.Status != StatusSucceeded {
		t.Errorf("expected status succeeded, got %s", res.Status)
	}
}

func TestExecutionManagerCancel(t *testing.T) {
	reg := NewRegistry()
	p1 := &managerMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := PipelineWrapper(nil)
	ev := events.NewInProcessEventBus()
	logger := telemetry.NewJSONLogger("info")
	tel := telemetry.NewInMemoryTelemetry()
	lifecycleMgr := lifecycle.NewLifecycleManager()

	_ = lifecycleMgr.Transition(lifecycle.StateBooting)
	_ = lifecycleMgr.Transition(lifecycle.StateReady)

	concurrency := config.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycleMgr, concurrency)
	defer em.Shutdown()

	req := &ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    rtcontext.PriorityClassNormal,
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
	reg := NewRegistry()
	p1 := &managerMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := PipelineWrapper(nil)
	ev := events.NewInProcessEventBus()
	logger := telemetry.NewJSONLogger("info")
	tel := telemetry.NewInMemoryTelemetry()
	lifecycleMgr := lifecycle.NewLifecycleManager()

	_ = lifecycleMgr.Transition(lifecycle.StateBooting)
	_ = lifecycleMgr.Transition(lifecycle.StateReady)

	concurrency := config.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycleMgr, concurrency)
	defer em.Shutdown()

	reqs := []*ExecutionRequest{
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
	reg := NewRegistry()
	p1 := &managerMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := PipelineWrapper(nil)
	ev := events.NewInProcessEventBus()
	logger := telemetry.NewJSONLogger("info")
	tel := telemetry.NewInMemoryTelemetry()
	lifecycleMgr := lifecycle.NewLifecycleManager()

	_ = lifecycleMgr.Transition(lifecycle.StateBooting)
	_ = lifecycleMgr.Transition(lifecycle.StateReady)

	concurrency := config.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycleMgr, concurrency)
	defer em.Shutdown()

	req := &ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    rtcontext.PriorityClassNormal,
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
	reg := NewRegistry()
	p1 := &managerMockProvider{name: "openai-primary"}
	_ = reg.Register(p1)

	pipeline := PipelineWrapper(nil)
	ev := events.NewInProcessEventBus()
	logger := telemetry.NewJSONLogger("info")
	tel := telemetry.NewInMemoryTelemetry()
	lifecycleMgr := lifecycle.NewLifecycleManager()

	_ = lifecycleMgr.Transition(lifecycle.StateBooting)
	_ = lifecycleMgr.Transition(lifecycle.StateReady)

	concurrency := config.ConcurrencyConfig{
		GlobalLimit: 10,
		QueueSize:   5,
	}

	em := NewExecutionManager(reg, pipeline, ev, logger, tel, lifecycleMgr, concurrency)
	defer em.Shutdown()

	req := &ExecutionRequest{
		ProviderRef: "openai-primary",
		Priority:    rtcontext.PriorityClassNormal,
		Metadata: map[string]string{
			"fail": "true",
		},
		RetryPolicy: &RetryPolicy{
			MaxAttempts:     2,
			BackoffStrategy: BackoffConstant,
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

	if res.Status != StatusFailed {
		t.Errorf("expected status failed, got %s", res.Status)
	}

	_ = em.ActiveCount()
}

func TestExecutionLocking(t *testing.T) {
	exec := &Execution{}
	exec.Lock()
	exec.ID = "lock-test"
	exec.Unlock()

	exec.RLock()
	_ = exec.ID
	exec.RUnlock()
}
