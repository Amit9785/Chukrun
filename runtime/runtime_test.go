package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"chukrun/runtime/execution"
	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
	"chukrun/runtime/provider"
)

type mockProvider struct {
	name         string
	executeFunc  func(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error)
	capabilities provider.ProviderCapabilities
}

func (m *mockProvider) Execute(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, request)
	}
	return &kernel.ExecutionResult{
		ID:     request.ID,
		Status: kernel.StatusSucceeded,
		Output: "mock response",
	}, nil
}

func (m *mockProvider) Stream(ctx context.Context, request *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error) {
	out := make(chan kernel.StreamChunk, 2)
	out <- kernel.StreamChunk{ID: request.ID, Content: "mock chunk 1"}
	out <- kernel.StreamChunk{ID: request.ID, Content: "mock chunk 2"}
	close(out)
	return out, nil
}

func (m *mockProvider) Name() string {
	return m.name
}

func (m *mockProvider) Capabilities() provider.ProviderCapabilities {
	return m.capabilities
}

func TestCoreRuntimeLifecycleAndExecution(t *testing.T) {
	p := &mockProvider{
		name: "mock-llm",
		capabilities: provider.ProviderCapabilities{
			Streaming:       true,
			FunctionCalling: false,
		},
	}

	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Runtime: kernel.RuntimeConfig{
				LogLevel: "info",
				Concurrency: kernel.ConcurrencyConfig{
					GlobalLimit: 5,
					QueueSize:   2,
				},
			},
		}),
	)

	ctx := context.Background()

	// Verify health check before initialization fails or is uninitialized
	h, err := rt.Health(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting health: %v", err)
	}
	if h.Overall != kernel.HealthHealthy && h.Components["lifecycle"].Details != "UNINITIALIZED" {
		t.Errorf("expected UNINITIALIZED health state, got overall %s, state %s", h.Overall, h.Components["lifecycle"].Details)
	}

	// Try execute before initialize -> should fail
	_, err = rt.Execute(ctx, &kernel.ExecutionRequest{ID: "req-1"})
	if err == nil {
		t.Fatal("expected execution to fail before initialization")
	}

	// Initialize
	err = rt.Initialize(ctx)
	if err != nil {
		t.Fatalf("failed to initialize runtime: %v", err)
	}

	// Double initialize should fail
	err = rt.Initialize(ctx)
	if err == nil {
		t.Fatal("expected double initialization to fail")
	}

	h, err = rt.Health(ctx)
	if err != nil {
		t.Fatalf("failed to get health: %v", err)
	}
	if h.Overall != kernel.HealthHealthy || h.Components["lifecycle"].Details != "READY" {
		t.Errorf("expected health state READY, got overall %s, state %s", h.Overall, h.Components["lifecycle"].Details)
	}

	// Execute successfully
	res, err := rt.Execute(ctx, &kernel.ExecutionRequest{
		ID:          "req-2",
		ProviderRef: "mock-llm",
		Payload:     "hello",
	})
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	if res.Status != kernel.StatusSucceeded || res.Output != "mock response" {
		t.Errorf("expected successful output, got: %+v", res)
	}

	// Shutdown
	err = rt.Shutdown(ctx)
	if err != nil {
		t.Fatalf("failed to shutdown runtime: %v", err)
	}

	// Execute after shutdown should fail
	_, err = rt.Execute(ctx, &kernel.ExecutionRequest{ID: "req-3"})
	if err == nil {
		t.Fatal("expected execution to fail after shutdown")
	}
}

func TestGracefulShutdownAndDrain(t *testing.T) {
	var inFlightRequest sync.WaitGroup
	inFlightRequest.Add(1)

	// Mock provider that sleeps for 200ms
	p := &mockProvider{
		name: "slow-llm",
		executeFunc: func(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
			inFlightRequest.Done()
			select {
			case <-time.After(200 * time.Millisecond):
				return &kernel.ExecutionResult{ID: request.ID, Status: kernel.StatusSucceeded, Output: "slow done"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Runtime: kernel.RuntimeConfig{
				ShutdownTimeoutMS: 500, // wait up to 500ms
				Concurrency: kernel.ConcurrencyConfig{
					GlobalLimit: 5,
					QueueSize:   0,
				},
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	// Execute slow request in a goroutine
	var slowExecResult *kernel.ExecutionResult
	var slowExecErr error
	var execWg sync.WaitGroup
	execWg.Add(1)

	go func() {
		defer execWg.Done()
		slowExecResult, slowExecErr = rt.Execute(ctx, &kernel.ExecutionRequest{
			ID:          "slow-req",
			ProviderRef: "slow-llm",
		})
	}()

	// Wait for the request to reach the provider
	inFlightRequest.Wait()

	// Shutdown in parallel
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- rt.Shutdown(ctx)
	}()

	// Slow execution should complete successfully before shutdown completes
	execWg.Wait()
	if slowExecErr != nil {
		t.Errorf("slow request failed: %v", slowExecErr)
	}
	if slowExecResult == nil || slowExecResult.Output != "slow done" {
		t.Errorf("unexpected slow request result: %+v", slowExecResult)
	}

	err := <-shutdownDone
	if err != nil {
		t.Errorf("failed to shutdown: %v", err)
	}
}

func TestConcurrencyLimiting(t *testing.T) {
	requestStarted := make(chan struct{}, 2)
	blockChan := make(chan struct{})

	p := &mockProvider{
		name: "blocking-llm",
		executeFunc: func(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
			requestStarted <- struct{}{}
			<-blockChan
			return &kernel.ExecutionResult{ID: request.ID, Status: kernel.StatusSucceeded}, nil
		},
	}

	// Concurrency Limit = 1, Queue Size = 1
	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Runtime: kernel.RuntimeConfig{
				Concurrency: kernel.ConcurrencyConfig{
					GlobalLimit: 1,
					QueueSize:   1,
				},
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	// Launch request 1 -> should run and block
	go func() {
		_, _ = rt.Execute(ctx, &kernel.ExecutionRequest{ID: "req-active", ProviderRef: "blocking-llm"})
	}()

	// Wait for request 1 to start
	<-requestStarted

	// Launch request 2 -> should enter queue
	queuedExecErr := make(chan error, 1)
	go func() {
		_, err := rt.Execute(ctx, &kernel.ExecutionRequest{ID: "req-queued", ProviderRef: "blocking-llm"})
		queuedExecErr <- err
	}()

	// Give it a moment to queue
	time.Sleep(50 * time.Millisecond)

	// Launch request 3 -> should fail fast (limit + queue full)
	_, err3 := rt.Execute(ctx, &kernel.ExecutionRequest{ID: "req-saturated", ProviderRef: "blocking-llm"})
	if err3 == nil {
		t.Fatal("expected request 3 to fail fast due to saturation")
	}

	platErr, ok := err3.(*kernel.PlatformError)
	if !ok || platErr.Category != kernel.ErrCategorySaturation {
		t.Errorf("expected saturation error, got: %v", err3)
	}

	// Unblock request 1
	close(blockChan)

	// Request 2 (queued) should now acquire the slot, execute, and succeed
	select {
	case err := <-queuedExecErr:
		if err != nil {
			t.Errorf("queued request failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for queued request to finish")
	}

	_ = rt.Shutdown(ctx)
}

type mockMiddlewareForAdd struct{}

func (m *mockMiddlewareForAdd) Handle(ctx context.Context, req *kernel.ExecutionRequest, next execution.Handler) (*kernel.ExecutionResult, error) {
	return next(ctx, req)
}

func TestBootstrapOptions(t *testing.T) {
	log := observability.NewJSONLogger("debug")
	tel := observability.NewInMemoryTelemetry()
	mw := &mockMiddlewareForAdd{}

	rt := NewRuntime(
		WithConfigFile("non_existent_config.json"),
		WithLogger(log),
		WithTelemetry(tel),
		WithMiddleware(mw),
	)

	ctx := context.Background()
	err := rt.Initialize(ctx)
	if err == nil {
		t.Fatal("expected initialization to fail due to missing config file")
	}
}

func TestStreamingFlow(t *testing.T) {
	p := &mockProvider{
		name: "stream-llm",
		capabilities: provider.ProviderCapabilities{
			Streaming: true,
		},
	}

	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Runtime: kernel.RuntimeConfig{
				Concurrency: kernel.ConcurrencyConfig{
					GlobalLimit: 5,
				},
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	ch, err := rt.Stream(ctx, &kernel.ExecutionRequest{
		ID:          "stream-req",
		ProviderRef: "stream-llm",
	})
	if err != nil {
		t.Fatalf("unexpected stream start error: %v", err)
	}

	count := 0
	for chunk := range ch {
		if chunk.Error != nil {
			t.Fatalf("unexpected chunk error: %v", chunk.Error)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 stream chunks, got: %d", count)
	}

	_ = rt.Shutdown(ctx)
}

func TestAutomaticTransitionToRunning(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	// After Initialize, state should be READY
	h, _ := rt.Health(ctx)
	if h.State != kernel.StateReady {
		t.Errorf("expected state READY, got: %s", h.State)
	}

	// First execution
	_, err := rt.Execute(ctx, &kernel.ExecutionRequest{
		ID:          "exec-1",
		ProviderRef: "mock-llm",
	})
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// State should now be RUNNING automatically
	h, _ = rt.Health(ctx)
	if h.State != kernel.StateRunning {
		t.Errorf("expected state to transition to RUNNING, got: %s", h.State)
	}

	_ = rt.Shutdown(ctx)
}

func TestHealthCheckingWithDegradedComponent(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Lifecycle: kernel.LifecycleConfig{
				AcceptWhenDegraded: true,
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	// Cast to access internal lifecycle for test injection
	coreRt := rt.(*CoreRuntime)

	// Report non-fatal telemetry failure
	coreRt.lifecycle.ReportComponentHealth("telemetry", false, false, errors.New("telemetry down"))

	// Check Health
	h, _ := rt.Health(ctx)
	if h.State != kernel.StateDegraded || h.Overall != kernel.HealthDegraded {
		t.Errorf("expected state DEGRADED, got overall: %s, state: %s", h.Overall, h.State)
	}

	// Should still be able to execute since AcceptWhenDegraded is true
	_, err := rt.Execute(ctx, &kernel.ExecutionRequest{
		ID:          "exec-2",
		ProviderRef: "mock-llm",
	})
	if err != nil {
		t.Errorf("expected execution to succeed in degraded state, got: %v", err)
	}

	// Reconfigure degraded to reject requests
	coreRt.lifecycle.Configure(1000, false)

	// Now execution should fail
	_, err = rt.Execute(ctx, &kernel.ExecutionRequest{
		ID:          "exec-3",
		ProviderRef: "mock-llm",
	})
	if err == nil {
		t.Error("expected execution to fail when degraded readiness is disabled")
	}

	_ = rt.Shutdown(ctx)
}

func TestRestartFromFailedState(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Lifecycle: kernel.LifecycleConfig{
				RestartCooldownMS: 20,
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	coreRt := rt.(*CoreRuntime)

	// Simulate fatal component failure
	coreRt.lifecycle.ReportComponentHealth("event_bus", false, true, errors.New("bus failure"))

	// Health status should be Unhealthy / FAILED
	h, _ := rt.Health(ctx)
	if h.State != kernel.StateFailed || h.Overall != kernel.HealthUnhealthy {
		t.Errorf("expected state FAILED, got overall: %s, state: %s", h.Overall, h.State)
	}

	// Execute should fail in FAILED state
	_, err := rt.Execute(ctx, &kernel.ExecutionRequest{
		ID:          "exec-4",
		ProviderRef: "mock-llm",
	})
	if err == nil {
		t.Error("expected execution to fail when runtime is failed")
	}

	// Restart
	err = rt.Restart(ctx)
	if err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	// State should be back to READY
	h, _ = rt.Health(ctx)
	if h.State != kernel.StateReady {
		t.Errorf("expected state READY after restart, got: %s", h.State)
	}

	_ = rt.Shutdown(ctx)
}

func TestAsyncExecutionEngine(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	coreRt := rt.(*CoreRuntime)
	mgr := coreRt.execution

	exec, err := mgr.Submit(ctx, &kernel.ExecutionRequest{
		ProviderRef: "mock-llm",
		Priority:    kernel.PriorityClassHigh,
	})
	if err != nil {
		t.Fatalf("failed to submit execution: %v", err)
	}

	if exec.State != kernel.ExecStateQueued && exec.State != kernel.ExecStateRunning {
		t.Errorf("unexpected initial state: %s", exec.State)
	}

	res, err := mgr.Wait(ctx, exec.ID)
	if err != nil {
		t.Fatalf("failed to wait on execution: %v", err)
	}

	if res.State != kernel.ExecStateSucceeded {
		t.Errorf("expected SUCCEEDED state, got: %s", res.State)
	}
	if res.AttemptCount != 1 {
		t.Errorf("expected 1 attempt, got: %d", res.AttemptCount)
	}

	_ = rt.Shutdown(ctx)
}

func TestExecutionEngineCancellation(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
		WithConfigOverrides(&kernel.Config{
			Runtime: kernel.RuntimeConfig{
				Concurrency: kernel.ConcurrencyConfig{
					GlobalLimit: 1, // force queuing
				},
			},
		}),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	coreRt := rt.(*CoreRuntime)
	mgr := coreRt.execution

	// First execution occupies the single slot
	exec1, _ := mgr.Submit(ctx, &kernel.ExecutionRequest{
		ProviderRef: "mock-llm",
	})

	// Second execution goes to queue
	exec2, _ := mgr.Submit(ctx, &kernel.ExecutionRequest{
		ProviderRef: "mock-llm",
	})

	// Cancel the second queued execution
	err := mgr.Cancel(ctx, exec2.ID)
	if err != nil {
		t.Fatalf("failed to cancel: %v", err)
	}

	res, err := mgr.Wait(ctx, exec2.ID)
	if err != nil {
		t.Fatalf("failed to wait: %v", err)
	}

	if res.State != kernel.ExecStateCancelled {
		t.Errorf("expected CANCELLED state, got: %s", res.State)
	}

	_, _ = mgr.Wait(ctx, exec1.ID)
	_ = rt.Shutdown(ctx)
}

func TestExecutionEngineBatching(t *testing.T) {
	p := &mockProvider{name: "mock-llm"}
	rt := NewRuntime(
		WithProvider(p),
	)

	ctx := context.Background()
	_ = rt.Initialize(ctx)

	coreRt := rt.(*CoreRuntime)
	mgr := coreRt.execution

	reqs := []*kernel.ExecutionRequest{
		{ProviderRef: "mock-llm"},
		{ProviderRef: "mock-llm"},
		{ProviderRef: "mock-llm"},
	}

	batch, err := mgr.SubmitBatch(ctx, reqs)
	if err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}

	if len(batch.Executions) != 3 {
		t.Errorf("expected 3 executions, got: %d", len(batch.Executions))
	}

	for _, exec := range batch.Executions {
		res, err := mgr.Wait(ctx, exec.ID)
		if err != nil {
			t.Fatalf("failed to wait on batch execution: %v", err)
		}
		if res.State != kernel.ExecStateSucceeded {
			t.Errorf("expected SUCCEEDED, got: %s", res.State)
		}
	}

	_ = rt.Shutdown(ctx)
}


