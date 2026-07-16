package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"chukrun/runtime/execution"
	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
	"chukrun/runtime/provider"
)

type CoreRuntime struct {
	mu          sync.Mutex
	cfg         *kernel.Config
	configFile  string
	overrides   *kernel.Config
	lifecycle   *kernel.LifecycleManager
	execution   *execution.ExecutionManager
	registry    *provider.Registry
	pipeline    *execution.Pipeline
	events      kernel.EventBus
	logger      observability.Logger
	telemetry   observability.Telemetry
	providers   []provider.Provider
	middlewares []execution.Middleware
}

func (r *CoreRuntime) Initialize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lifecycle.GetState() != kernel.StateUninitialized {
		return kernel.NewError(kernel.ErrCategoryInternal, "runtime already initialized or initialization in progress", false, nil)
	}

	if err := r.lifecycle.Transition(kernel.StateBooting); err != nil {
		return err
	}

	// 1. Load config
	cfg, err := kernel.LoadConfig(r.configFile, r.overrides)
	if err != nil {
		_ = r.lifecycle.Transition(kernel.StateFailed)
		return err
	}
	r.cfg = cfg
	r.lifecycle.Configure(cfg.Lifecycle.RestartCooldownMS, cfg.Lifecycle.AcceptWhenDegraded)

	// 2. Setup Logger & Telemetry settings
	r.configureLoggerAndTelemetry(cfg)

	if r.logger == nil {
		r.logger = observability.NewPlatformLogger()
	}

	// 3. Setup Event Bus
	if r.events == nil {
		r.events = kernel.NewInProcessEventBus()
	}

	// 4. Setup Telemetry
	if r.telemetry == nil {
		r.telemetry = observability.NewInMemoryTelemetry()
	}

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.starting",
		Timestamp: time.Now(),
	})

	// 5. Setup Provider Registry
	r.registry = provider.NewRegistry()
	for _, p := range r.providers {
		if err := r.registry.Register(p); err != nil {
			_ = r.lifecycle.Transition(kernel.StateFailed)
			return err
		}
	}

	// 6. Setup Middleware Pipeline
	r.pipeline = execution.NewPipeline(r.middlewares)
	if err := r.pipeline.Validate(); err != nil {
		_ = r.lifecycle.Transition(kernel.StateFailed)
		return err
	}

	// 7. Setup Execution Manager
	r.execution = execution.NewExecutionManager(
		r.registry,
		r.pipeline,
		r.events,
		r.logger,
		r.telemetry,
		r.lifecycle,
		cfg.Runtime.Concurrency,
	)

	// Transition to READY
	if err := r.lifecycle.Transition(kernel.StateReady); err != nil {
		return err
	}

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.ready",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) Execute(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
	if r.execution == nil {
		return nil, kernel.NewError(kernel.ErrCategoryInternal, "runtime execution engine not initialized", false, nil)
	}

	exec, err := r.execution.Submit(ctx, req)
	if err != nil {
		return nil, err
	}

	return r.execution.Wait(ctx, exec.ID)
}

func (r *CoreRuntime) Stream(ctx context.Context, req *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error) {
	if r.execution == nil {
		return nil, kernel.NewError(kernel.ErrCategoryInternal, "runtime execution engine not initialized", false, nil)
	}
	return r.execution.Stream(ctx, req)
}

func (r *CoreRuntime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.lifecycle.GetState()
	if state == kernel.StateStopped {
		return kernel.NewError(kernel.ErrCategoryInternal, "runtime already shutdown", false, nil)
	}

	if err := r.lifecycle.Transition(kernel.StateDraining); err != nil {
		return err
	}

	if r.execution != nil {
		r.execution.Shutdown()
	}

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.shutdown_started",
		Timestamp: time.Now(),
	})

	// Perform graceful drain check
	drained := false
	var drainTimeout <-chan time.Time
	if r.cfg != nil {
		drainTimeout = time.After(time.Duration(r.cfg.Runtime.ShutdownTimeoutMS) * time.Millisecond)
	} else {
		drainTimeout = time.After(15 * time.Second)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for !drained {
		select {
		case <-ticker.C:
			if r.execution == nil || r.execution.ActiveCount() == 0 {
				drained = true
			}
		case <-drainTimeout:
			if r.logger != nil {
				r.logger.Warn(ctx, "shutdown deadline exceeded; cancelling remaining requests")
			}
			drained = true
		case <-ctx.Done():
			drained = true
		}
	}

	_ = r.lifecycle.Transition(kernel.StateStopped)

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.shutdown_completed",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) Health(ctx context.Context) (*kernel.HealthStatus, error) {
	status := r.lifecycle.HealthStatus()

	registeredCount := 0
	if r.registry != nil {
		registeredCount = len(r.registry.List())
	}

	status.Components["registry"] = kernel.ComponentHealth{
		State:     kernel.HealthHealthy,
		Details:   fmt.Sprintf("%d registered", registeredCount),
		Fatal:     true,
		CheckedAt: time.Now(),
	}

	return status, nil
}

func (r *CoreRuntime) Restart(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.lifecycle.GetState()
	if state != kernel.StateFailed {
		return kernel.NewError(kernel.ErrCategoryInternal, "cannot restart: runtime is not in FAILED state", false, nil)
	}

	if err := r.lifecycle.CheckRestartCooldown(); err != nil {
		_ = r.events.Publish(ctx, kernel.Event{
			Type:      "runtime.restart_rejected",
			Timestamp: time.Now(),
			Payload:   map[string]any{"reason": "cooldown_active"},
		})
		return err
	}

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.restart_requested",
		Timestamp: time.Now(),
	})

	if err := r.lifecycle.Transition(kernel.StateBooting); err != nil {
		return err
	}

	if r.execution != nil {
		r.execution.Shutdown()
	}

	cfg, err := kernel.LoadConfig(r.configFile, r.overrides)
	if err != nil {
		_ = r.lifecycle.Transition(kernel.StateFailed)
		return err
	}
	r.cfg = cfg
	r.lifecycle.Configure(cfg.Lifecycle.RestartCooldownMS, cfg.Lifecycle.AcceptWhenDegraded)

	r.configureLoggerAndTelemetry(cfg)
	r.logger = observability.NewPlatformLogger()
	r.events = kernel.NewInProcessEventBus()
	r.telemetry = observability.NewInMemoryTelemetry()

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.starting",
		Timestamp: time.Now(),
	})

	r.registry = provider.NewRegistry()
	for _, p := range r.providers {
		if err := r.registry.Register(p); err != nil {
			_ = r.lifecycle.Transition(kernel.StateFailed)
			return err
		}
	}

	r.pipeline = execution.NewPipeline(r.middlewares)
	if err := r.pipeline.Validate(); err != nil {
		_ = r.lifecycle.Transition(kernel.StateFailed)
		return err
	}
	r.execution = execution.NewExecutionManager(
		r.registry,
		r.pipeline,
		r.events,
		r.logger,
		r.telemetry,
		r.lifecycle,
		cfg.Runtime.Concurrency,
	)

	if err := r.lifecycle.Transition(kernel.StateReady); err != nil {
		return err
	}

	_ = r.events.Publish(ctx, kernel.Event{
		Type:      "runtime.ready",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) configureLoggerAndTelemetry(cfg *kernel.Config) {
	observability.SetGlobalLogLevel(observability.ParseLevel(cfg.Logging.Level))
	observability.ClearLogSinks()
	observability.SetDebugMode(cfg.DebugMode)
	observability.SetGlobalSamplingRate(cfg.Telemetry.Tracing.Sampling.DefaultRate)
	for k, rate := range cfg.Telemetry.Tracing.Sampling.PriorityOverrides {
		observability.SetPriorityOverride(k, rate)
	}

	isJSON := strings.ToLower(cfg.Logging.Format) == "json"
	for _, sinkName := range cfg.Logging.Sinks {
		switch strings.ToLower(sinkName) {
		case "stdout":
			observability.RegisterLogSink(observability.NewStdoutSink(isJSON))
		case "file":
			if cfg.Logging.FilePath != "" {
				if fileSink, err := observability.NewFileSink(cfg.Logging.FilePath, isJSON); err == nil {
					observability.RegisterLogSink(fileSink)
				}
			}
		case "otlp":
			if cfg.Logging.OTLPEndpoint != "" {
				observability.RegisterLogSink(observability.NewOTLPSink(cfg.Logging.OTLPEndpoint))
			}
		}
	}
}
