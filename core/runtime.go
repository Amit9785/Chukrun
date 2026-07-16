package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"chukrun/core/config"
	"chukrun/core/errors"
	"chukrun/core/events"
	"chukrun/core/execution"
	"chukrun/core/lifecycle"
	"chukrun/core/middleware"
	"chukrun/core/telemetry"
)

// Runtime is the stable, versioned contract representing the engine coordinator.
type Runtime interface {
	// Initialize bootstraps all internal components. Must be called
	// exactly once before Execute or Health are used.
	Initialize(ctx context.Context) error

	// Execute runs a single unit of work and returns a normalized result.
	Execute(ctx context.Context, req *execution.ExecutionRequest) (*execution.ExecutionResult, error)

	// Stream runs a single unit of work and streams partial results.
	Stream(ctx context.Context, req *execution.ExecutionRequest) (<-chan execution.StreamChunk, error)

	// Shutdown gracefully drains in-flight executions and releases resources.
	Shutdown(ctx context.Context) error

	// Health reports the readiness of the Runtime and its components.
	Health(ctx context.Context) (*lifecycle.HealthStatus, error)

	// Restart re-runs the full bootstrap sequence on the existing Runtime.
	Restart(ctx context.Context) error
}

type CoreRuntime struct {
	mu          sync.Mutex
	cfg         *config.Config
	configFile  string
	overrides   *config.Config
	lifecycle   *lifecycle.LifecycleManager
	execution   *execution.ExecutionManager
	registry    *execution.Registry
	pipeline    execution.PipelineWrapper
	events      events.EventBus
	logger      telemetry.Logger
	telemetry   telemetry.Telemetry
	providers   []execution.Provider
	middlewares []middleware.Middleware
}

func (r *CoreRuntime) Initialize(ctx context.Context) error {
	r.mu.Lock()
	r.mu.Unlock()

	if r.lifecycle.GetState() != lifecycle.StateUninitialized {
		return errors.NewError(errors.ErrCategoryInternal, "runtime already initialized or initialization in progress", false, nil)
	}

	if err := r.lifecycle.Transition(lifecycle.StateBooting); err != nil {
		return err
	}

	// 1. Load config
	cfg, err := config.LoadConfig(r.configFile, r.overrides)
	if err != nil {
		_ = r.lifecycle.Transition(lifecycle.StateFailed)
		return err
	}
	r.cfg = cfg
	r.lifecycle.Configure(cfg.Lifecycle.RestartCooldownMS, cfg.Lifecycle.AcceptWhenDegraded)

	// 2. Setup Logger & Telemetry settings
	r.configureLoggerAndTelemetry(cfg)

	if r.logger == nil {
		r.logger = telemetry.NewPlatformLogger()
	}

	// 3. Setup Event Bus
	if r.events == nil {
		r.events = events.NewInProcessEventBus()
	}

	// 4. Setup Telemetry
	if r.telemetry == nil {
		r.telemetry = telemetry.NewInMemoryTelemetry()
	}

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.starting",
		Timestamp: time.Now(),
	})

	// 5. Setup Provider Registry
	r.registry = execution.NewRegistry()
	for _, p := range r.providers {
		if err := r.registry.Register(p); err != nil {
			_ = r.lifecycle.Transition(lifecycle.StateFailed)
			return err
		}
	}

	// 6. Setup Middleware Pipeline
	pipe := middleware.NewPipeline(r.middlewares)
	if err := pipe.Validate(); err != nil {
		_ = r.lifecycle.Transition(lifecycle.StateFailed)
		return err
	}
	r.pipeline = pipe

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
	if err := r.lifecycle.Transition(lifecycle.StateReady); err != nil {
		return err
	}

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.ready",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) Execute(ctx context.Context, req *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	if r.execution == nil {
		return nil, errors.NewError(errors.ErrCategoryInternal, "runtime execution engine not initialized", false, nil)
	}

	exec, err := r.execution.Submit(ctx, req)
	if err != nil {
		return nil, err
	}

	return r.execution.Wait(ctx, exec.ID)
}

func (r *CoreRuntime) Stream(ctx context.Context, req *execution.ExecutionRequest) (<-chan execution.StreamChunk, error) {
	if r.execution == nil {
		return nil, errors.NewError(errors.ErrCategoryInternal, "runtime execution engine not initialized", false, nil)
	}
	return r.execution.Stream(ctx, req)
}

func (r *CoreRuntime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.lifecycle.GetState()
	if state == lifecycle.StateStopped {
		return errors.NewError(errors.ErrCategoryInternal, "runtime already shutdown", false, nil)
	}

	if err := r.lifecycle.Transition(lifecycle.StateDraining); err != nil {
		return err
	}

	if r.execution != nil {
		r.execution.Shutdown()
	}

	_ = r.events.Publish(ctx, events.Event{
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

	_ = r.lifecycle.Transition(lifecycle.StateStopped)

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.shutdown_completed",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) Health(ctx context.Context) (*lifecycle.HealthStatus, error) {
	status := r.lifecycle.HealthStatus()

	registeredCount := 0
	if r.registry != nil {
		registeredCount = len(r.registry.List())
	}

	status.Components["registry"] = lifecycle.ComponentHealth{
		State:     lifecycle.HealthHealthy,
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
	if state != lifecycle.StateFailed {
		return errors.NewError(errors.ErrCategoryInternal, "cannot restart: runtime is not in FAILED state", false, nil)
	}

	if err := r.lifecycle.CheckRestartCooldown(); err != nil {
		_ = r.events.Publish(ctx, events.Event{
			Type:      "runtime.restart_rejected",
			Timestamp: time.Now(),
			Payload:   map[string]any{"reason": "cooldown_active"},
		})
		return err
	}

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.restart_requested",
		Timestamp: time.Now(),
	})

	if err := r.lifecycle.Transition(lifecycle.StateBooting); err != nil {
		return err
	}

	if r.execution != nil {
		r.execution.Shutdown()
	}

	cfg, err := config.LoadConfig(r.configFile, r.overrides)
	if err != nil {
		_ = r.lifecycle.Transition(lifecycle.StateFailed)
		return err
	}
	r.cfg = cfg
	r.lifecycle.Configure(cfg.Lifecycle.RestartCooldownMS, cfg.Lifecycle.AcceptWhenDegraded)

	r.configureLoggerAndTelemetry(cfg)
	r.logger = telemetry.NewPlatformLogger()
	r.events = events.NewInProcessEventBus()
	r.telemetry = telemetry.NewInMemoryTelemetry()

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.starting",
		Timestamp: time.Now(),
	})

	r.registry = execution.NewRegistry()
	for _, p := range r.providers {
		if err := r.registry.Register(p); err != nil {
			_ = r.lifecycle.Transition(lifecycle.StateFailed)
			return err
		}
	}

	pipe := middleware.NewPipeline(r.middlewares)
	if err := pipe.Validate(); err != nil {
		_ = r.lifecycle.Transition(lifecycle.StateFailed)
		return err
	}
	r.pipeline = pipe

	r.execution = execution.NewExecutionManager(
		r.registry,
		r.pipeline,
		r.events,
		r.logger,
		r.telemetry,
		r.lifecycle,
		cfg.Runtime.Concurrency,
	)

	if err := r.lifecycle.Transition(lifecycle.StateReady); err != nil {
		return err
	}

	_ = r.events.Publish(ctx, events.Event{
		Type:      "runtime.ready",
		Timestamp: time.Now(),
	})

	return nil
}

func (r *CoreRuntime) configureLoggerAndTelemetry(cfg *config.Config) {
	telemetry.SetGlobalLogLevel(telemetry.ParseLevel(cfg.Logging.Level))
	telemetry.ClearLogSinks()
	telemetry.SetDebugMode(cfg.DebugMode)
	telemetry.SetGlobalSamplingRate(cfg.Telemetry.Tracing.Sampling.DefaultRate)
	for k, rate := range cfg.Telemetry.Tracing.Sampling.PriorityOverrides {
		telemetry.SetPriorityOverride(k, rate)
	}

	isJSON := strings.ToLower(cfg.Logging.Format) == "json"
	for _, sinkName := range cfg.Logging.Sinks {
		switch strings.ToLower(sinkName) {
		case "stdout":
			telemetry.RegisterLogSink(telemetry.NewStdoutSink(isJSON))
		case "file":
			if cfg.Logging.FilePath != "" {
				if fileSink, err := telemetry.NewFileSink(cfg.Logging.FilePath, isJSON); err == nil {
					telemetry.RegisterLogSink(fileSink)
				}
			}
		case "otlp":
			if cfg.Logging.OTLPEndpoint != "" {
				telemetry.RegisterLogSink(telemetry.NewOTLPSink(cfg.Logging.OTLPEndpoint))
			}
		}
	}
}
