package execution

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
	"chukrun/runtime/provider"
)

const (
	eventExecutionStarted   = "execution.started"
	eventExecutionCompleted = "execution.completed"
	eventExecutionFailed    = "execution.failed"
	errExecutionNotFound    = "execution not found"
)

type ExecutionManager struct {
	registry       *provider.Registry
	pipeline       *Pipeline
	events         kernel.EventBus
	logger         observability.Logger
	telemetry      observability.Telemetry
	lifecycle      *kernel.LifecycleManager
	sem            chan struct{}
	queue          chan struct{} // kept for cap reference
	executions     map[string]*kernel.Execution
	execMu         sync.RWMutex
	waiters        map[string][]chan struct{}
	waitersMu      sync.Mutex
	streamChans    map[string]chan kernel.StreamChunk
	streamChansMu  sync.RWMutex
	scheduler      *PriorityScheduler
	scheduleSignal chan struct{}
	shutdownChan   chan struct{}
	providerActive map[string]int
	providerLimits map[string]int
	providerMu     sync.RWMutex
}

func NewExecutionManager(
	registry *provider.Registry,
	pipeline *Pipeline,
	events kernel.EventBus,
	logger observability.Logger,
	telemetry observability.Telemetry,
	lifecycle *kernel.LifecycleManager,
	concurrency kernel.ConcurrencyConfig,
) *ExecutionManager {
	globalLimit := concurrency.GlobalLimit
	queueSize := concurrency.QueueSize
	if globalLimit <= 0 {
		globalLimit = 1000
	}
	if queueSize < 0 {
		queueSize = 0
	}

	em := &ExecutionManager{
		registry:       registry,
		pipeline:       pipeline,
		events:         events,
		logger:         logger,
		telemetry:      telemetry,
		lifecycle:      lifecycle,
		sem:            make(chan struct{}, globalLimit),
		queue:          make(chan struct{}, queueSize),
		executions:     make(map[string]*kernel.Execution),
		waiters:        make(map[string][]chan struct{}),
		streamChans:    make(map[string]chan kernel.StreamChunk),
		scheduler:      NewPriorityScheduler(),
		scheduleSignal: make(chan struct{}, 1000),
		shutdownChan:   make(chan struct{}),
		providerActive: make(map[string]int),
		providerLimits: make(map[string]int),
	}

	em.providerLimits["openai-primary"] = 500
	em.providerLimits["anthropic-primary"] = 500

	go em.schedulerLoop()

	return em
}

func (em *ExecutionManager) Shutdown() {
	close(em.shutdownChan)
}

func (em *ExecutionManager) Submit(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.Execution, error) {
	if !em.lifecycle.IsReady() {
		return nil, kernel.NewError(
			kernel.ErrCategoryInternal,
			fmt.Sprintf("cannot accept execution request: runtime is not ready (current state: %s)", em.lifecycle.GetState()),
			false,
			nil,
		)
	}

	if req.ProviderRef == "" {
		return nil, kernel.NewError(kernel.ErrCategoryValidation, "provider_ref cannot be empty", false, nil)
	}

	if req.ID == "" {
		req.ID = fmt.Sprintf("exec-%016x-%08x", time.Now().UnixNano(), rand.Uint32())
	}

	exec := &kernel.Execution{
		ID:        req.ID,
		Request:   req,
		State:     kernel.ExecStatePending,
		Priority:  req.Priority,
		CreatedAt: time.Now(),
		ParentID:  req.ParentID,
	}
	if exec.Priority == "" {
		exec.Priority = kernel.PriorityClassNormal
	}

	em.execMu.Lock()
	if em.scheduler.Size() >= cap(em.queue) {
		em.execMu.Unlock()
		em.telemetry.IncrementCounter("runtime_executions_total", map[string]string{"status": "saturated"})
		return nil, kernel.NewError(kernel.ErrCategorySaturation, "execution queue is full", false, nil)
	}

	em.executions[exec.ID] = exec
	em.execMu.Unlock()

	exec.Lock()
	exec.State = kernel.ExecStateQueued
	exec.Unlock()

	em.scheduler.Enqueue(exec)

	_ = em.events.Publish(ctx, kernel.Event{
		Type:      "execution.queued",
		Timestamp: time.Now(),
		Payload: map[string]any{
			"execution_id":           exec.ID,
			"priority":               string(exec.Priority),
			"queue_depth_at_enqueue": em.scheduler.Size(),
		},
	})

	select {
	case em.scheduleSignal <- struct{}{}:
	default:
	}

	return exec, nil
}

func (em *ExecutionManager) SubmitBatch(ctx context.Context, reqs []*kernel.ExecutionRequest) (*kernel.Batch, error) {
	batchID := fmt.Sprintf("batch-%016x", time.Now().UnixNano())
	executions := make([]*kernel.Execution, 0, len(reqs))

	for _, req := range reqs {
		if req.Metadata == nil {
			req.Metadata = make(map[string]string)
		}
		req.Metadata["batch_id"] = batchID

		exec, err := em.Submit(ctx, req)
		if err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}

	return &kernel.Batch{
		ID:         batchID,
		Executions: executions,
	}, nil
}

func (em *ExecutionManager) Stream(ctx context.Context, req *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error) {
	if !em.lifecycle.IsReady() {
		return nil, kernel.NewError(
			kernel.ErrCategoryInternal,
			fmt.Sprintf("cannot accept stream request: runtime is not ready (current state: %s)", em.lifecycle.GetState()),
			false,
			nil,
		)
	}

	exec, err := em.Submit(ctx, req)
	if err != nil {
		return nil, err
	}

	outCh := make(chan kernel.StreamChunk, 16)

	em.streamChansMu.Lock()
	em.streamChans[exec.ID] = outCh
	em.streamChansMu.Unlock()

	return outCh, nil
}

func (em *ExecutionManager) Get(ctx context.Context, executionID string) (*kernel.Execution, error) {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return nil, kernel.NewError(kernel.ErrCategoryValidation, errExecutionNotFound, false, nil)
	}
	return exec, nil
}

func (em *ExecutionManager) Cancel(ctx context.Context, executionID string) error {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return kernel.NewError(kernel.ErrCategoryValidation, errExecutionNotFound, false, nil)
	}

	exec.Lock()
	defer exec.Unlock()

	state := exec.State
	if isTerminalState(state) {
		return nil
	}

	if state == kernel.ExecStatePending || state == kernel.ExecStateQueued || state == kernel.ExecStateRetrying {
		em.scheduler.Remove(executionID)

		exec.State = kernel.ExecStateCancelled
		now := time.Now()
		exec.CompletedAt = &now
		exec.Result = &kernel.ExecutionResult{
			ID:     executionID,
			Status: kernel.StatusCancelled,
			State:  kernel.ExecStateCancelled,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryCancelled),
				Message:   "execution cancelled by caller before running",
				Retryable: false,
			},
		}

		_ = em.events.Publish(ctx, kernel.Event{
			Type:      "execution.cancelled",
			Timestamp: now,
			Payload: map[string]any{
				"execution_id": executionID,
				"reason":       "caller_requested",
			},
		})

		em.notifyWaiters(executionID)
		return nil
	}

	if state == kernel.ExecStateRunning {
		if exec.CancelFunc != nil {
			exec.CancelFunc()
		}
	}

	return nil
}

func (em *ExecutionManager) Wait(ctx context.Context, executionID string) (*kernel.ExecutionResult, error) {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return nil, kernel.NewError(kernel.ErrCategoryValidation, errExecutionNotFound, false, nil)
	}

	exec.RLock()
	isTerminal := isTerminalState(exec.State)
	res := exec.Result
	exec.RUnlock()

	if isTerminal {
		return res, nil
	}

	ch := make(chan struct{})
	em.waitersMu.Lock()
	em.waiters[executionID] = append(em.waiters[executionID], ch)
	em.waitersMu.Unlock()

	select {
	case <-ch:
		em.execMu.RLock()
		exec = em.executions[executionID]
		em.execMu.RUnlock()

		exec.RLock()
		res = exec.Result
		exec.RUnlock()
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (em *ExecutionManager) schedulerLoop() {
	for {
		select {
		case <-em.shutdownChan:
			return
		case <-em.scheduleSignal:
			em.scheduleNext()
		}
	}
}

func (em *ExecutionManager) scheduleNext() {
	for {
		if len(em.sem) >= cap(em.sem) {
			return
		}

		exec := em.scheduler.Dequeue(func(e *kernel.Execution) bool {
			pName := e.Request.ProviderRef
			if pName == "" {
				return true
			}

			em.providerMu.RLock()
			active := em.providerActive[pName]
			limit, exists := em.providerLimits[pName]
			em.providerMu.RUnlock()

			if exists && active >= limit {
				return false
			}
			return true
		})

		if exec == nil {
			return
		}

		em.sem <- struct{}{}

		pName := exec.Request.ProviderRef
		if pName != "" {
			em.providerMu.Lock()
			em.providerActive[pName]++
			em.providerMu.Unlock()
		}

		go em.runExecution(exec)
	}
}

func (em *ExecutionManager) resolvePolicies(exec *kernel.Execution) (*kernel.RetryPolicy, *kernel.TimeoutPolicy) {
	retryPolicy := exec.Request.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = &kernel.RetryPolicy{
			MaxAttempts:     3,
			BackoffStrategy: kernel.BackoffExponential,
			BaseDelay:       200 * time.Millisecond,
			MaxDelay:        30 * time.Second,
			Jitter:          true,
		}
	}

	timeoutPolicy := exec.Request.TimeoutPolicy
	if timeoutPolicy == nil {
		tot := 60 * time.Second
		if exec.Request.Timeout > 0 {
			tot = exec.Request.Timeout
		}
		timeoutPolicy = &kernel.TimeoutPolicy{
			Total: tot,
		}
	}
	return retryPolicy, timeoutPolicy
}

func (em *ExecutionManager) waitBackoff(ctx context.Context, exec *kernel.Execution, retryPolicy *kernel.RetryPolicy, attempt int, lastErr error) error {
	delay := CalculateBackoff(retryPolicy, attempt-1)

	exec.Lock()
	exec.State = kernel.ExecStateRetrying
	exec.Unlock()

	_ = em.events.Publish(ctx, kernel.Event{
		Type:      "execution.retrying",
		Timestamp: time.Now(),
		Payload: map[string]any{
			"execution_id":   exec.ID,
			"attempt_number": attempt,
			"backoff_ms":     delay.Milliseconds(),
			"reason":         lastErr.Error(),
		},
	})

	select {
	case <-time.After(delay):
		exec.Lock()
		exec.State = kernel.ExecStateRunning
		exec.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (em *ExecutionManager) runSingleAttempt(ctx context.Context, exec *kernel.Execution, p provider.Provider, attempt int, timeoutPolicy *kernel.TimeoutPolicy) (*kernel.ExecutionResult, error) {
	attemptTimeout := timeoutPolicy.PerAttempt
	if attemptTimeout <= 0 {
		attemptTimeout = timeoutPolicy.Total
	}
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
	defer cancelAttempt()

	currAttempt := kernel.Attempt{
		Number:      attempt,
		StartedAt:   time.Now(),
		ProviderRef: p.Name(),
	}

	resChan := make(chan struct {
		res *kernel.ExecutionResult
		err error
	}, 1)

	go func() {
		res, err := p.Execute(attemptCtx, exec.Request)
		resChan <- struct {
			res *kernel.ExecutionResult
			err error
		}{res, err}
	}()

	select {
	case outcome := <-resChan:
		endedAt := time.Now()
		currAttempt.EndedAt = &endedAt

		if outcome.err == nil {
			exec.Lock()
			currAttempt.EndedAt = &endedAt
			exec.Attempts = append(exec.Attempts, currAttempt)
			exec.Unlock()
			return outcome.res, nil
		}

		currAttempt.Error = kernel.ToExecutionError(outcome.err)
		exec.Lock()
		exec.Attempts = append(exec.Attempts, currAttempt)
		exec.Unlock()
		return nil, outcome.err

	case <-attemptCtx.Done():
		endedAt := time.Now()
		currAttempt.EndedAt = &endedAt

		var lastErr error
		if ctx.Err() != nil {
			lastErr = kernel.NewError(kernel.ErrCategoryTimeout, "execution exceeded configured deadline", false, ctx.Err())
		} else {
			lastErr = kernel.NewError(kernel.ErrCategoryTimeout, "per-attempt timeout exceeded", true, attemptCtx.Err())
		}

		currAttempt.Error = kernel.ToExecutionError(lastErr)
		exec.Lock()
		exec.Attempts = append(exec.Attempts, currAttempt)
		exec.Unlock()
		return nil, lastErr
	}
}

func (em *ExecutionManager) isRetryable(err error, policy *kernel.RetryPolicy) bool {
	if err == nil {
		return false
	}
	retryable := true
	if platErr, ok := err.(*kernel.PlatformError); ok {
		retryable = platErr.Retryable
	}
	if policy.RetryableCheck != nil {
		retryable = policy.RetryableCheck(err)
	}
	return retryable
}

func (em *ExecutionManager) executeAttempts(ctx context.Context, exec *kernel.Execution, p provider.Provider, retryPolicy *kernel.RetryPolicy, timeoutPolicy *kernel.TimeoutPolicy) (*kernel.ExecutionResult, error) {
	var lastErr error
	var executionResult *kernel.ExecutionResult

AttemptLoop:
	for attempt := 1; attempt <= retryPolicy.MaxAttempts; attempt++ {
		if attempt > 1 {
			if err := em.waitBackoff(ctx, exec, retryPolicy, attempt, lastErr); err != nil {
				lastErr = err
				break AttemptLoop
			}
		}

		executionResult, lastErr = em.runSingleAttempt(ctx, exec, p, attempt, timeoutPolicy)
		if lastErr == nil {
			break AttemptLoop
		}

		if !em.isRetryable(lastErr, retryPolicy) {
			break AttemptLoop
		}
	}

	return executionResult, lastErr
}

func (em *ExecutionManager) translateError(lastErr error) (kernel.ExecutionState, kernel.ExecutionStatus, *kernel.ExecutionError) {
	var finalState kernel.ExecutionState
	var overallStatus kernel.ExecutionStatus
	var execErr *kernel.ExecutionError

	if platErr, ok := lastErr.(*kernel.PlatformError); ok {
		execErr = &kernel.ExecutionError{
			Category:  string(platErr.Category),
			Message:   platErr.Message,
			Retryable: platErr.Retryable,
		}
		switch platErr.Category {
		case kernel.ErrCategoryTimeout:
			finalState = kernel.ExecStateTimedOut
			overallStatus = kernel.StatusTimedOut
		case kernel.ErrCategoryCancelled:
			finalState = kernel.ExecStateCancelled
			overallStatus = kernel.StatusCancelled
		default:
			finalState = kernel.ExecStateFailed
			overallStatus = kernel.StatusFailed
		}
	} else {
		finalState = kernel.ExecStateFailed
		overallStatus = kernel.StatusFailed
		execErr = &kernel.ExecutionError{
			Category:  string(kernel.ErrCategoryInternal),
			Message:   lastErr.Error(),
			Retryable: false,
		}
	}
	return finalState, overallStatus, execErr
}

func (em *ExecutionManager) handleResult(exec *kernel.Execution, lastErr error, executionResult *kernel.ExecutionResult, startTime time.Time) {
	exec.Lock()
	defer exec.Unlock()

	nowEnded := time.Now()
	exec.CompletedAt = &nowEnded

	if lastErr == nil && executionResult != nil {
		exec.Result = executionResult
		exec.Result.State = kernel.ExecStateSucceeded
		exec.Result.Status = kernel.StatusSucceeded
		exec.Result.AttemptCount = len(exec.Attempts)
		exec.Result.RetryCount = len(exec.Attempts) - 1
		exec.Result.Attempts = exec.Attempts
		exec.Result.Duration = time.Since(startTime)
		exec.State = kernel.ExecStateSucceeded
		return
	}

	finalState, overallStatus, execErr := em.translateError(lastErr)
	exec.Result = &kernel.ExecutionResult{
		ID:           exec.ID,
		Status:       overallStatus,
		State:        finalState,
		Error:        execErr,
		Duration:     time.Since(startTime),
		AttemptCount: len(exec.Attempts),
		RetryCount:   len(exec.Attempts) - 1,
		Attempts:     exec.Attempts,
	}
	exec.State = finalState
}

func (em *ExecutionManager) publishOutcomeEvents(ctx context.Context, exec *kernel.Execution, finalState kernel.ExecutionState, timeout time.Duration) {
	nowEnded := time.Now()
	switch finalState {
	case kernel.ExecStateSucceeded:
		var tok kernel.TokenUsage
		if exec.Result.TokenUsage != nil {
			tok = *exec.Result.TokenUsage
		}
		_ = em.events.Publish(ctx, kernel.Event{
			Type:      "execution.succeeded",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id": exec.ID,
				"duration_ms":  exec.Result.Duration.Milliseconds(),
				"token_usage":  tok,
			},
		})
		em.telemetry.IncrementCounter("runtime_executions_total", map[string]string{"status": "succeeded"})
	case kernel.ExecStateTimedOut:
		_ = em.events.Publish(ctx, kernel.Event{
			Type:      "execution.timed_out",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id":          exec.ID,
				"configured_timeout_ms": timeout.Milliseconds(),
				"elapsed_ms":            exec.Result.Duration.Milliseconds(),
			},
		})
		em.telemetry.IncrementCounter("runtime_executions_total", map[string]string{"status": "timed_out"})
	case kernel.ExecStateCancelled:
		_ = em.events.Publish(ctx, kernel.Event{
			Type:      "execution.cancelled",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id": exec.ID,
				"reason":       "caller_requested",
			},
		})
		em.telemetry.IncrementCounter("runtime_executions_total", map[string]string{"status": "cancelled"})
	default:
		_ = em.events.Publish(ctx, kernel.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": exec.Result.Error.Category,
				"attempt_count":  len(exec.Attempts),
			},
		})
		em.telemetry.IncrementCounter("runtime_executions_total", map[string]string{"status": "failed"})
	}
}

func (em *ExecutionManager) runExecution(exec *kernel.Execution) {
	defer func() {
		if r := recover(); r != nil {
			exec.Lock()
			exec.State = kernel.ExecStateFailed
			now := time.Now()
			exec.CompletedAt = &now
			exec.Result = &kernel.ExecutionResult{
				ID:     exec.ID,
				Status: kernel.StatusFailed,
				State:  kernel.ExecStateFailed,
				Error: &kernel.ExecutionError{
					Category:  string(kernel.ErrCategoryInternal),
					Message:   fmt.Sprintf("internal panic: %v", r),
					Retryable: false,
				},
			}
			exec.Unlock()

			em.releaseSlots(exec)
			em.notifyWaiters(exec.ID)

			if em.logger != nil {
				em.logger.Fatal(fmt.Sprintf("panic recovered in execution %s: %v", exec.ID, r))
			}
		}
	}()

	exec.Lock()
	exec.State = kernel.ExecStateRunning
	now := time.Now()
	exec.StartedAt = &now
	exec.Unlock()

	ctx := context.Background()
	spanCtx, endSpan := em.telemetry.StartSpan(ctx, "ExecutionManager.runExecution")
	defer endSpan()

	_ = em.events.Publish(spanCtx, kernel.Event{
		Type:      "execution.started",
		Timestamp: now,
		Payload: map[string]any{
			"execution_id":   exec.ID,
			"attempt_number": 1,
			"provider_ref":   exec.Request.ProviderRef,
		},
	})

	retryPolicy, timeoutPolicy := em.resolvePolicies(exec)
	totalCtx, cancelTotal := context.WithTimeout(spanCtx, timeoutPolicy.Total)

	exec.Lock()
	exec.CancelFunc = cancelTotal
	exec.Unlock()

	defer cancelTotal()

	p, err := em.registry.Resolve(exec.Request.ProviderRef)
	if err != nil {
		exec.Lock()
		exec.State = kernel.ExecStateFailed
		nowFailed := time.Now()
		exec.CompletedAt = &nowFailed
		exec.Result = &kernel.ExecutionResult{
			ID:     exec.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryInternal),
				Message:   fmt.Sprintf("failed to resolve provider: %v", err),
				Retryable: false,
			},
		}
		exec.Unlock()

		_ = em.events.Publish(spanCtx, kernel.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowFailed,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": string(kernel.ErrCategoryInternal),
				"attempt_count":  0,
			},
		})

		em.releaseSlots(exec)
		em.notifyWaiters(exec.ID)
		return
	}

	em.streamChansMu.RLock()
	outCh, isStream := em.streamChans[exec.ID]
	em.streamChansMu.RUnlock()

	if isStream {
		em.runStreamExecution(spanCtx, totalCtx, exec, p, outCh)
		return
	}

	startTime := time.Now()
	executionResult, lastErr := em.executeAttempts(totalCtx, exec, p, retryPolicy, timeoutPolicy)
	em.handleResult(exec, lastErr, executionResult, startTime)

	exec.RLock()
	finalState := exec.State
	exec.RUnlock()

	em.publishOutcomeEvents(spanCtx, exec, finalState, timeoutPolicy.Total)
	em.releaseSlots(exec)
	em.notifyWaiters(exec.ID)
}

func (em *ExecutionManager) runStreamExecution(spanCtx, totalCtx context.Context, exec *kernel.Execution, p provider.Provider, outCh chan kernel.StreamChunk) {
	defer func() {
		em.streamChansMu.Lock()
		delete(em.streamChans, exec.ID)
		em.streamChansMu.Unlock()
		close(outCh)
	}()

	inCh, err := p.Stream(totalCtx, exec.Request)
	if err != nil {
		outCh <- kernel.StreamChunk{ID: exec.ID, Error: err}

		exec.Lock()
		exec.State = kernel.ExecStateFailed
		nowFailed := time.Now()
		exec.CompletedAt = &nowFailed
		exec.Result = &kernel.ExecutionResult{
			ID:     exec.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error:  kernel.ToExecutionError(err),
		}
		exec.Unlock()

		_ = em.events.Publish(spanCtx, kernel.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowFailed,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": string(kernel.ErrCategoryProvider),
			},
		})

		em.releaseSlots(exec)
		em.notifyWaiters(exec.ID)
		return
	}

	for {
		select {
		case chunk, ok := <-inCh:
			if !ok {
				exec.Lock()
				exec.State = kernel.ExecStateSucceeded
				nowEnded := time.Now()
				exec.CompletedAt = &nowEnded
				exec.Result = &kernel.ExecutionResult{
					ID:     exec.ID,
					Status: kernel.StatusSucceeded,
					State:  kernel.ExecStateSucceeded,
				}
				exec.Unlock()

				_ = em.events.Publish(spanCtx, kernel.Event{
					Type:      "execution.succeeded",
					Timestamp: nowEnded,
					Payload: map[string]any{
						"execution_id": exec.ID,
					},
				})

				em.releaseSlots(exec)
				em.notifyWaiters(exec.ID)
				return
			}
			outCh <- chunk
		case <-totalCtx.Done():
			outCh <- kernel.StreamChunk{ID: exec.ID, Error: totalCtx.Err()}

			exec.Lock()
			exec.State = kernel.ExecStateTimedOut
			nowEnded := time.Now()
			exec.CompletedAt = &nowEnded
			exec.Result = &kernel.ExecutionResult{
				ID:     exec.ID,
				Status: kernel.StatusTimedOut,
				State:  kernel.ExecStateTimedOut,
				Error:  kernel.ToExecutionError(totalCtx.Err()),
			}
			exec.Unlock()

			_ = em.events.Publish(spanCtx, kernel.Event{
				Type:      "execution.timed_out",
				Timestamp: nowEnded,
				Payload: map[string]any{
					"execution_id": exec.ID,
				},
			})

			em.releaseSlots(exec)
			em.notifyWaiters(exec.ID)
			return
		}
	}
}

func (em *ExecutionManager) releaseSlots(exec *kernel.Execution) {
	<-em.sem

	pName := exec.Request.ProviderRef
	if pName != "" {
		em.providerMu.Lock()
		em.providerActive[pName]--
		em.providerMu.Unlock()
	}

	select {
	case em.scheduleSignal <- struct{}{}:
	default:
	}
}

func (em *ExecutionManager) notifyWaiters(executionID string) {
	em.waitersMu.Lock()
	chans := em.waiters[executionID]
	delete(em.waiters, executionID)
	em.waitersMu.Unlock()

	for _, ch := range chans {
		close(ch)
	}
}

func (em *ExecutionManager) ActiveCount() int {
	return len(em.sem)
}

func isTerminalState(state kernel.ExecutionState) bool {
	return state == kernel.ExecStateSucceeded || state == kernel.ExecStateFailed || state == kernel.ExecStateCancelled || state == kernel.ExecStateTimedOut
}
