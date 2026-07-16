package execution

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	rtcontext "chukrun/core/context"
	"chukrun/core/config"
	"chukrun/core/errors"
	"chukrun/core/events"
	"chukrun/core/lifecycle"
	"chukrun/core/telemetry"
)

const (
	eventExecutionStarted   = "execution.started"
	eventExecutionCompleted = "execution.completed"
	eventExecutionFailed    = "execution.failed"
	errExecutionNotFound    = "execution not found"
)

type ExecutionManager struct {
	registry       *Registry
	pipeline       PipelineWrapper
	events         events.EventBus
	logger         telemetry.Logger
	telemetry      telemetry.Telemetry
	lifecycle      *lifecycle.LifecycleManager
	sem            chan struct{}
	queue          chan struct{} // kept for cap reference
	executions     map[string]*Execution
	execMu         sync.RWMutex
	waiters        map[string][]chan struct{}
	waitersMu      sync.Mutex
	streamChans    map[string]chan StreamChunk
	streamChansMu  sync.RWMutex
	scheduler      *PriorityScheduler
	scheduleSignal chan struct{}
	shutdownChan   chan struct{}
	providerActive map[string]int
	providerLimits map[string]int
	providerMu     sync.RWMutex
}

func NewExecutionManager(
	registry *Registry,
	pipeline PipelineWrapper,
	events events.EventBus,
	logger telemetry.Logger,
	tel telemetry.Telemetry,
	lifecycle *lifecycle.LifecycleManager,
	concurrency config.ConcurrencyConfig,
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
		telemetry:      tel,
		lifecycle:      lifecycle,
		sem:            make(chan struct{}, globalLimit),
		queue:          make(chan struct{}, queueSize),
		executions:     make(map[string]*Execution),
		waiters:        make(map[string][]chan struct{}),
		streamChans:    make(map[string]chan StreamChunk),
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

func (em *ExecutionManager) Submit(ctx context.Context, req *ExecutionRequest) (*Execution, error) {
	if !em.lifecycle.IsReady() {
		return nil, errors.NewError(
			errors.ErrCategoryInternal,
			fmt.Sprintf("cannot accept execution request: runtime is not ready (current state: %s)", em.lifecycle.GetState()),
			false,
			nil,
		)
	}

	if req.ProviderRef == "" {
		return nil, errors.NewError(errors.ErrCategoryValidation, "provider_ref cannot be empty", false, nil)
	}

	if req.ID == "" {
		req.ID = fmt.Sprintf("exec-%016x-%08x", time.Now().UnixNano(), rand.Uint32())
	}

	exec := &Execution{
		ID:        req.ID,
		Request:   req,
		State:     ExecStatePending,
		Priority:  req.Priority,
		CreatedAt: time.Now(),
		ParentID:  req.ParentID,
	}
	if exec.Priority == "" {
		exec.Priority = rtcontext.PriorityClassNormal
	}

	em.execMu.Lock()
	if em.scheduler.Size() >= cap(em.queue) {
		em.execMu.Unlock()
		em.telemetry.Counter("runtime_executions_total").Inc(ctx, telemetry.Label{Key: "status", Value: "saturated"})
		return nil, errors.NewError(errors.ErrCategorySaturation, "execution queue is full", false, nil)
	}

	em.executions[exec.ID] = exec
	em.execMu.Unlock()

	exec.Lock()
	exec.State = ExecStateQueued
	exec.Unlock()

	em.scheduler.Enqueue(exec)

	_ = em.events.Publish(ctx, events.Event{
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

	if em.lifecycle.GetState() == lifecycle.StateReady {
		_ = em.lifecycle.Transition(lifecycle.StateRunning)
	}

	return exec, nil
}

func (em *ExecutionManager) SubmitBatch(ctx context.Context, reqs []*ExecutionRequest) (*Batch, error) {
	batchID := fmt.Sprintf("batch-%016x", time.Now().UnixNano())
	executions := make([]*Execution, 0, len(reqs))

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

	return &Batch{
		ID:         batchID,
		Executions: executions,
	}, nil
}

func (em *ExecutionManager) Stream(ctx context.Context, req *ExecutionRequest) (<-chan StreamChunk, error) {
	if !em.lifecycle.IsReady() {
		return nil, errors.NewError(
			errors.ErrCategoryInternal,
			fmt.Sprintf("cannot accept stream request: runtime is not ready (current state: %s)", em.lifecycle.GetState()),
			false,
			nil,
		)
	}

	exec, err := em.Submit(ctx, req)
	if err != nil {
		return nil, err
	}

	outCh := make(chan StreamChunk, 16)

	em.streamChansMu.Lock()
	em.streamChans[exec.ID] = outCh
	em.streamChansMu.Unlock()

	return outCh, nil
}

func (em *ExecutionManager) Get(ctx context.Context, executionID string) (*Execution, error) {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return nil, errors.NewError(errors.ErrCategoryValidation, errExecutionNotFound, false, nil)
	}
	return exec, nil
}

func (em *ExecutionManager) Cancel(ctx context.Context, executionID string) error {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return errors.NewError(errors.ErrCategoryValidation, errExecutionNotFound, false, nil)
	}

	exec.Lock()
	defer exec.Unlock()

	state := exec.State
	if isTerminalState(state) {
		return nil
	}

	if state == ExecStatePending || state == ExecStateQueued || state == ExecStateRetrying {
		em.scheduler.Remove(executionID)

		exec.State = ExecStateCancelled
		now := time.Now()
		exec.CompletedAt = &now
		exec.Result = &ExecutionResult{
			ID:     executionID,
			Status: StatusCancelled,
			State:  ExecStateCancelled,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryCancelled),
				Message:   "execution cancelled by caller before running",
				Retryable: false,
			},
		}

		_ = em.events.Publish(ctx, events.Event{
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

	if state == ExecStateRunning {
		if exec.CancelFunc != nil {
			exec.CancelFunc()
		}
	}

	return nil
}

func (em *ExecutionManager) Wait(ctx context.Context, executionID string) (*ExecutionResult, error) {
	em.execMu.RLock()
	exec, exists := em.executions[executionID]
	em.execMu.RUnlock()

	if !exists {
		return nil, errors.NewError(errors.ErrCategoryValidation, errExecutionNotFound, false, nil)
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

		exec := em.scheduler.Dequeue(func(e *Execution) bool {
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

func (em *ExecutionManager) resolvePolicies(exec *Execution) (*RetryPolicy, *TimeoutPolicy) {
	retryPolicy := exec.Request.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = &RetryPolicy{
			MaxAttempts:     3,
			BackoffStrategy: BackoffExponential,
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
		timeoutPolicy = &TimeoutPolicy{
			Total: tot,
		}
	}
	return retryPolicy, timeoutPolicy
}

func (em *ExecutionManager) waitBackoff(ctx context.Context, exec *Execution, retryPolicy *RetryPolicy, attempt int, lastErr error) error {
	delay := CalculateBackoff(retryPolicy, attempt-1)

	exec.Lock()
	exec.State = ExecStateRetrying
	exec.Unlock()

	_ = em.events.Publish(ctx, events.Event{
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
		exec.State = ExecStateRunning
		exec.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (em *ExecutionManager) runSingleAttempt(ctx context.Context, exec *Execution, p Provider, attempt int, timeoutPolicy *TimeoutPolicy) (*ExecutionResult, error) {
	attemptTimeout := timeoutPolicy.PerAttempt
	if attemptTimeout <= 0 {
		attemptTimeout = timeoutPolicy.Total
	}
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
	defer cancelAttempt()

	currAttempt := Attempt{
		Number:      attempt,
		StartedAt:   time.Now(),
		ProviderRef: p.Name(),
	}

	resChan := make(chan struct {
		res *ExecutionResult
		err error
	}, 1)

	go func() {
		res, err := p.Execute(attemptCtx, exec.Request)
		resChan <- struct {
			res *ExecutionResult
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

			if outcome.res != nil {
				if outcome.res.TokenUsage != nil {
					em.telemetry.RecordTokenUsage(attemptCtx, telemetry.TokenUsage{
						PromptTokens:     int64(outcome.res.TokenUsage.PromptTokens),
						CompletionTokens: int64(outcome.res.TokenUsage.CompletionTokens),
						TotalTokens:      int64(outcome.res.TokenUsage.TotalTokens),
						Provider:         p.Name(),
					})
				}
				if outcome.res.Cost != nil && outcome.res.Cost.AmountUSD != 0 {
					em.telemetry.RecordCost(attemptCtx, telemetry.CostEstimate{
						AmountUSD: outcome.res.Cost.AmountUSD,
						Provider:  p.Name(),
					})
				}
			}
			return outcome.res, nil
		}

		currAttempt.Error = errors.ToExecutionError(outcome.err)
		exec.Lock()
		exec.Attempts = append(exec.Attempts, currAttempt)
		exec.Unlock()

		// Cost fallback estimation on failure
		promptText := extractPrompt(exec.Request.Payload)
		estUsage := telemetry.EstimateStreamingTokenUsage(promptText, "")
		estUsage.Provider = p.Name()
		em.telemetry.RecordTokenUsage(attemptCtx, estUsage)
		estCost := float64(estUsage.TotalTokens) * 0.0000015
		em.telemetry.RecordCost(attemptCtx, telemetry.CostEstimate{
			AmountUSD: estCost,
			Provider:  p.Name(),
		})

		return nil, outcome.err

	case <-attemptCtx.Done():
		endedAt := time.Now()
		currAttempt.EndedAt = &endedAt

		var lastErr error
		if ctx.Err() != nil {
			lastErr = errors.NewError(errors.ErrCategoryTimeout, "execution exceeded configured deadline", false, ctx.Err())
		} else {
			lastErr = errors.NewError(errors.ErrCategoryTimeout, "per-attempt timeout exceeded", true, attemptCtx.Err())
		}

		currAttempt.Error = errors.ToExecutionError(lastErr)
		exec.Lock()
		exec.Attempts = append(exec.Attempts, currAttempt)
		exec.Unlock()

		// Cost fallback estimation on timeout
		promptText := extractPrompt(exec.Request.Payload)
		estUsage := telemetry.EstimateStreamingTokenUsage(promptText, "")
		estUsage.Provider = p.Name()
		em.telemetry.RecordTokenUsage(attemptCtx, estUsage)
		estCost := float64(estUsage.TotalTokens) * 0.0000015
		em.telemetry.RecordCost(attemptCtx, telemetry.CostEstimate{
			AmountUSD: estCost,
			Provider:  p.Name(),
		})

		return nil, lastErr
	}
}

func (em *ExecutionManager) isRetryable(err error, policy *RetryPolicy) bool {
	if err == nil {
		return false
	}
	retryable := true
	if platErr, ok := err.(*errors.PlatformError); ok {
		retryable = platErr.Retryable
	}
	if policy.RetryableCheck != nil {
		retryable = policy.RetryableCheck(err)
	}
	return retryable
}

func (em *ExecutionManager) executeAttempts(ctx context.Context, exec *Execution, p Provider, retryPolicy *RetryPolicy, timeoutPolicy *TimeoutPolicy) (*ExecutionResult, error) {
	var lastErr error
	var executionResult *ExecutionResult

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

func (em *ExecutionManager) translateError(lastErr error) (ExecutionState, ExecutionStatus, *errors.ExecutionError) {
	var finalState ExecutionState
	var overallStatus ExecutionStatus
	var execErr *errors.ExecutionError

	if platErr, ok := lastErr.(*errors.PlatformError); ok {
		execErr = &errors.ExecutionError{
			Category:  string(platErr.Category),
			Message:   platErr.Message,
			Retryable: platErr.Retryable,
		}
		switch platErr.Category {
		case errors.ErrCategoryTimeout:
			finalState = ExecStateTimedOut
			overallStatus = StatusTimedOut
		case errors.ErrCategoryCancelled:
			finalState = ExecStateCancelled
			overallStatus = StatusCancelled
		default:
			finalState = ExecStateFailed
			overallStatus = StatusFailed
		}
	} else {
		finalState = ExecStateFailed
		overallStatus = StatusFailed
		execErr = &errors.ExecutionError{
			Category:  string(errors.ErrCategoryInternal),
			Message:   lastErr.Error(),
			Retryable: false,
		}
	}
	return finalState, overallStatus, execErr
}

func (em *ExecutionManager) handleResult(exec *Execution, lastErr error, executionResult *ExecutionResult, startTime time.Time) {
	exec.Lock()
	defer exec.Unlock()

	nowEnded := time.Now()
	exec.CompletedAt = &nowEnded

	if lastErr == nil && executionResult != nil {
		exec.Result = executionResult
		if executionResult.Error != nil || (executionResult.Status != "" && executionResult.Status != StatusSucceeded) {
			// Middleware short-circuit failure
			if executionResult.State == "" {
				exec.State = ExecStateFailed
				exec.Result.State = ExecStateFailed
			} else {
				exec.State = executionResult.State
			}
			exec.Result.Duration = time.Since(startTime)
			return
		}
		exec.Result.State = ExecStateSucceeded
		exec.Result.Status = StatusSucceeded
		exec.Result.AttemptCount = len(exec.Attempts)
		exec.Result.RetryCount = len(exec.Attempts) - 1
		exec.Result.Attempts = exec.Attempts
		exec.Result.Duration = time.Since(startTime)
		exec.State = ExecStateSucceeded
		return
	}

	finalState, overallStatus, execErr := em.translateError(lastErr)
	exec.Result = &ExecutionResult{
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

func (em *ExecutionManager) publishOutcomeEvents(ctx context.Context, exec *Execution, finalState ExecutionState, timeout time.Duration) {
	nowEnded := time.Now()
	switch finalState {
	case ExecStateSucceeded:
		var tok TokenUsage
		if exec.Result.TokenUsage != nil {
			tok = *exec.Result.TokenUsage
		}
		_ = em.events.Publish(ctx, events.Event{
			Type:      "execution.succeeded",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id": exec.ID,
				"duration_ms":  exec.Result.Duration.Milliseconds(),
				"token_usage":  tok,
			},
		})
		em.telemetry.Counter("runtime_executions_total").Inc(ctx, telemetry.Label{Key: "status", Value: "succeeded"})
	case ExecStateTimedOut:
		_ = em.events.Publish(ctx, events.Event{
			Type:      "execution.timed_out",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id":          exec.ID,
				"configured_timeout_ms": timeout.Milliseconds(),
				"elapsed_ms":            exec.Result.Duration.Milliseconds(),
			},
		})
		em.telemetry.Counter("runtime_executions_total").Inc(ctx, telemetry.Label{Key: "status", Value: "timed_out"})
	case ExecStateCancelled:
		_ = em.events.Publish(ctx, events.Event{
			Type:      "execution.cancelled",
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id": exec.ID,
				"reason":       "caller_requested",
			},
		})
		em.telemetry.Counter("runtime_executions_total").Inc(ctx, telemetry.Label{Key: "status", Value: "cancelled"})
	default:
		_ = em.events.Publish(ctx, events.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowEnded,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": exec.Result.Error.Category,
				"attempt_count":  len(exec.Attempts),
			},
		})
		em.telemetry.Counter("runtime_executions_total").Inc(ctx, telemetry.Label{Key: "status", Value: "failed"})
	}
}

func (em *ExecutionManager) runExecution(exec *Execution) {
	defer func() {
		if r := recover(); r != nil {
			exec.Lock()
			exec.State = ExecStateFailed
			now := time.Now()
			exec.CompletedAt = &now
			exec.Result = &ExecutionResult{
				ID:     exec.ID,
				Status: StatusFailed,
				State:  ExecStateFailed,
				Error: &errors.ExecutionError{
					Category:  string(errors.ErrCategoryInternal),
					Message:   fmt.Sprintf("internal panic: %v", r),
					Retryable: false,
				},
			}
			exec.Unlock()

			em.releaseSlots(exec)
			em.notifyWaiters(exec.ID)

			if em.logger != nil {
				em.logger.Fatal(context.Background(), fmt.Sprintf("panic recovered in execution %s: %v", exec.ID, r))
			}
		}
	}()

	exec.Lock()
	exec.State = ExecStateRunning
	now := time.Now()
	exec.StartedAt = &now
	exec.Unlock()

	ctx := context.Background()
	spanCtx, span := em.telemetry.StartSpan(ctx, "ExecutionManager.runExecution")
	defer span.End()

	_ = em.events.Publish(spanCtx, events.Event{
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
		exec.State = ExecStateFailed
		nowFailed := time.Now()
		exec.CompletedAt = &nowFailed
		exec.Result = &ExecutionResult{
			ID:     exec.ID,
			Status: StatusFailed,
			State:  ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryInternal),
				Message:   fmt.Sprintf("failed to resolve provider: %v", err),
				Retryable: false,
			},
		}
		exec.Unlock()

		_ = em.events.Publish(spanCtx, events.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowFailed,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": string(errors.ErrCategoryInternal),
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
	var executionResult *ExecutionResult
	var lastErr error

	if em.pipeline != nil {
		coreHandler := func(pipeCtx context.Context, pipeReq *ExecutionRequest) (*ExecutionResult, error) {
			return em.executeAttempts(pipeCtx, exec, p, retryPolicy, timeoutPolicy)
		}
		chained := em.pipeline.Wrap(coreHandler)
		executionResult, lastErr = chained(totalCtx, exec.Request)
	} else {
		executionResult, lastErr = em.executeAttempts(totalCtx, exec, p, retryPolicy, timeoutPolicy)
	}
	em.handleResult(exec, lastErr, executionResult, startTime)

	exec.RLock()
	finalState := exec.State
	exec.RUnlock()

	em.publishOutcomeEvents(spanCtx, exec, finalState, timeoutPolicy.Total)
	em.releaseSlots(exec)
	em.notifyWaiters(exec.ID)
}

func (em *ExecutionManager) runStreamExecution(spanCtx, totalCtx context.Context, exec *Execution, p Provider, outCh chan StreamChunk) {
	defer func() {
		em.streamChansMu.Lock()
		delete(em.streamChans, exec.ID)
		em.streamChansMu.Unlock()
		close(outCh)
	}()

	inCh, err := p.Stream(totalCtx, exec.Request)
	if err != nil {
		outCh <- StreamChunk{ID: exec.ID, Error: err}

		exec.Lock()
		exec.State = ExecStateFailed
		nowFailed := time.Now()
		exec.CompletedAt = &nowFailed
		exec.Result = &ExecutionResult{
			ID:     exec.ID,
			Status: StatusFailed,
			State:  ExecStateFailed,
			Error:  errors.ToExecutionError(err),
		}
		exec.Unlock()

		_ = em.events.Publish(spanCtx, events.Event{
			Type:      eventExecutionFailed,
			Timestamp: nowFailed,
			Payload: map[string]any{
				"execution_id":   exec.ID,
				"error_category": string(errors.ErrCategoryProvider),
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
				exec.State = ExecStateSucceeded
				nowEnded := time.Now()
				exec.CompletedAt = &nowEnded
				exec.Result = &ExecutionResult{
					ID:     exec.ID,
					Status: StatusSucceeded,
					State:  ExecStateSucceeded,
				}
				exec.Unlock()

				_ = em.events.Publish(spanCtx, events.Event{
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
			outCh <- StreamChunk{ID: exec.ID, Error: totalCtx.Err()}

			exec.Lock()
			exec.State = ExecStateTimedOut
			nowEnded := time.Now()
			exec.CompletedAt = &nowEnded
			exec.Result = &ExecutionResult{
				ID:     exec.ID,
				Status: StatusTimedOut,
				State:  ExecStateTimedOut,
				Error:  errors.ToExecutionError(totalCtx.Err()),
			}
			exec.Unlock()

			_ = em.events.Publish(spanCtx, events.Event{
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

func (em *ExecutionManager) releaseSlots(exec *Execution) {
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

func isTerminalState(state ExecutionState) bool {
	return state == ExecStateSucceeded || state == ExecStateFailed || state == ExecStateCancelled || state == ExecStateTimedOut
}

func extractPrompt(payload any) string {
	if payload == nil {
		return ""
	}
	if str, ok := payload.(string); ok {
		return str
	}
	if m, ok := payload.(map[string]any); ok {
		if promptVal, ok := m["prompt"]; ok {
			if str, ok := promptVal.(string); ok {
				return str
			}
		}
	}
	return fmt.Sprintf("%v", payload)
}
