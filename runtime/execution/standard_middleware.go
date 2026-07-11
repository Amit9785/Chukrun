package execution

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	rtcontext "chukrun/runtime/context"
	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
)

// ----------------------------------------------------
// Authentication Middleware
// ----------------------------------------------------

type AuthenticationMiddleware struct {
	verifier func(token string) (string, string, map[string]string, error)
}

func NewAuthenticationMiddleware(verifier func(token string) (string, string, map[string]string, error)) *AuthenticationMiddleware {
	if verifier == nil {
		panic("NewAuthenticationMiddleware: verifier function cannot be nil")
	}
	return &AuthenticationMiddleware{verifier: verifier}
}

func (m *AuthenticationMiddleware) Name() string               { return "Authentication" }
func (m *AuthenticationMiddleware) Dependencies() []Capability { return nil }
func (m *AuthenticationMiddleware) Provides() []Capability     { return []Capability{"authenticated"} }
func (m *AuthenticationMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *AuthenticationMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	var token string
	if req.Metadata != nil {
		token = req.Metadata["auth-token"]
		if token == "" {
			token = req.Metadata["Authorization"]
		}
	}

	if token == "" {
		res := &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryAuth),
				Message:   "missing authentication token",
				Retryable: false,
			},
		}
		return res, nil
	}

	userID, orgID, claims, err := m.verifier(token)
	if err != nil {
		res := &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryAuth),
				Message:   fmt.Sprintf("authentication failed: %v", err),
				Retryable: false,
			},
		}
		return res, nil
	}

	// Derive user context
	derivedCtx := rtcontext.WithUser(ctx, userID, orgID, claims)
	return next(derivedCtx, req)
}

// ----------------------------------------------------
// Authorization Middleware
// ----------------------------------------------------

type AuthorizationMiddleware struct{}

func NewAuthorizationMiddleware() *AuthorizationMiddleware {
	return &AuthorizationMiddleware{}
}

func (m *AuthorizationMiddleware) Name() string               { return "Authorization" }
func (m *AuthorizationMiddleware) Dependencies() []Capability { return []Capability{"authenticated"} }
func (m *AuthorizationMiddleware) Provides() []Capability     { return []Capability{"authorized"} }
func (m *AuthorizationMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *AuthorizationMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	ul := rtcontext.GetUserLayer(ctx)
	if ul == nil || ul.UserID == "" {
		return &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryAuth),
				Message:   "unauthorized: user identity not found",
				Retryable: false,
			},
		}, nil
	}

	// Check role and priority escalation
	role := ul.Claims["role"]
	if req.Priority == kernel.PriorityClassCritical && role != "admin" {
		return &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategoryAuth),
				Message:   "unauthorized: critical priority class requires admin privileges",
				Retryable: false,
			},
		}, nil
	}

	return next(ctx, req)
}

// ----------------------------------------------------
// Rate Limiting Middleware
// ----------------------------------------------------

type tokenBucket struct {
	tokens   float64
	lastTick time.Time
}

type RateLimitingMiddleware struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     float64 // tokens per second
	capacity float64 // max burst size
}

func NewRateLimitingMiddleware(rate, capacity float64) *RateLimitingMiddleware {
	return &RateLimitingMiddleware{
		buckets:  make(map[string]*tokenBucket),
		rate:     rate,
		capacity: capacity,
	}
}

func (m *RateLimitingMiddleware) Name() string               { return "RateLimiting" }
func (m *RateLimitingMiddleware) Dependencies() []Capability { return []Capability{"authenticated"} }
func (m *RateLimitingMiddleware) Provides() []Capability     { return []Capability{"rate_limit_checked"} }
func (m *RateLimitingMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *RateLimitingMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	userID := rtcontext.GetUserID(ctx)
	if userID == "" {
		// Fail open as it's a convenience check and user context isn't resolved (unexpected)
		return next(ctx, req)
	}

	m.mu.Lock()
	bucket, exists := m.buckets[userID]
	if !exists {
		bucket = &tokenBucket{
			tokens:   m.capacity,
			lastTick: time.Now(),
		}
		m.buckets[userID] = bucket
	}

	now := time.Now()
	elapsed := now.Sub(bucket.lastTick).Seconds()
	bucket.lastTick = now
	bucket.tokens = bucket.tokens + elapsed*m.rate
	if bucket.tokens > m.capacity {
		bucket.tokens = m.capacity
	}

	if bucket.tokens < 1.0 {
		m.mu.Unlock()
		return &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategorySaturation),
				Message:   "rate limit exceeded",
				Retryable: true,
			},
		}, nil
	}

	bucket.tokens -= 1.0
	m.mu.Unlock()

	return next(ctx, req)
}

// ----------------------------------------------------
// Caching Middleware
// ----------------------------------------------------

type CachingMiddleware struct {
	mu    sync.RWMutex
	cache map[string]*kernel.ExecutionResult
}

func NewCachingMiddleware() *CachingMiddleware {
	return &CachingMiddleware{
		cache: make(map[string]*kernel.ExecutionResult),
	}
}

func (m *CachingMiddleware) Name() string               { return "Caching" }
func (m *CachingMiddleware) Dependencies() []Capability { return []Capability{"authorized"} }
func (m *CachingMiddleware) Provides() []Capability     { return []Capability{"cache_checked"} }
func (m *CachingMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *CachingMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	userID := rtcontext.GetUserID(ctx)
	cacheKey := fmt.Sprintf("%s:%s:%v", userID, req.ProviderRef, req.Payload)

	m.mu.RLock()
	cachedRes, hit := m.cache[cacheKey]
	m.mu.RUnlock()

	if hit {
		return cachedRes, nil
	}

	res, err := next(ctx, req)
	if err == nil && res != nil && res.Status == kernel.StatusSucceeded {
		m.mu.Lock()
		m.cache[cacheKey] = res
		m.mu.Unlock()
	}

	return res, err
}

// ----------------------------------------------------
// Logging Middleware
// ----------------------------------------------------

type LoggingMiddleware struct {
	logger observability.Logger
}

func NewLoggingMiddleware(logger observability.Logger) *LoggingMiddleware {
	return &LoggingMiddleware{logger: logger}
}

func (m *LoggingMiddleware) Name() string               { return "Logging" }
func (m *LoggingMiddleware) Dependencies() []Capability { return nil }
func (m *LoggingMiddleware) Provides() []Capability     { return nil }
func (m *LoggingMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *LoggingMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	start := time.Now()
	if m.logger != nil {
		m.logger.Info(fmt.Sprintf("middleware: starting execution request %s", req.ID))
	}

	res, err := next(ctx, req)

	if m.logger != nil {
		duration := time.Since(start)
		status := "Unknown"
		if res != nil {
			status = string(res.Status)
		}
		m.logger.Info(fmt.Sprintf("middleware: finished execution request %s in %v with status %s", req.ID, duration, status))
	}

	return res, err
}

// ----------------------------------------------------
// Telemetry Middleware
// ----------------------------------------------------

type TelemetryMiddleware struct {
	telemetry observability.Telemetry
}

func NewTelemetryMiddleware(telemetry observability.Telemetry) *TelemetryMiddleware {
	return &TelemetryMiddleware{telemetry: telemetry}
}

func (m *TelemetryMiddleware) Name() string               { return "Telemetry" }
func (m *TelemetryMiddleware) Dependencies() []Capability { return nil }
func (m *TelemetryMiddleware) Provides() []Capability     { return nil }
func (m *TelemetryMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *TelemetryMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	if m.telemetry != nil {
		m.telemetry.IncrementCounter("middleware_executions_total", map[string]string{"name": m.Name()})
	}
	return next(ctx, req)
}

// ----------------------------------------------------
// Cost Guard Middleware
// ----------------------------------------------------

type CostGuardMiddleware struct {
	defaultEstimate float64
}

func NewCostGuardMiddleware(defaultEstimate float64) *CostGuardMiddleware {
	return &CostGuardMiddleware{defaultEstimate: defaultEstimate}
}

func (m *CostGuardMiddleware) Name() string               { return "CostGuard" }
func (m *CostGuardMiddleware) Dependencies() []Capability { return []Capability{"authenticated"} }
func (m *CostGuardMiddleware) Provides() []Capability     { return []Capability{"budget_reserved"} }
func (m *CostGuardMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *CostGuardMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	budget := rtcontext.GetCostBudget(ctx)
	if budget == nil {
		// No budget configured, bypass checks
		return next(ctx, req)
	}

	estimate := m.defaultEstimate
	if req.Metadata != nil {
		if estStr, ok := req.Metadata["cost_estimate"]; ok {
			if parsed, err := strconv.ParseFloat(estStr, 64); err == nil {
				estimate = parsed
			}
		}
	}

	if !budget.TryReserve(estimate) {
		return &kernel.ExecutionResult{
			ID:     req.ID,
			Status: kernel.StatusFailed,
			State:  kernel.ExecStateFailed,
			Error: &kernel.ExecutionError{
				Category:  string(kernel.ErrCategorySaturation),
				Message:   "cost budget exceeded",
				Retryable: false,
			},
		}, nil
	}

	return next(ctx, req)
}
