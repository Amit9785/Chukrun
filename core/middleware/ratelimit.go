package middleware

import (
	"context"
	"sync"
	"time"

	rtcontext "chukrun/core/context"
	"chukrun/core/errors"
	"chukrun/core/execution"
)

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

func (m *RateLimitingMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
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
		return &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategorySaturation),
				Message:   "rate limit exceeded",
				Retryable: true,
			},
		}, nil
	}

	bucket.tokens -= 1.0
	m.mu.Unlock()

	return next(ctx, req)
}
