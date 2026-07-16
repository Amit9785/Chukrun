package middleware

import (
	"context"
	"fmt"
	"sync"

	rtcontext "chukrun/core/context"
	"chukrun/core/execution"
)

type CachingMiddleware struct {
	mu    sync.RWMutex
	cache map[string]*execution.ExecutionResult
}

func NewCachingMiddleware() *CachingMiddleware {
	return &CachingMiddleware{
		cache: make(map[string]*execution.ExecutionResult),
	}
}

func (m *CachingMiddleware) Name() string               { return "Caching" }
func (m *CachingMiddleware) Dependencies() []Capability { return []Capability{"authorized"} }
func (m *CachingMiddleware) Provides() []Capability     { return []Capability{"cache_checked"} }
func (m *CachingMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *CachingMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	userID := rtcontext.GetUserID(ctx)
	cacheKey := fmt.Sprintf("%s:%s:%v", userID, req.ProviderRef, req.Payload)

	m.mu.RLock()
	cachedRes, hit := m.cache[cacheKey]
	m.mu.RUnlock()

	if hit {
		return cachedRes, nil
	}

	res, err := next(ctx, req)
	if err == nil && res != nil && res.Status == execution.StatusSucceeded {
		m.mu.Lock()
		m.cache[cacheKey] = res
		m.mu.Unlock()
	}

	return res, err
}
