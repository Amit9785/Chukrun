package middleware

import (
	"context"
	"errors"
	"testing"

	rtcontext "chukrun/core/context"
	rtErrors "chukrun/core/errors"
	"chukrun/core/execution"
	"chukrun/core/telemetry"
)

// ----------------------------------------------------
// Mock Middlewares for Validation Tests
// ----------------------------------------------------

type dummyMiddleware struct {
	name   string
	deps   []Capability
	provs  []Capability
	failM  FailureMode
	called bool
}

func (d *dummyMiddleware) Name() string               { return d.name }
func (d *dummyMiddleware) Dependencies() []Capability { return d.deps }
func (d *dummyMiddleware) Provides() []Capability     { return d.provs }
func (d *dummyMiddleware) FailureMode() FailureMode   { return d.failM }

func (d *dummyMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	d.called = true
	return next(ctx, req)
}

func TestPipelineValidation(t *testing.T) {
	t.Run("Valid Order", func(t *testing.T) {
		mw1 := &dummyMiddleware{name: "auth", provs: []Capability{"authenticated"}}
		mw2 := &dummyMiddleware{name: "authz", deps: []Capability{"authenticated"}, provs: []Capability{"authorized"}}
		mw3 := &dummyMiddleware{name: "cache", deps: []Capability{"authorized"}, provs: []Capability{"cache_checked"}}

		pipeline := NewPipeline([]Middleware{mw1, mw2, mw3})
		if err := pipeline.Validate(); err != nil {
			t.Fatalf("expected validation to pass, got %v", err)
		}
	})

	t.Run("Unsatisfied Dependency", func(t *testing.T) {
		mw1 := &dummyMiddleware{name: "authz", deps: []Capability{"authenticated"}, provs: []Capability{"authorized"}}

		pipeline := NewPipeline([]Middleware{mw1})
		err := pipeline.Validate()
		if err == nil {
			t.Fatal("expected validation to fail due to unsatisfied dependency")
		}
		if !errors.Is(err, ErrUnsatisfiedDependency) {
			t.Errorf("expected ErrUnsatisfiedDependency, got: %v", err)
		}
	})

	t.Run("Invalid Order (Cache before Auth)", func(t *testing.T) {
		mw1 := &dummyMiddleware{name: "cache", deps: []Capability{"authorized"}, provs: []Capability{"cache_checked"}}
		mw2 := &dummyMiddleware{name: "auth", provs: []Capability{"authenticated"}}
		mw3 := &dummyMiddleware{name: "authz", deps: []Capability{"authenticated"}, provs: []Capability{"authorized"}}

		pipeline := NewPipeline([]Middleware{mw1, mw2, mw3})
		err := pipeline.Validate()
		if err == nil {
			t.Fatal("expected validation to fail due to ordering")
		}
		if !errors.Is(err, ErrUnsatisfiedDependency) {
			t.Errorf("expected ErrUnsatisfiedDependency, got: %v", err)
		}
	})

	t.Run("Duplicate Providers", func(t *testing.T) {
		mw1 := &dummyMiddleware{name: "auth1", provs: []Capability{"authenticated"}}
		mw2 := &dummyMiddleware{name: "auth2", provs: []Capability{"authenticated"}}

		pipeline := NewPipeline([]Middleware{mw1, mw2})
		err := pipeline.Validate()
		if err == nil {
			t.Fatal("expected validation to fail due to duplicate provider")
		}
		if !errors.Is(err, ErrDuplicateCapabilityProvider) {
			t.Errorf("expected ErrDuplicateCapabilityProvider, got: %v", err)
		}
	})
}

// ----------------------------------------------------
// Authentication Middleware Tests
// ----------------------------------------------------

func getTestVerifier() func(token string) (string, string, map[string]string, error) {
	return func(token string) (string, string, map[string]string, error) {
		switch token {
		case "admin-token":
			return "admin-123", "org-456", map[string]string{"role": "admin"}, nil
		case "valid-token":
			return "user-123", "org-456", map[string]string{"role": "user"}, nil
		default:
			return "", "", nil, errors.New("invalid token")
		}
	}
}

func TestAuthenticationMiddleware_PanicOnNilVerifier(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when verifier is nil")
		}
	}()
	_ = NewAuthenticationMiddleware(nil)
}

func TestAuthenticationMiddleware_MissingToken(t *testing.T) {
	mw := NewAuthenticationMiddleware(getTestVerifier())
	req := &execution.ExecutionRequest{ID: "req-1"}
	res, err := mw.Handle(context.Background(), req, func(ctx context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})

	if err != nil {
		t.Fatalf("unexpected handler error: %v", err)
	}
	if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategoryAuth) {
		t.Errorf("expected auth failure, got status %s, category %s", res.Status, res.Error.Category)
	}
}

func TestAuthenticationMiddleware_InvalidToken(t *testing.T) {
	mw := NewAuthenticationMiddleware(getTestVerifier())
	req := &execution.ExecutionRequest{
		ID:       "req-2",
		Metadata: map[string]string{"auth-token": "bad-token"},
	}
	res, err := mw.Handle(context.Background(), req, func(ctx context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})

	if err != nil {
		t.Fatalf("unexpected handler error: %v", err)
	}
	if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategoryAuth) {
		t.Errorf("expected auth failure, got status %s, category %s", res.Status, res.Error.Category)
	}
}

func TestAuthenticationMiddleware_ValidToken(t *testing.T) {
	mw := NewAuthenticationMiddleware(getTestVerifier())
	req := &execution.ExecutionRequest{
		ID:       "req-3",
		Metadata: map[string]string{"auth-token": "valid-token"},
	}
	called := false
	res, err := mw.Handle(context.Background(), req, func(ctx context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		called = true
		userID := rtcontext.GetUserID(ctx)
		if userID != "user-123" {
			t.Errorf("expected context userID user-123, got %s", userID)
		}
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})

	if err != nil {
		t.Fatalf("unexpected handler error: %v", err)
	}
	if !called {
		t.Fatal("core handler not called")
	}
	if res.Status != execution.StatusSucceeded {
		t.Errorf("expected status succeeded, got %s", res.Status)
	}
}

// ----------------------------------------------------
// Authorization Middleware Tests
// ----------------------------------------------------

func TestAuthorizationMiddleware_NoIdentity(t *testing.T) {
	mw := NewAuthorizationMiddleware()
	req := &execution.ExecutionRequest{ID: "req-1"}
	res, err := mw.Handle(context.Background(), req, func(ctx context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategoryAuth) {
		t.Errorf("expected access denied, got status %s, category %s", res.Status, res.Error.Category)
	}
}

func TestAuthorizationMiddleware_NormalPriorityAllowed(t *testing.T) {
	mw := NewAuthorizationMiddleware()
	ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", nil)
	req := &execution.ExecutionRequest{ID: "req-2", Priority: rtcontext.PriorityClassNormal}
	called := false
	_, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		called = true
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected core handler to be called")
	}
}

func TestAuthorizationMiddleware_CriticalPriorityBlockedForNonAdmin(t *testing.T) {
	mw := NewAuthorizationMiddleware()
	ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", map[string]string{"role": "user"})
	req := &execution.ExecutionRequest{ID: "req-3", Priority: rtcontext.PriorityClassCritical}
	res, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategoryAuth) {
		t.Errorf("expected critical priority to be blocked for non-admin, got status %s, category %s", res.Status, res.Error.Category)
	}
}

func TestAuthorizationMiddleware_CriticalPriorityAllowedForAdmin(t *testing.T) {
	mw := NewAuthorizationMiddleware()
	ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", map[string]string{"role": "admin"})
	req := &execution.ExecutionRequest{ID: "req-4", Priority: rtcontext.PriorityClassCritical}
	called := false
	_, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		called = true
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected critical priority to be allowed for admin")
	}
}

// ----------------------------------------------------
// Rate Limiting Middleware Tests
// ----------------------------------------------------

func TestRateLimitingMiddleware(t *testing.T) {
	mw := NewRateLimitingMiddleware(10.0, 1.0) // 10 tokens/sec, capacity 1

	t.Run("Under Limit", func(t *testing.T) {
		ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", nil)
		req := &execution.ExecutionRequest{ID: "req-1"}

		res, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != execution.StatusSucceeded {
			t.Errorf("expected succeeded status, got %s", res.Status)
		}
	})

	t.Run("Exceeded Limit", func(t *testing.T) {
		ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", nil)
		req := &execution.ExecutionRequest{ID: "req-2"}

		// Consume remaining token
		_, _ = mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
		})

		// Next request immediately should fail
		res, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategorySaturation) {
			t.Errorf("expected rate limit failure, got status %s, category %s", res.Status, res.Error.Category)
		}
	})
}

// ----------------------------------------------------
// Caching Middleware Tests
// ----------------------------------------------------

func TestCachingMiddleware(t *testing.T) {
	mw := NewCachingMiddleware()

	t.Run("Cache Miss then Cache Hit", func(t *testing.T) {
		ctx := rtcontext.WithUser(context.Background(), "user-1", "org-1", nil)
		req := &execution.ExecutionRequest{
			ID:          "req-1",
			ProviderRef: "mock-llm",
			Payload:     "hello cache",
		}

		coreCount := 0
		coreHandler := func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			coreCount++
			return &execution.ExecutionResult{ID: r.ID, Status: execution.StatusSucceeded}, nil
		}

		// First request: Cache Miss
		res1, err := mw.Handle(ctx, req, coreHandler)
		if err != nil || res1.Status != execution.StatusSucceeded {
			t.Fatalf("first execution failed: %v", err)
		}
		if coreCount != 1 {
			t.Errorf("expected core to be called 1 time, got %d", coreCount)
		}

		// Second request (same user, same payload): Cache Hit
		res2, err := mw.Handle(ctx, req, coreHandler)
		if err != nil || res2.Status != execution.StatusSucceeded {
			t.Fatalf("second execution failed: %v", err)
		}
		if coreCount != 1 {
			t.Errorf("expected core NOT to be called on cache hit, got %d", coreCount)
		}
	})

	t.Run("User Key Partitioning", func(t *testing.T) {
		req := &execution.ExecutionRequest{
			ID:          "req-2",
			ProviderRef: "mock-llm",
			Payload:     "partition testing",
		}

		coreCount := 0
		coreHandler := func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			coreCount++
			return &execution.ExecutionResult{ID: r.ID, Status: execution.StatusSucceeded}, nil
		}

		ctxUser1 := rtcontext.WithUser(context.Background(), "user-1", "org-1", nil)
		ctxUser2 := rtcontext.WithUser(context.Background(), "user-2", "org-1", nil)

		// User 1 requests -> Cache miss
		_, _ = mw.Handle(ctxUser1, req, coreHandler)
		if coreCount != 1 {
			t.Fatalf("expected 1 call, got %d", coreCount)
		}

		// User 2 requests same payload -> Cache miss (user partitioning prevents sharing)
		_, _ = mw.Handle(ctxUser2, req, coreHandler)
		if coreCount != 2 {
			t.Errorf("expected 2 calls, got %d (potential cache poisoning across users)", coreCount)
		}
	})
}

// ----------------------------------------------------
// Cost Guard Middleware Tests
// ----------------------------------------------------

func TestCostGuardMiddleware(t *testing.T) {
	mw := NewCostGuardMiddleware(0.05)

	t.Run("Within Cost Budget", func(t *testing.T) {
		budget := rtcontext.NewCostBudget(0.10, "USD")
		ctx := rtcontext.WithCostBudget(context.Background(), *budget)
		ctx = rtcontext.WithUser(ctx, "user-1", "org-1", nil)

		req := &execution.ExecutionRequest{
			ID:       "req-1",
			Metadata: map[string]string{"cost_estimate": "0.04"},
		}

		called := false
		res, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			called = true
			return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Fatal("expected core handler to run")
		}
		if res.Status != execution.StatusSucceeded {
			t.Errorf("expected status succeeded, got %s", res.Status)
		}
	})

	t.Run("Cost Budget Exceeded", func(t *testing.T) {
		budget := rtcontext.NewCostBudget(0.10, "USD")
		ctx := rtcontext.WithCostBudget(context.Background(), *budget)
		ctx = rtcontext.WithUser(ctx, "user-1", "org-1", nil)

		// Exceed budget with 0.12 estimate
		req := &execution.ExecutionRequest{
			ID:       "req-2",
			Metadata: map[string]string{"cost_estimate": "0.12"},
		}

		res, err := mw.Handle(ctx, req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != execution.StatusFailed || res.Error.Category != string(rtErrors.ErrCategorySaturation) {
			t.Errorf("expected cost limit saturated, got status %s, category %s", res.Status, res.Error.Category)
		}
	})
}

func TestLoggingMiddleware(t *testing.T) {
	log := telemetry.NewJSONLogger("info")
	logMw := NewLoggingMiddleware(log)
	if logMw.Name() != "Logging" {
		t.Errorf("unexpected name: %s", logMw.Name())
	}
	if len(logMw.Dependencies()) != 0 || len(logMw.Provides()) != 0 {
		t.Error("expected empty deps and provides")
	}
	if logMw.FailureMode() != FailOpen {
		t.Error("expected FailOpen")
	}

	req := &execution.ExecutionRequest{ID: "req-1"}
	calledLog := false
	_, _ = logMw.Handle(context.Background(), req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		calledLog = true
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if !calledLog {
		t.Fatal("expected inner handler to be called")
	}
}

func TestTelemetryMiddleware(t *testing.T) {
	tel := telemetry.NewInMemoryTelemetry()
	telMw := NewTelemetryMiddleware(tel)
	if telMw.Name() != "Telemetry" {
		t.Errorf("unexpected name: %s", telMw.Name())
	}
	if len(telMw.Dependencies()) != 0 || len(telMw.Provides()) != 0 {
		t.Error("expected empty deps and provides")
	}
	if telMw.FailureMode() != FailOpen {
		t.Error("expected FailOpen")
	}

	req := &execution.ExecutionRequest{ID: "req-1"}
	calledTel := false
	_, _ = telMw.Handle(context.Background(), req, func(c context.Context, r *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
		calledTel = true
		return &execution.ExecutionResult{Status: execution.StatusSucceeded}, nil
	})
	if !calledTel {
		t.Fatal("expected inner handler to be called")
	}
	metrics := tel.GetMetrics()
	found := false
	for _, m := range metrics {
		if m.Name == "middleware_executions_total" && m.Value == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected telemetry metric 'middleware_executions_total' with value 1 to be registered, got %v", metrics)
	}
}

func TestStandardMiddlewareMetadata(t *testing.T) {
	auth := NewAuthenticationMiddleware(func(token string) (string, string, map[string]string, error) {
		return "", "", nil, nil
	})
	if auth.Name() != "Authentication" || len(auth.Provides()) != 1 || auth.Provides()[0] != "authenticated" || auth.FailureMode() != FailClosed {
		t.Error("incorrect auth metadata")
	}

	authz := NewAuthorizationMiddleware()
	if authz.Name() != "Authorization" || len(authz.Dependencies()) != 1 || authz.Dependencies()[0] != "authenticated" || len(authz.Provides()) != 1 || authz.Provides()[0] != "authorized" || authz.FailureMode() != FailClosed {
		t.Error("incorrect authz metadata")
	}

	rate := NewRateLimitingMiddleware(10.0, 1.0)
	if rate.Name() != "RateLimiting" || len(rate.Dependencies()) != 1 || rate.Dependencies()[0] != "authenticated" || len(rate.Provides()) != 1 || rate.Provides()[0] != "rate_limit_checked" || rate.FailureMode() != FailOpen {
		t.Error("incorrect rate limiting metadata")
	}

	cache := NewCachingMiddleware()
	if cache.Name() != "Caching" || len(cache.Dependencies()) != 1 || cache.Dependencies()[0] != "authorized" || len(cache.Provides()) != 1 || cache.Provides()[0] != "cache_checked" || cache.FailureMode() != FailOpen {
		t.Error("incorrect caching metadata")
	}

	cg := NewCostGuardMiddleware(0.05)
	if cg.Name() != "CostGuard" || len(cg.Dependencies()) != 1 || cg.Dependencies()[0] != "authenticated" || len(cg.Provides()) != 1 || cg.Provides()[0] != "budget_reserved" || cg.FailureMode() != FailClosed {
		t.Error("incorrect cost guard metadata")
	}
}
