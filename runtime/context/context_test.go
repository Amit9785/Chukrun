package context

import (
	"sync"
	"testing"
	"time"

	"chukrun/runtime/kernel"
)

func TestRootContextAndLayers(t *testing.T) {
	ctx := NewRootContext("test-inst-123", "v1.0")
	rl := GetRuntime(ctx)
	if rl == nil {
		t.Fatal("expected runtime layer to be initialized")
	}
	if rl.InstanceID != "test-inst-123" || rl.Version != "v1.0" {
		t.Errorf("unexpected runtime layer values: %+v", rl)
	}
	if GetSessionID(ctx) != "" || GetExecutionID(ctx) != "" || GetUserID(ctx) != "" || GetTraceID(ctx) != "" {
		t.Error("expected other layer values to be empty initially")
	}
}

func TestSessionDerivation(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	sessCtx := WithSession(root, "sess-1", "user-1")
	if GetSessionID(sessCtx) != "sess-1" || GetUserID(sessCtx) != "user-1" {
		t.Errorf("WithSession failed: id=%s user=%s", GetSessionID(sessCtx), GetUserID(sessCtx))
	}
	if GetSessionID(root) != "" {
		t.Error("WithSession mutated root context")
	}
}

func TestUserDerivation(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	sessCtx := WithSession(root, "sess-1", "user-1")
	userCtx := WithUser(sessCtx, "user-2", "org-1", map[string]string{
		"role":  "admin",
		"token": "sensitive",
	})
	if GetUserID(userCtx) != "user-2" {
		t.Errorf("WithUser failed to update user id, got %s", GetUserID(userCtx))
	}
	ul := userCtx.Value(keyUser).(*UserLayer)
	if ul.Claims["role"] != "admin" {
		t.Error("WithUser failed to copy safe claims")
	}
	if _, ok := ul.Claims["token"]; ok {
		t.Error("WithUser failed to filter out sensitive token claim")
	}
	if GetUserID(sessCtx) != "user-1" {
		t.Error("WithUser mutated parent context")
	}
}

func TestExecutionDerivation(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	sessCtx := WithSession(root, "sess-1", "user-1")
	userCtx := WithUser(sessCtx, "user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := WithExecution(userCtx, "exec-1", 5*time.Second, kernel.PriorityClassHigh)
	if GetExecutionID(execCtx) != "exec-1" {
		t.Errorf("WithExecution failed, got id=%s", GetExecutionID(execCtx))
	}
	if GetTraceID(execCtx) != "tr-exec-1" {
		t.Errorf("WithExecution failed to generate trace id, got %s", GetTraceID(execCtx))
	}
	if GetExecutionID(userCtx) != "" {
		t.Error("WithExecution mutated parent context")
	}
}

func TestAttemptDerivation(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	sessCtx := WithSession(root, "sess-1", "user-1")
	userCtx := WithUser(sessCtx, "user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := WithExecution(userCtx, "exec-1", 5*time.Second, kernel.PriorityClassHigh)
	attemptCtx := WithAttempt(execCtx, 2)
	if GetAttemptNumber(attemptCtx) != 2 {
		t.Errorf("WithAttempt failed, got %d", GetAttemptNumber(attemptCtx))
	}
	if GetAttemptNumber(execCtx) != 1 {
		t.Error("WithAttempt mutated parent context")
	}
}

func TestMetadataDerivation(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	sessCtx := WithSession(root, "sess-1", "user-1")
	userCtx := WithUser(sessCtx, "user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := WithExecution(userCtx, "exec-1", 5*time.Second, kernel.PriorityClassHigh)
	attemptCtx := WithAttempt(execCtx, 2)
	metaCtx, err := WithMetadata(attemptCtx, "k1", "v1")
	if err != nil {
		t.Fatalf("WithMetadata failed: %v", err)
	}
	if val, ok := GetMetadata(metaCtx, "k1"); !ok || val != "v1" {
		t.Error("WithMetadata failed to store metadata value")
	}
	if _, ok := GetMetadata(attemptCtx, "k1"); ok {
		t.Error("WithMetadata mutated parent context")
	}

	largeVal := make([]byte, 4100)
	_, err = WithMetadata(metaCtx, "k2", string(largeVal))
	if err == nil {
		t.Error("expected error when exceeding metadata size cap")
	}
}

func TestDeadlineClamping(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")

	tests := []struct {
		name           string
		parentDuration time.Duration
		childDuration  time.Duration
		expectClamped  bool
	}{
		{
			name:           "Child shorter than parent",
			parentDuration: 10 * time.Second,
			childDuration:  5 * time.Second,
			expectClamped:  false,
		},
		{
			name:           "Child longer than parent (clamping)",
			parentDuration: 5 * time.Second,
			childDuration:  10 * time.Second,
			expectClamped:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentCtx := WithExecution(root, "parent", tt.parentDuration, kernel.PriorityClassNormal)
			childCtx := WithChildExecution(parentCtx, "child", &tt.childDuration)

			parentDeadline, _ := parentCtx.Deadline()
			childDeadline, _ := childCtx.Deadline()

			if tt.expectClamped {
				if childDeadline.After(parentDeadline) {
					t.Errorf("child deadline %v was not clamped to parent deadline %v", childDeadline, parentDeadline)
				}
			} else {
				if childDeadline.After(parentDeadline) {
					t.Errorf("child deadline %v is somehow after parent deadline %v", childDeadline, parentDeadline)
				}
			}
		})
	}
}

func TestSessionStateMutableAndGC(t *testing.T) {
	store := GetSessionStore()
	store.SetExpiry(100 * time.Millisecond)

	state := store.GetOrCreate("session-x")
	state.Set("foo", "bar")

	if val, ok := state.Get("foo"); !ok || val != "bar" {
		t.Errorf("expected Get('foo') to return 'bar', got: %v", val)
	}

	state.Delete("foo")
	if _, ok := state.Get("foo"); ok {
		t.Error("expected Delete to remove key")
	}

	// Test Session Expiry GC
	store.GetOrCreate("session-expire")
	time.Sleep(150 * time.Millisecond)

	// Trigger GC manually by accessing another session
	store.GetOrCreate("session-active")

	store.mu.Lock()
	_, exists := store.sessions["session-expire"]
	store.mu.Unlock()

	if exists {
		t.Error("session-expire was not garbage collected after expiry duration")
	}
}

func TestCostBudgetAtomicUpdating(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")

	cb := NewCostBudget(10.0, "USD")
	ctx1 := WithCostBudget(WithExecution(root, "exec-1", 0, kernel.PriorityClassNormal), *cb)

	// Derive children
	ctx2 := WithChildExecution(ctx1, "child-1", nil)
	ctx3 := WithChildExecution(ctx1, "child-2", nil)

	// Add spent on child 1
	GetCostBudget(ctx2).AddSpent(1.5)
	// Add spent on child 2
	GetCostBudget(ctx3).AddSpent(2.5)

	// Assert shared spent value is updated correctly across all derived contexts
	if GetCostBudget(ctx1).Spent() != 4.0 {
		t.Errorf("expected 4.0 spent, got: %f", GetCostBudget(ctx1).Spent())
	}
	if GetCostBudget(ctx2).Spent() != 4.0 {
		t.Errorf("expected 4.0 spent on child 1, got: %f", GetCostBudget(ctx2).Spent())
	}
}

func TestContextProtoSerialization(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	priorities := []kernel.PriorityClass{
		kernel.PriorityClassCritical,
		kernel.PriorityClassHigh,
		kernel.PriorityClassNormal,
		kernel.PriorityClassLow,
		kernel.PriorityClassBackground,
	}

	for _, prio := range priorities {
		ctx := WithCostBudget(
			WithExecution(
				WithSession(mgr.NewRootContext(), "sess-1", "user-1"),
				"exec-1", 5*time.Second, prio,
			),
			*NewCostBudget(50.0, "USD"),
		)

		ctx, _ = WithMetadata(ctx, "test-key", "test-val")

		proto := ToProto(ctx)

		if proto.SessionID != "sess-1" || proto.UserID != "user-1" || proto.ExecutionID != "exec-1" {
			t.Errorf("serialization error: %+v", proto)
		}
		if proto.Metadata["test-key"] != "test-val" {
			t.Errorf("metadata serialization failed: %+v", proto.Metadata)
		}

		reconstructed := FromProto(proto, mgr)

		if GetSessionID(reconstructed) != "sess-1" || GetUserID(reconstructed) != "user-1" || GetExecutionID(reconstructed) != "exec-1" {
			t.Errorf("deserialization error: %+v", reconstructed)
		}
		if val, ok := GetMetadata(reconstructed, "test-key"); !ok || val != "test-val" {
			t.Error("deserialization failed to restore metadata")
		}

		// Verify priority matches
		reqL := reconstructed.Value(keyRequest).(*RequestLayer)
		if reqL.Priority != prio {
			t.Errorf("expected priority %s, got %s", prio, reqL.Priority)
		}
	}
}

func TestContextConcurrencySafety(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")

	wg := sync.WaitGroup{}
	workers := 20

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := WithSession(root, "sess-con", "user-con")
			ctx = WithExecution(ctx, "exec-con", 10*time.Second, kernel.PriorityClassNormal)
			for j := 0; j < 10; j++ {
				_, _ = WithMetadata(ctx, "k", "v")
			}
		}(i)
	}

	wg.Wait()
}

func TestNativeCompatibility(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")
	ctx := WithExecution(WithSession(root, "sess-std", "user-std"), "exec-std", 0, kernel.PriorityClassNormal)

	if GetSessionID(ctx) != "sess-std" || GetExecutionID(ctx) != "exec-std" {
		t.Error("failed standard context correlation verification")
	}
}

func TestContextMerge(t *testing.T) {
	root1 := NewRootContext("inst-1", "v1.0")
	root2 := NewRootContext("inst-2", "v1.0")

	_, err := Merge(root1, root2, MergeRules{OnConflict: OverlayWins})
	if err != ErrCrossRuntimeMerge {
		t.Errorf("expected ErrCrossRuntimeMerge, got: %v", err)
	}

	ctx1 := WithSession(root1, "sess-1", "user-1")
	ctx1, _ = WithMetadata(ctx1, "k1", "v1")

	ctx2 := WithSession(root1, "sess-1", "user-1")
	ctx2, _ = WithMetadata(ctx2, "k1", "v2")
	ctx2, _ = WithMetadata(ctx2, "k2", "v3")

	mergedOverlay, err := Merge(ctx1, ctx2, MergeRules{OnConflict: OverlayWins})
	if err != nil {
		t.Fatalf("failed overlay merge: %v", err)
	}
	v1, _ := GetMetadata(mergedOverlay, "k1")
	v2, _ := GetMetadata(mergedOverlay, "k2")
	if v1 != "v2" || v2 != "v3" {
		t.Errorf("OverlayWins strategy failed: k1=%s k2=%s", v1, v2)
	}

	_, err = Merge(ctx1, ctx2, MergeRules{OnConflict: ErrorOnConflict})
	if err == nil {
		t.Error("expected error on conflict but got none")
	}
}

func BenchmarkContextDerivation(b *testing.B) {
	root := NewRootContext("inst-1", "v1.0")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := WithSession(root, "sess", "user")
		ctx = WithExecution(ctx, "exec", 5*time.Second, kernel.PriorityClassNormal)
		_, _ = WithMetadata(ctx, "k", "v")
	}
}

func BenchmarkVariableLookup(b *testing.B) {
	root := NewRootContext("inst-1", "v1.0")
	ctx := WithSession(root, "sess", "user")
	ctx = WithExecution(ctx, "exec", 5*time.Second, kernel.PriorityClassNormal)
	ctx, _ = WithMetadata(ctx, "k", "v")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetTraceID(ctx)
		_ = GetSessionID(ctx)
		_, _ = GetMetadata(ctx, "k")
	}
}

func BenchmarkSerialization(b *testing.B) {
	root := NewRootContext("inst-1", "v1.0")
	ctx := WithSession(root, "sess", "user")
	ctx = WithExecution(ctx, "exec", 5*time.Second, kernel.PriorityClassNormal)
	ctx, _ = WithMetadata(ctx, "k", "v")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto := ToProto(ctx)
		_ = FromProto(proto, NewManager("inst-1", "v1.0"))
	}
}

func TestContextMergeAdvanced(t *testing.T) {
	root := NewRootContext("inst-1", "v1.0")

	// 1. User layer conflicts
	u1 := WithUser(root, "user-1", "org-1", map[string]string{"role": "admin"})
	u2 := WithUser(root, "user-2", "org-2", map[string]string{"role": "user"})

	// Overlay wins
	mUserOverlay, err := Merge(u1, u2, MergeRules{OnConflict: OverlayWins})
	if err != nil {
		t.Fatalf("failed merging user overlay: %v", err)
	}
	if GetUserID(mUserOverlay) != "user-2" {
		t.Errorf("expected user-2, got %s", GetUserID(mUserOverlay))
	}

	// Base wins
	mUserBase, err := Merge(u1, u2, MergeRules{OnConflict: BaseWins})
	if err != nil {
		t.Fatalf("failed merging user base: %v", err)
	}
	if GetUserID(mUserBase) != "user-1" {
		t.Errorf("expected user-1, got %s", GetUserID(mUserBase))
	}

	// Error on conflict
	_, err = Merge(u1, u2, MergeRules{OnConflict: ErrorOnConflict})
	if err == nil {
		t.Error("expected error merging conflicting user layers under ErrorOnConflict")
	}

	// 2. Request layer conflicts
	req1 := WithExecution(root, "exec-1", 5*time.Second, kernel.PriorityClassHigh)
	req2 := WithExecution(root, "exec-2", 10*time.Second, kernel.PriorityClassNormal)

	// Overlay wins request
	mReqOverlay, err := Merge(req1, req2, MergeRules{OnConflict: OverlayWins})
	if err != nil {
		t.Fatalf("failed merging request overlay: %v", err)
	}
	if GetExecutionID(mReqOverlay) != "exec-2" {
		t.Errorf("expected exec-2, got %s", GetExecutionID(mReqOverlay))
	}

	// Error on conflict request
	_, err = Merge(req1, req2, MergeRules{OnConflict: ErrorOnConflict})
	if err == nil {
		t.Error("expected error merging conflicting request layers under ErrorOnConflict")
	}
}

func TestSensitiveContextVariables(t *testing.T) {
	root := NewRootContext("test-inst", "v1.0")

	ctxKey := WithSensitiveKey(root, "api_key")
	if !IsSensitiveKey(ctxKey, "api_key") {
		t.Error("expected api_key to be marked sensitive")
	}
	if IsSensitiveKey(ctxKey, "other") {
		t.Error("expected other key to not be marked sensitive")
	}

	ctxVar := WithSensitiveVariable(root, "secret_var", "secret_val")
	if !IsSensitiveKey(ctxVar, "secret_var") {
		t.Error("expected secret_var to be marked sensitive")
	}
}
