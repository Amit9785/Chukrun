package context

import (
	"sync"
	"testing"
	"time"

	"chukrun/runtime/kernel"
)

func TestRootContextAndLayers(t *testing.T) {
	mgr := NewManager("test-inst-123", "v1.0")
	ctx := mgr.NewRootContext()

	if ctx.runtime == nil {
		t.Fatal("expected runtime layer to be initialized")
	}
	if ctx.runtime.InstanceID != "test-inst-123" || ctx.runtime.Version != "v1.0" {
		t.Errorf("unexpected runtime layer values: %+v", ctx.runtime)
	}

	if ctx.SessionID() != "" || ctx.ExecutionID() != "" || ctx.UserID() != "" || ctx.TraceID() != "" {
		t.Error("expected other layer values to be empty initially")
	}
}

func TestSessionDerivation(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	sessCtx := root.WithSession("sess-1", "user-1")
	if sessCtx.SessionID() != "sess-1" || sessCtx.UserID() != "user-1" {
		t.Errorf("WithSession failed: id=%s user=%s", sessCtx.SessionID(), sessCtx.UserID())
	}
	if root.SessionID() != "" {
		t.Error("WithSession mutated root context")
	}
}

func TestUserDerivation(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	sessCtx := root.WithSession("sess-1", "user-1")
	userCtx := sessCtx.WithUser("user-2", "org-1", map[string]string{
		"role":  "admin",
		"token": "sensitive",
	})
	if userCtx.UserID() != "user-2" {
		t.Errorf("WithUser failed to update user id, got %s", userCtx.UserID())
	}
	if userCtx.user.Claims["role"] != "admin" {
		t.Error("WithUser failed to copy safe claims")
	}
	if _, ok := userCtx.user.Claims["token"]; ok {
		t.Error("WithUser failed to filter out sensitive token claim")
	}
	if sessCtx.UserID() != "user-1" {
		t.Error("WithUser mutated parent context")
	}
}

func TestExecutionDerivation(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	sessCtx := root.WithSession("sess-1", "user-1")
	userCtx := sessCtx.WithUser("user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := userCtx.WithExecution("exec-1", 5*time.Second, kernel.PriorityClassHigh)
	if execCtx.ExecutionID() != "exec-1" {
		t.Errorf("WithExecution failed, got id=%s", execCtx.ExecutionID())
	}
	if execCtx.TraceID() != "tr-exec-1" {
		t.Errorf("WithExecution failed to generate trace id, got %s", execCtx.TraceID())
	}
	if userCtx.ExecutionID() != "" {
		t.Error("WithExecution mutated parent context")
	}
}

func TestAttemptDerivation(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	sessCtx := root.WithSession("sess-1", "user-1")
	userCtx := sessCtx.WithUser("user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := userCtx.WithExecution("exec-1", 5*time.Second, kernel.PriorityClassHigh)
	attemptCtx := execCtx.WithAttempt(2)
	if attemptCtx.AttemptNumber() != 2 {
		t.Errorf("WithAttempt failed, got %d", attemptCtx.AttemptNumber())
	}
	if execCtx.AttemptNumber() != 1 {
		t.Error("WithAttempt mutated parent context")
	}
}

func TestMetadataDerivation(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	sessCtx := root.WithSession("sess-1", "user-1")
	userCtx := sessCtx.WithUser("user-2", "org-1", map[string]string{"role": "admin"})
	execCtx := userCtx.WithExecution("exec-1", 5*time.Second, kernel.PriorityClassHigh)
	attemptCtx := execCtx.WithAttempt(2)
	metaCtx, err := attemptCtx.WithMetadata("k1", "v1")
	if err != nil {
		t.Fatalf("WithMetadata failed: %v", err)
	}
	if val, ok := metaCtx.Metadata("k1"); !ok || val != "v1" {
		t.Error("WithMetadata failed to store metadata value")
	}
	if _, ok := attemptCtx.Metadata("k1"); ok {
		t.Error("WithMetadata mutated parent context")
	}

	largeVal := make([]byte, 4100)
	_, err = metaCtx.WithMetadata("k2", string(largeVal))
	if err == nil {
		t.Error("expected error when exceeding metadata size cap")
	}
}

func TestDeadlineClamping(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

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
			parentCtx := root.WithExecution("parent", tt.parentDuration, kernel.PriorityClassNormal)
			childCtx := parentCtx.WithChildExecution("child", &tt.childDuration)

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
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	cb := NewCostBudget(10.0, "USD")
	ctx1 := root.WithExecution("exec-1", 0, kernel.PriorityClassNormal).WithCostBudget(*cb)

	// Derive children
	ctx2 := ctx1.WithChildExecution("child-1", nil)
	ctx3 := ctx1.WithChildExecution("child-2", nil)

	// Add spent on child 1
	ctx2.CostBudgetRemaining().AddSpent(1.5)
	// Add spent on child 2
	ctx3.CostBudgetRemaining().AddSpent(2.5)

	// Assert shared spent value is updated correctly across all derived contexts
	if ctx1.CostBudgetRemaining().Spent() != 4.0 {
		t.Errorf("expected 4.0 spent, got: %f", ctx1.CostBudgetRemaining().Spent())
	}
	if ctx2.CostBudgetRemaining().Spent() != 4.0 {
		t.Errorf("expected 4.0 spent on child 1, got: %f", ctx2.CostBudgetRemaining().Spent())
	}
}

func TestContextProtoSerialization(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	ctx := mgr.NewRootContext().
		WithSession("sess-1", "user-1").
		WithExecution("exec-1", 5*time.Second, kernel.PriorityClassHigh).
		WithCostBudget(*NewCostBudget(50.0, "USD"))

	ctx, _ = ctx.WithMetadata("test-key", "test-val")

	proto := ctx.ToProto()

	if proto.SessionID != "sess-1" || proto.UserID != "user-1" || proto.ExecutionID != "exec-1" {
		t.Errorf("serialization error: %+v", proto)
	}
	if proto.Metadata["test-key"] != "test-val" {
		t.Errorf("metadata serialization failed: %+v", proto.Metadata)
	}

	reconstructed := FromProto(proto, mgr)

	if reconstructed.SessionID() != "sess-1" || reconstructed.UserID() != "user-1" || reconstructed.ExecutionID() != "exec-1" {
		t.Errorf("deserialization error: %+v", reconstructed)
	}
	if val, ok := reconstructed.Metadata("test-key"); !ok || val != "test-val" {
		t.Error("deserialization failed to restore metadata")
	}
}

func TestContextConcurrencySafety(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	root := mgr.NewRootContext()

	wg := sync.WaitGroup{}
	workers := 20

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := root.WithSession("sess-con", "user-con")
			ctx = ctx.WithExecution("exec-con", 10*time.Second, kernel.PriorityClassNormal)
			for j := 0; j < 10; j++ {
				_, _ = ctx.WithMetadata("k", "v")
			}
		}(i)
	}

	wg.Wait()
}

func TestStdContextBridges(t *testing.T) {
	mgr := NewManager("test-inst", "v1.0")
	ctx := mgr.NewRootContext().WithSession("sess-std", "user-std").WithExecution("exec-std", 0, kernel.PriorityClassNormal)

	std := ctx.StdContext()
	extracted, ok := FromStdContext(std)

	if !ok {
		t.Fatal("failed to extract Context from std context")
	}
	if extracted.SessionID() != "sess-std" || extracted.ExecutionID() != "exec-std" {
		t.Errorf("incorrect extracted context values: %s %s", extracted.SessionID(), extracted.ExecutionID())
	}
}
