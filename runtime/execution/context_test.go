package execution

import (
	"context"
	"testing"
)

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	// Initially empty checks
	if GetTraceID(ctx) != "" {
		t.Errorf("expected empty trace id, got: %s", GetTraceID(ctx))
	}
	if GetSessionID(ctx) != "" {
		t.Errorf("expected empty session id, got: %s", GetSessionID(ctx))
	}
	if GetUserID(ctx) != "" {
		t.Errorf("expected empty user id, got: %s", GetUserID(ctx))
	}
	if GetCostBudget(ctx) != 0.0 {
		t.Errorf("expected empty cost budget 0.0, got: %f", GetCostBudget(ctx))
	}

	// Write correlation IDs
	ctx = WithCorrelationIDs(ctx, "tr-1", "sess-1", "user-1")

	if GetTraceID(ctx) != "tr-1" {
		t.Errorf("expected trace id 'tr-1', got: %s", GetTraceID(ctx))
	}
	if GetSessionID(ctx) != "sess-1" {
		t.Errorf("expected session id 'sess-1', got: %s", GetSessionID(ctx))
	}
	if GetUserID(ctx) != "user-1" {
		t.Errorf("expected user id 'user-1', got: %s", GetUserID(ctx))
	}

	// Write and read cost budget
	ctx = WithCostBudget(ctx, 15.75)
	if GetCostBudget(ctx) != 15.75 {
		t.Errorf("expected cost budget 15.75, got: %f", GetCostBudget(ctx))
	}
}
