package execution

import (
	stdcontext "context"

	"chukrun/core/context"
)

type contextKey string

const (
	keyTraceID    contextKey = "trace_id"
	keySessionID  contextKey = "session_id"
	keyUserID     contextKey = "user_id"
	keyCostBudget contextKey = "cost_budget"
)

var (
	compatTraceID   any = "trace_id"
	compatSessionID any = "session_id"
	compatUserID    any = "user_id"
)

// WithCorrelationIDs creates a child context with correlation IDs attached
func WithCorrelationIDs(parent stdcontext.Context, traceID, sessionID, userID string) stdcontext.Context {
	if sessionID != "" {
		parent = context.WithSession(parent, sessionID, userID)
	} else if userID != "" {
		parent = context.WithUser(parent, userID, "", nil)
	}

	if traceID != "" {
		parent = stdcontext.WithValue(parent, keyTraceID, traceID)
		parent = stdcontext.WithValue(parent, compatTraceID, traceID)
	}
	if sessionID != "" {
		parent = stdcontext.WithValue(parent, keySessionID, sessionID)
		parent = stdcontext.WithValue(parent, compatSessionID, sessionID)
	}
	if userID != "" {
		parent = stdcontext.WithValue(parent, keyUserID, userID)
		parent = stdcontext.WithValue(parent, compatUserID, userID)
	}

	return parent
}

// WithCostBudget creates a child context with a cost budget attached
func WithCostBudget(parent stdcontext.Context, budget float64) stdcontext.Context {
	cb := context.NewCostBudget(budget, "USD")
	parent = context.WithCostBudget(parent, *cb)
	parent = stdcontext.WithValue(parent, keyCostBudget, budget)
	return parent
}

// GetTraceID extracts trace_id from context
func GetTraceID(ctx stdcontext.Context) string {
	if tid := context.GetTraceID(ctx); tid != "" {
		return tid
	}
	if val, ok := ctx.Value(keyTraceID).(string); ok {
		return val
	}
	if val, ok := ctx.Value("trace_id").(string); ok {
		return val
	}
	return ""
}

// GetSessionID extracts session_id from context
func GetSessionID(ctx stdcontext.Context) string {
	if sid := context.GetSessionID(ctx); sid != "" {
		return sid
	}
	if val, ok := ctx.Value(keySessionID).(string); ok {
		return val
	}
	if val, ok := ctx.Value("session_id").(string); ok {
		return val
	}
	return ""
}

// GetUserID extracts user_id from context
func GetUserID(ctx stdcontext.Context) string {
	if uid := context.GetUserID(ctx); uid != "" {
		return uid
	}
	if val, ok := ctx.Value(keyUserID).(string); ok {
		return val
	}
	if val, ok := ctx.Value("user_id").(string); ok {
		return val
	}
	return ""
}

// GetCostBudget extracts cost_budget from context
func GetCostBudget(ctx stdcontext.Context) float64 {
	if cb := context.GetCostBudget(ctx); cb != nil {
		return cb.Limit
	}
	if val, ok := ctx.Value(keyCostBudget).(float64); ok {
		return val
	}
	return 0.0
}
