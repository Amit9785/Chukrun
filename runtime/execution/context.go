package execution

import (
	stdcontext "context"

	"chukrun/runtime/context"
)

type contextKey string

const (
	keyTraceID    contextKey = "trace_id"
	keySessionID  contextKey = "session_id"
	keyUserID     contextKey = "user_id"
	keyCostBudget contextKey = "cost_budget"
)

// WithCorrelationIDs creates a child context with correlation IDs attached
func WithCorrelationIDs(parent stdcontext.Context, traceID, sessionID, userID string) stdcontext.Context {
	c, ok := context.FromStdContext(parent)
	if !ok {
		mgr := context.NewManager("default", "1.0.0")
		c = mgr.NewRootContext()
	}

	if sessionID != "" {
		c = c.WithSession(sessionID, userID)
	} else if userID != "" {
		c = c.WithUser(userID, "", nil)
	}

	std := c.StdContext()
	if traceID != "" {
		std = stdcontext.WithValue(std, keyTraceID, traceID)
		std = stdcontext.WithValue(std, "trace_id", traceID)
	}
	if sessionID != "" {
		std = stdcontext.WithValue(std, keySessionID, sessionID)
		std = stdcontext.WithValue(std, "session_id", sessionID)
	}
	if userID != "" {
		std = stdcontext.WithValue(std, keyUserID, userID)
		std = stdcontext.WithValue(std, "user_id", userID)
	}

	return std
}

// WithCostBudget creates a child context with a cost budget attached
func WithCostBudget(parent stdcontext.Context, budget float64) stdcontext.Context {
	c, ok := context.FromStdContext(parent)
	if !ok {
		mgr := context.NewManager("default", "1.0.0")
		c = mgr.NewRootContext()
	}

	cb := context.NewCostBudget(budget, "USD")
	c = c.WithCostBudget(*cb)

	std := stdcontext.WithValue(c.StdContext(), keyCostBudget, budget)
	return std
}

// GetTraceID extracts trace_id from context
func GetTraceID(ctx stdcontext.Context) string {
	if c, ok := context.FromStdContext(ctx); ok {
		if tid := c.TraceID(); tid != "" {
			return tid
		}
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
	if c, ok := context.FromStdContext(ctx); ok {
		if sid := c.SessionID(); sid != "" {
			return sid
		}
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
	if c, ok := context.FromStdContext(ctx); ok {
		if uid := c.UserID(); uid != "" {
			return uid
		}
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
	if c, ok := context.FromStdContext(ctx); ok {
		if cb := c.CostBudgetRemaining(); cb != nil {
			return cb.Limit
		}
	}
	if val, ok := ctx.Value(keyCostBudget).(float64); ok {
		return val
	}
	return 0.0
}
