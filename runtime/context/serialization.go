package context

import (
	"time"

	"chukrun/runtime/kernel"
)

type CostBudgetProto struct {
	Limit    float64 `json:"limit"`
	Spent    float64 `json:"spent"`
	Currency string  `json:"currency"`
}

type ContextProto struct {
	SessionID         string            `json:"session_id"`
	UserID            string            `json:"user_id"`
	ExecutionID       string            `json:"execution_id"`
	ParentExecutionID string            `json:"parent_execution_id"`
	TraceID           string            `json:"trace_id"`
	DeadlineUnixMs    int64             `json:"deadline_unix_ms"`
	Priority          int32             `json:"priority"`
	Metadata          map[string]string `json:"metadata"`
	CostBudget        *CostBudgetProto  `json:"cost_budget"`
}

func mapPriorityToInt(req *RequestLayer) int32 {
	if req == nil {
		return 2 // Default: Normal
	}
	switch req.Priority {
	case kernel.PriorityClassCritical:
		return 4
	case kernel.PriorityClassHigh:
		return 3
	case kernel.PriorityClassNormal:
		return 2
	case kernel.PriorityClassLow:
		return 1
	case kernel.PriorityClassBackground:
		return 0
	default:
		return 2
	}
}

func mapIntToPriority(val int32) kernel.PriorityClass {
	switch val {
	case 4:
		return kernel.PriorityClassCritical
	case 3:
		return kernel.PriorityClassHigh
	case 2:
		return kernel.PriorityClassNormal
	case 1:
		return kernel.PriorityClassLow
	case 0:
		return kernel.PriorityClassBackground
	default:
		return kernel.PriorityClassNormal
	}
}

func (c *Context) ToProto() *ContextProto {
	var budgetProto *CostBudgetProto
	if cb := c.CostBudgetRemaining(); cb != nil {
		budgetProto = &CostBudgetProto{
			Limit:    cb.Limit,
			Spent:    cb.Spent(),
			Currency: cb.Currency,
		}
	}

	var deadlineUnix int64
	if d, ok := c.Deadline(); ok {
		deadlineUnix = d.UnixNano() / int64(time.Millisecond)
	}

	var parentExecID string
	if c.request != nil {
		parentExecID = c.request.ParentExecutionID
	}

	metadataCopy := make(map[string]string)
	if c.request != nil && c.request.Metadata != nil {
		for k, v := range c.request.Metadata {
			metadataCopy[k] = v
		}
	}

	return &ContextProto{
		SessionID:         c.SessionID(),
		UserID:            c.UserID(),
		ExecutionID:       c.ExecutionID(),
		ParentExecutionID: parentExecID,
		TraceID:           c.TraceID(),
		DeadlineUnixMs:    deadlineUnix,
		Priority:          mapPriorityToInt(c.request),
		Metadata:          metadataCopy,
		CostBudget:        budgetProto,
	}
}

func parseDeadline(deadlineUnixMs int64) time.Duration {
	if deadlineUnixMs <= 0 {
		return 0
	}
	deadlineTime := time.Unix(0, deadlineUnixMs*int64(time.Millisecond))
	deadline := time.Until(deadlineTime)
	if deadline < 0 {
		return 0
	}
	return deadline
}

func restoreExecution(ctx *Context, proto *ContextProto, deadline time.Duration, priorityClass kernel.PriorityClass) *Context {
	if proto.ExecutionID == "" {
		return ctx
	}
	ctx = ctx.WithExecution(proto.ExecutionID, deadline, priorityClass)
	if ctx.request != nil {
		if proto.ParentExecutionID != "" {
			ctx.request.ParentExecutionID = proto.ParentExecutionID
		}
		if proto.TraceID != "" {
			ctx.request.TraceID = proto.TraceID
		}
	}
	return ctx
}

func FromProto(proto *ContextProto, mgr Manager) *Context {
	if proto == nil {
		return nil
	}
	ctx := mgr.NewRootContext()
	if proto.SessionID != "" {
		ctx = ctx.WithSession(proto.SessionID, proto.UserID)
	} else if proto.UserID != "" {
		ctx = ctx.WithUser(proto.UserID, "", nil)
	}

	priorityClass := mapIntToPriority(proto.Priority)
	deadline := parseDeadline(proto.DeadlineUnixMs)
	ctx = restoreExecution(ctx, proto, deadline, priorityClass)

	if proto.CostBudget != nil {
		cb := NewCostBudget(proto.CostBudget.Limit, proto.CostBudget.Currency)
		cb.AddSpent(proto.CostBudget.Spent)
		ctx = ctx.WithCostBudget(*cb)
	}

	for k, v := range proto.Metadata {
		ctx, _ = ctx.WithMetadata(k, v)
	}

	return ctx
}
