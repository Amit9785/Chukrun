package context

import (
	stdcontext "context"
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

func mapPriorityToInt(priority kernel.PriorityClass) int32 {
	switch priority {
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

func ToProto(ctx stdcontext.Context) *ContextProto {
	var budgetProto *CostBudgetProto
	if cb := GetCostBudget(ctx); cb != nil {
		budgetProto = &CostBudgetProto{
			Limit:    cb.Limit,
			Spent:    cb.Spent(),
			Currency: cb.Currency,
		}
	}

	var deadlineUnix int64
	if d, ok := ctx.Deadline(); ok {
		deadlineUnix = d.UnixNano() / int64(time.Millisecond)
	}

	metadataCopy := make(map[string]string)
	if req := getRequestLayer(ctx); req != nil && req.Metadata != nil {
		for k, v := range req.Metadata {
			metadataCopy[k] = v
		}
	}

	return &ContextProto{
		SessionID:         GetSessionID(ctx),
		UserID:            GetUserID(ctx),
		ExecutionID:       GetExecutionID(ctx),
		ParentExecutionID: GetParentExecutionID(ctx),
		TraceID:           GetTraceID(ctx),
		DeadlineUnixMs:    deadlineUnix,
		Priority:          mapPriorityToInt(GetPriority(ctx)),
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

func FromProto(proto *ContextProto, mgr Manager) stdcontext.Context {
	if proto == nil {
		return nil
	}
	ctx := mgr.NewRootContext()
	if proto.SessionID != "" {
		ctx = WithSession(ctx, proto.SessionID, proto.UserID)
	} else if proto.UserID != "" {
		ctx = WithUser(ctx, proto.UserID, "", nil)
	}

	priorityClass := mapIntToPriority(proto.Priority)
	deadline := parseDeadline(proto.DeadlineUnixMs)
	if proto.ExecutionID != "" {
		ctx = WithExecution(ctx, proto.ExecutionID, deadline, priorityClass)
		if req := getRequestLayer(ctx); req != nil {
			if proto.ParentExecutionID != "" {
				req.ParentExecutionID = proto.ParentExecutionID
			}
			if proto.TraceID != "" {
				req.TraceID = proto.TraceID
			}
		}
	}

	if proto.CostBudget != nil {
		cb := NewCostBudget(proto.CostBudget.Limit, proto.CostBudget.Currency)
		cb.AddSpent(proto.CostBudget.Spent)
		ctx = WithCostBudget(ctx, *cb)
	}

	for k, v := range proto.Metadata {
		ctx, _ = WithMetadata(ctx, k, v)
	}

	return ctx
}
