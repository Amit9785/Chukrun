package middleware

import (
	"context"
	"strconv"

	rtcontext "chukrun/core/context"
	"chukrun/core/errors"
	"chukrun/core/execution"
)

type CostGuardMiddleware struct {
	defaultEstimate float64
}

func NewCostGuardMiddleware(defaultEstimate float64) *CostGuardMiddleware {
	return &CostGuardMiddleware{defaultEstimate: defaultEstimate}
}

func (m *CostGuardMiddleware) Name() string               { return "CostGuard" }
func (m *CostGuardMiddleware) Dependencies() []Capability { return []Capability{"authenticated"} }
func (m *CostGuardMiddleware) Provides() []Capability     { return []Capability{"budget_reserved"} }
func (m *CostGuardMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *CostGuardMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	budget := rtcontext.GetCostBudget(ctx)
	if budget == nil {
		// No budget configured, bypass checks
		return next(ctx, req)
	}

	estimate := m.defaultEstimate
	if req.Metadata != nil {
		if estStr, ok := req.Metadata["cost_estimate"]; ok {
			if parsed, err := strconv.ParseFloat(estStr, 64); err == nil {
				estimate = parsed
			}
		}
	}

	if !budget.TryReserve(estimate) {
		return &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategorySaturation),
				Message:   "cost budget exceeded",
				Retryable: false,
			},
		}, nil
	}

	return next(ctx, req)
}
