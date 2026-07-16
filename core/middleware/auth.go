package middleware

import (
	"context"
	"fmt"

	rtcontext "chukrun/core/context"
	"chukrun/core/errors"
	"chukrun/core/execution"
)

type AuthenticationMiddleware struct {
	verifier func(token string) (string, string, map[string]string, error)
}

func NewAuthenticationMiddleware(verifier func(token string) (string, string, map[string]string, error)) *AuthenticationMiddleware {
	if verifier == nil {
		panic("NewAuthenticationMiddleware: verifier function cannot be nil")
	}
	return &AuthenticationMiddleware{verifier: verifier}
}

func (m *AuthenticationMiddleware) Name() string               { return "Authentication" }
func (m *AuthenticationMiddleware) Dependencies() []Capability { return nil }
func (m *AuthenticationMiddleware) Provides() []Capability     { return []Capability{"authenticated"} }
func (m *AuthenticationMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *AuthenticationMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	var token string
	if req.Metadata != nil {
		token = req.Metadata["auth-token"]
		if token == "" {
			token = req.Metadata["Authorization"]
		}
	}

	if token == "" {
		res := &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryAuth),
				Message:   "missing authentication token",
				Retryable: false,
			},
		}
		return res, nil
	}

	userID, orgID, claims, err := m.verifier(token)
	if err != nil {
		res := &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryAuth),
				Message:   fmt.Sprintf("authentication failed: %v", err),
				Retryable: false,
			},
		}
		return res, nil
	}

	// Derive user context
	derivedCtx := rtcontext.WithUser(ctx, userID, orgID, claims)
	return next(derivedCtx, req)
}

type AuthorizationMiddleware struct{}

func NewAuthorizationMiddleware() *AuthorizationMiddleware {
	return &AuthorizationMiddleware{}
}

func (m *AuthorizationMiddleware) Name() string               { return "Authorization" }
func (m *AuthorizationMiddleware) Dependencies() []Capability { return []Capability{"authenticated"} }
func (m *AuthorizationMiddleware) Provides() []Capability     { return []Capability{"authorized"} }
func (m *AuthorizationMiddleware) FailureMode() FailureMode   { return FailClosed }

func (m *AuthorizationMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	ul := rtcontext.GetUserLayer(ctx)
	if ul == nil || ul.UserID == "" {
		return &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryAuth),
				Message:   "unauthorized: user identity not found",
				Retryable: false,
			},
		}, nil
	}

	// Check role and priority escalation
	role := ul.Claims["role"]
	if req.Priority == rtcontext.PriorityClassCritical && role != "admin" {
		return &execution.ExecutionResult{
			ID:     req.ID,
			Status: execution.StatusFailed,
			State:  execution.ExecStateFailed,
			Error: &errors.ExecutionError{
				Category:  string(errors.ErrCategoryAuth),
				Message:   "unauthorized: critical priority class requires admin privileges",
				Retryable: false,
			},
		}, nil
	}

	return next(ctx, req)
}
