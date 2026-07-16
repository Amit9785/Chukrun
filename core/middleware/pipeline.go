package middleware

import (
	"context"
	"errors"
	"fmt"

	"chukrun/core/execution"
)

// Capability defines abstract capabilities provided or depended on by middleware
type Capability string

// FailureMode dictates how middleware behaves when encountering internal errors
type FailureMode int

const (
	FailClosed FailureMode = iota
	FailOpen
)

// Sentinel errors for pipeline validation
var (
	ErrInvalidMiddlewareOrder      = errors.New("invalid middleware order")
	ErrDuplicateCapabilityProvider = errors.New("duplicate capability provider")
	ErrUnsatisfiedDependency       = errors.New("unsatisfied dependency")
)

// Middleware wraps request executions with pre/post hooks and declares metadata
type Middleware interface {
	Name() string
	Dependencies() []Capability
	Provides() []Capability
	FailureMode() FailureMode
	Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error)
}

// Pipeline holds and chains registered middleware
type Pipeline struct {
	middlewares []Middleware
}

func NewPipeline(middlewares []Middleware) *Pipeline {
	return &Pipeline{
		middlewares: middlewares,
	}
}

// Register appends a new middleware to the pipeline
func (p *Pipeline) Register(m Middleware) error {
	p.middlewares = append(p.middlewares, m)
	return nil
}

// Validate topological order of middleware capabilities
func (p *Pipeline) Validate() error {
	provided := make(map[Capability]bool)
	for _, mw := range p.middlewares {
		for _, dep := range mw.Dependencies() {
			if !provided[dep] {
				return fmt.Errorf("%w: middleware %s depends on capability %s which is not provided by any earlier middleware in the chain", ErrUnsatisfiedDependency, mw.Name(), dep)
			}
		}
		for _, prov := range mw.Provides() {
			if provided[prov] {
				return fmt.Errorf("%w: capability %s is provided by multiple middlewares", ErrDuplicateCapabilityProvider, prov)
			}
			provided[prov] = true
		}
	}
	return nil
}

// Wrap chains all middlewares around the core execution handler
func (p *Pipeline) Wrap(coreHandler execution.Handler) execution.Handler {
	handler := coreHandler
	// Chain from right to left so execution runs in order of slice index
	for i := len(p.middlewares) - 1; i >= 0; i-- {
		mw := p.middlewares[i]
		next := handler
		handler = func(ctx context.Context, req *execution.ExecutionRequest) (*execution.ExecutionResult, error) {
			return mw.Handle(ctx, req, next)
		}
	}
	return handler
}
