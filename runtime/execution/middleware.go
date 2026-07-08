package execution

import (
	"context"
	"chukrun/runtime/kernel"
)

// Handler represents the execution step function in the pipeline
type Handler func(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.ExecutionResult, error)

// Middleware wraps request executions with pre/post hooks
type Middleware interface {
	Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error)
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

// Wrap chains all middlewares around the core execution handler
func (p *Pipeline) Wrap(coreHandler Handler) Handler {
	handler := coreHandler
	// Chain from right to left so execution runs in order of slice index
	for i := len(p.middlewares) - 1; i >= 0; i-- {
		mw := p.middlewares[i]
		next := handler
		handler = func(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
			return mw.Handle(ctx, req, next)
		}
	}
	return handler
}
