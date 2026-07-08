package execution

import (
	"context"
	"chukrun/runtime/kernel"
)

type Engine interface {
	Submit(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.Execution, error)
	SubmitBatch(ctx context.Context, reqs []*kernel.ExecutionRequest) (*kernel.Batch, error)
	Stream(ctx context.Context, req *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error)
	Get(ctx context.Context, executionID string) (*kernel.Execution, error)
	Cancel(ctx context.Context, executionID string) error
	Wait(ctx context.Context, executionID string) (*kernel.ExecutionResult, error)
}
