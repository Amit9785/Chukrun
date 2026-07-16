package execution

import (
	"context"
)

type Engine interface {
	Submit(ctx context.Context, req *ExecutionRequest) (*Execution, error)
	SubmitBatch(ctx context.Context, reqs []*ExecutionRequest) (*Batch, error)
	Stream(ctx context.Context, req *ExecutionRequest) (<-chan StreamChunk, error)
	Get(ctx context.Context, executionID string) (*Execution, error)
	Cancel(ctx context.Context, executionID string) error
	Wait(ctx context.Context, executionID string) (*ExecutionResult, error)
}
