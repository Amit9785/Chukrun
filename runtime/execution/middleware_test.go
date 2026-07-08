package execution

import (
	"context"
	"testing"

	"chukrun/runtime/kernel"
)

type mockMiddleware struct {
	name   string
	traces *[]string
}

func (m *mockMiddleware) Handle(ctx context.Context, req *kernel.ExecutionRequest, next Handler) (*kernel.ExecutionResult, error) {
	*m.traces = append(*m.traces, "pre:"+m.name)
	res, err := next(ctx, req)
	*m.traces = append(*m.traces, "post:"+m.name)
	return res, err
}

func TestMiddlewarePipelineExecution(t *testing.T) {
	traces := make([]string, 0)

	mw1 := &mockMiddleware{name: "mw1", traces: &traces}
	mw2 := &mockMiddleware{name: "mw2", traces: &traces}

	pipeline := NewPipeline([]Middleware{mw1, mw2})

	coreHandler := func(ctx context.Context, req *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
		traces = append(traces, "core")
		return &kernel.ExecutionResult{ID: req.ID, Status: kernel.StatusSucceeded}, nil
	}

	chained := pipeline.Wrap(coreHandler)

	req := &kernel.ExecutionRequest{ID: "req-1"}
	res, err := chained(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	if res.Status != kernel.StatusSucceeded {
		t.Errorf("expected status succeeded, got: %s", res.Status)
	}

	// Verify exact execution order of traces
	expectedOrder := []string{"pre:mw1", "pre:mw2", "core", "post:mw2", "post:mw1"}
	if len(traces) != len(expectedOrder) {
		t.Fatalf("expected trace length %d, got %d. Traces: %v", len(expectedOrder), len(traces), traces)
	}

	for i, tr := range expectedOrder {
		if traces[i] != tr {
			t.Errorf("at index %d: expected %s, got %s", i, tr, traces[i])
		}
	}
}
