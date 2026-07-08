package provider

import (
	"context"
	"testing"

	"chukrun/runtime/kernel"
)

type testMockProvider struct {
	name string
}

func (m *testMockProvider) Execute(ctx context.Context, request *kernel.ExecutionRequest) (*kernel.ExecutionResult, error) {
	return nil, nil
}

func (m *testMockProvider) Stream(ctx context.Context, request *kernel.ExecutionRequest) (<-chan kernel.StreamChunk, error) {
	return nil, nil
}

func (m *testMockProvider) Name() string {
	return m.name
}

func (m *testMockProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{}
}

func TestProviderRegistry(t *testing.T) {
	reg := NewRegistry()

	// Resolve when registry is empty -> should fail
	_, err := reg.Resolve("openai")
	if err == nil {
		t.Fatal("expected resolve to fail when registry is empty")
	}

	p1 := &testMockProvider{name: "openai"}
	p2 := &testMockProvider{name: "anthropic"}

	// Success register
	if err := reg.Register(p1); err != nil {
		t.Fatalf("failed to register provider: %v", err)
	}

	// Duplicate register should fail
	if err := reg.Register(&testMockProvider{name: "openai"}); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}

	// Nil register should fail
	if err := reg.Register(nil); err == nil {
		t.Fatal("expected nil registration to fail")
	}

	// Empty name registration should fail
	if err := reg.Register(&testMockProvider{name: ""}); err == nil {
		t.Fatal("expected empty name registration to fail")
	}

	_ = reg.Register(p2)

	// List
	list := reg.List()
	if len(list) != 2 {
		t.Errorf("expected list length 2, got: %d", len(list))
	}

	// Resolve specific
	resolved, err := reg.Resolve("anthropic")
	if err != nil {
		t.Fatalf("failed to resolve: %v", err)
	}
	if resolved.Name() != "anthropic" {
		t.Errorf("expected anthropic, got: %s", resolved.Name())
	}

	// Resolve non-existent should fail
	_, err = reg.Resolve("non-existent")
	if err == nil {
		t.Fatal("expected resolving non-existent provider to fail")
	}

	// Resolve default auto-routing (when name is empty)
	resolvedDefault, err := reg.Resolve("")
	if err != nil {
		t.Fatalf("failed to resolve default: %v", err)
	}
	if resolvedDefault == nil {
		t.Fatal("expected non-nil default resolved provider")
	}
}
