package execution

import (
	"context"
)

// ProviderCapabilities describes features supported by a provider model
type ProviderCapabilities struct {
	Streaming       bool `json:"streaming"`
	FunctionCalling bool `json:"function_calling"`
	MaxContextToken int  `json:"max_context_token"`
}

// Provider defines standard model execution adapter interface
type Provider interface {
	Execute(ctx context.Context, request *ExecutionRequest) (*ExecutionResult, error)
	Stream(ctx context.Context, request *ExecutionRequest) (<-chan StreamChunk, error)
	Name() string
	Capabilities() ProviderCapabilities
}
