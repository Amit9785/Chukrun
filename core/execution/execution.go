package execution

import (
	"context"
	"sync"
	"time"

	rtcontext "chukrun/core/context"
	"chukrun/core/errors"
)

// Priority defines request execution priority
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
)

// ExecutionState represents the current state of an execution
type ExecutionState string

const (
	ExecStatePending   ExecutionState = "PENDING"
	ExecStateQueued    ExecutionState = "QUEUED"
	ExecStateRunning   ExecutionState = "RUNNING"
	ExecStateRetrying  ExecutionState = "RETRYING"
	ExecStateSucceeded ExecutionState = "SUCCEEDED"
	ExecStateFailed    ExecutionState = "FAILED"
	ExecStateCancelled ExecutionState = "CANCELLED"
	ExecStateTimedOut  ExecutionState = "TIMED_OUT"
)

type BackoffStrategy string

const (
	BackoffConstant    BackoffStrategy = "constant"
	BackoffLinear      BackoffStrategy = "linear"
	BackoffExponential BackoffStrategy = "exponential"
)

type RetryPolicy struct {
	MaxAttempts     int                  `json:"max_attempts" yaml:"max_attempts"`
	BackoffStrategy BackoffStrategy      `json:"backoff_strategy" yaml:"backoff_strategy"`
	BaseDelay       time.Duration        `json:"base_delay" yaml:"base_delay"`
	MaxDelay        time.Duration        `json:"max_delay" yaml:"max_delay"`
	Jitter          bool                 `json:"jitter" yaml:"jitter"`
	RetryableCheck  func(err error) bool `json:"-" yaml:"-"`
}

type TimeoutPolicy struct {
	Total      time.Duration `json:"total" yaml:"total"`
	PerAttempt time.Duration `json:"per_attempt" yaml:"per_attempt"`
}

// ExecutionStatus defines the current status of execution
type ExecutionStatus string

const (
	StatusSucceeded ExecutionStatus = "Succeeded"
	StatusFailed    ExecutionStatus = "Failed"
	StatusCancelled ExecutionStatus = "Cancelled"
	StatusTimedOut  ExecutionStatus = "TimedOut"
)

// TokenUsage tracks prompt and completion tokens used
type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// CostEstimate calculates request execution cost in USD
type CostEstimate struct {
	AmountUSD float64 `json:"amount_usd"`
}



// StreamChunk represents a chunk of streamed results
type StreamChunk struct {
	ID         string      `json:"id"`
	Content    string      `json:"content,omitempty"`
	TokenUsage *TokenUsage `json:"token_usage,omitempty"`
	Error      error       `json:"error,omitempty"`
}

// ExecutionRequest represents the input parameters for a request execution
type ExecutionRequest struct {
	ID            string                  `json:"id"`
	ProviderRef   string                  `json:"provider_ref"`
	Payload       any                     `json:"payload"`
	Priority      rtcontext.PriorityClass `json:"priority,omitempty"`
	RetryPolicy   *RetryPolicy            `json:"retry_policy,omitempty"`
	TimeoutPolicy *TimeoutPolicy          `json:"timeout_policy,omitempty"`
	Metadata      map[string]string       `json:"metadata,omitempty"`
	ParentID      *string                 `json:"parent_id,omitempty"`

	// Deprecated: use TimeoutPolicy or PriorityClass
	Timeout        time.Duration `json:"timeout,omitempty"`
	PriorityLegacy Priority      `json:"priority_legacy,omitempty"`
}

type Attempt struct {
	Number      int                    `json:"number"`
	StartedAt   time.Time              `json:"started_at"`
	EndedAt     *time.Time             `json:"ended_at,omitempty"`
	Error       *errors.ExecutionError `json:"error,omitempty"`
	ProviderRef string                 `json:"provider_ref"`
}

type Execution struct {
	ID          string                  `json:"id"`
	Request     *ExecutionRequest       `json:"request"`
	State       ExecutionState          `json:"state"`
	Priority    rtcontext.PriorityClass `json:"priority"`
	CreatedAt   time.Time               `json:"created_at"`
	StartedAt   *time.Time              `json:"started_at,omitempty"`
	CompletedAt *time.Time              `json:"completed_at,omitempty"`
	Attempts    []Attempt               `json:"attempts,omitempty"`
	Result      *ExecutionResult        `json:"result,omitempty"`
	ParentID    *string                 `json:"parent_id,omitempty"`
	CancelFunc  context.CancelFunc      `json:"-"`
	mu          sync.RWMutex            `json:"-"`
}

func (e *Execution) Lock() {
	e.mu.Lock()
}

func (e *Execution) Unlock() {
	e.mu.Unlock()
}

func (e *Execution) RLock() {
	e.mu.RLock()
}

func (e *Execution) RUnlock() {
	e.mu.RUnlock()
}

type Batch struct {
	ID         string       `json:"id"`
	Executions []*Execution `json:"executions"`
}

// ExecutionResult represents the outcome of a request execution
type ExecutionResult struct {
	ID           string                 `json:"id"`
	Status       ExecutionStatus        `json:"status"` // Succeeded, Failed, Cancelled, TimedOut
	State        ExecutionState         `json:"state"`  // PENDING, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT
	Output       any                    `json:"output,omitempty"`
	Error        *errors.ExecutionError `json:"error,omitempty"`
	TokenUsage   *TokenUsage            `json:"token_usage,omitempty"`
	Cost         *CostEstimate          `json:"cost,omitempty"`
	Duration     time.Duration          `json:"duration"`
	AttemptCount int                    `json:"attempt_count"`
	RetryCount   int                    `json:"retry_count"` // Deprecated: use AttemptCount
	Attempts     []Attempt              `json:"attempts,omitempty"`
}

// Handler represents the execution step function in the pipeline
type Handler func(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error)

// PipelineWrapper defines an interface for chaining middleware around the execution handler
type PipelineWrapper interface {
	Wrap(coreHandler Handler) Handler
}
