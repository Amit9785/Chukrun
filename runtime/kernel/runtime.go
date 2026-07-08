package kernel

import (
	"context"
	"sync"
	"time"
)

// Priority defines request execution priority
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
)

// PriorityClass defines execution priorities
type PriorityClass string

const (
	PriorityClassCritical   PriorityClass = "Critical"
	PriorityClassHigh       PriorityClass = "High"
	PriorityClassNormal     PriorityClass = "Normal"
	PriorityClassLow        PriorityClass = "Low"
	PriorityClassBackground PriorityClass = "Background"
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

// HealthState represents the overall state of the runtime health
type HealthState string

const (
	HealthHealthy   HealthState = "Healthy"
	HealthDegraded  HealthState = "Degraded"
	HealthUnhealthy HealthState = "Unhealthy"
	HealthDraining  HealthState = "Draining"
)

// ComponentHealth represents health status of individual components
type ComponentHealth struct {
	State     HealthState `json:"state"`
	Details   string      `json:"details,omitempty"`
	Fatal     bool        `json:"fatal,omitempty"`
	LastError string      `json:"last_error,omitempty"`
	CheckedAt time.Time   `json:"checked_at,omitempty"`
}

// HealthStatus reports readiness of Runtime and its components
type HealthStatus struct {
	Overall    HealthState                `json:"overall"`
	State      State                      `json:"state"`
	Components map[string]ComponentHealth `json:"components"`
	Since      time.Time                  `json:"since"`
	Reason     string                     `json:"reason,omitempty"`
}

func (h *HealthStatus) IsLive() bool {
	return h.State != StateUninitialized && h.State != StateStopped && h.State != StateFailed
}

func (h *HealthStatus) IsReady() bool {
	return h.State == StateReady || h.State == StateRunning
}

func (h *HealthStatus) IsReadyOrDegraded() bool {
	return h.State == StateReady || h.State == StateRunning || h.State == StateDegraded
}

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

// ExecutionError represents structured execution errors
type ExecutionError struct {
	Category  string `json:"category"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
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
	ID            string            `json:"id"`
	ProviderRef   string            `json:"provider_ref"`
	Payload       any               `json:"payload"`
	Priority      PriorityClass     `json:"priority,omitempty"`
	RetryPolicy   *RetryPolicy      `json:"retry_policy,omitempty"`
	TimeoutPolicy *TimeoutPolicy    `json:"timeout_policy,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	ParentID      *string           `json:"parent_id,omitempty"`

	// Deprecated: use TimeoutPolicy or PriorityClass
	Timeout        time.Duration `json:"timeout,omitempty"`
	PriorityLegacy Priority      `json:"priority_legacy,omitempty"`
}

type Attempt struct {
	Number      int             `json:"number"`
	StartedAt   time.Time       `json:"started_at"`
	EndedAt     *time.Time      `json:"ended_at,omitempty"`
	Error       *ExecutionError `json:"error,omitempty"`
	ProviderRef string          `json:"provider_ref"`
}

type Execution struct {
	ID          string            `json:"id"`
	Request     *ExecutionRequest `json:"request"`
	State       ExecutionState    `json:"state"`
	Priority    PriorityClass     `json:"priority"`
	CreatedAt   time.Time         `json:"created_at"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Attempts    []Attempt         `json:"attempts,omitempty"`
	Result      *ExecutionResult  `json:"result,omitempty"`
	ParentID    *string           `json:"parent_id,omitempty"`
	CancelFunc  context.CancelFunc `json:"-"`
	mu          sync.RWMutex       `json:"-"`
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
	ID           string          `json:"id"`
	Status       ExecutionStatus `json:"status"` // Succeeded, Failed, Cancelled, TimedOut
	State        ExecutionState  `json:"state"`  // SUCCEEDED, FAILED, CANCELLED, TIMED_OUT
	Output       any             `json:"output,omitempty"`
	Error        *ExecutionError `json:"error,omitempty"`
	TokenUsage   *TokenUsage     `json:"token_usage,omitempty"`
	Cost         *CostEstimate   `json:"cost,omitempty"`
	Duration     time.Duration   `json:"duration"`
	AttemptCount int             `json:"attempt_count"`
	RetryCount   int             `json:"retry_count"` // Deprecated: use AttemptCount
	Attempts     []Attempt       `json:"attempts,omitempty"`
}

// Runtime is the stable, versioned contract representing the engine coordinator.
type Runtime interface {
	// Initialize bootstraps all internal components. Must be called
	// exactly once before Execute or Health are used.
	Initialize(ctx context.Context) error

	// Execute runs a single unit of work and returns a normalized result.
	Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error)

	// Stream runs a single unit of work and streams partial results.
	Stream(ctx context.Context, req *ExecutionRequest) (<-chan StreamChunk, error)

	// Shutdown gracefully drains in-flight executions and releases resources.
	Shutdown(ctx context.Context) error

	// Health reports the readiness of the Runtime and its components.
	Health(ctx context.Context) (*HealthStatus, error)

	// Restart re-runs the full bootstrap sequence on the existing Runtime.
	Restart(ctx context.Context) error
}
