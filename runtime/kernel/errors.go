package kernel

import "fmt"

// ErrorCategory represents the classification of runtime errors
type ErrorCategory string

const (
	ErrCategoryConfig     ErrorCategory = "config"     // bad/missing configuration
	ErrCategoryProvider   ErrorCategory = "provider"   // provider-side failure
	ErrCategoryTimeout    ErrorCategory = "timeout"    // execution timed out
	ErrCategoryCancelled  ErrorCategory = "cancelled"  // context cancelled
	ErrCategoryValidation ErrorCategory = "validation" // invalid request or params
	ErrCategoryInternal   ErrorCategory = "internal"   // internal bug/panic recovery
	ErrCategorySaturation ErrorCategory = "saturation" // concurrency/rate limit saturated
	ErrCategoryAuth       ErrorCategory = "auth"       // authentication/authorization failure
)

// PlatformError is a custom error type carrying metadata
type PlatformError struct {
	Category  ErrorCategory
	Message   string
	Retryable bool
	Cause     error
}

func (e *PlatformError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Category, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Category, e.Message)
}

func (e *PlatformError) Unwrap() error {
	return e.Cause
}

// NewError creates a new structured PlatformError
func NewError(category ErrorCategory, message string, retryable bool, cause error) *PlatformError {
	return &PlatformError{
		Category:  category,
		Message:   message,
		Retryable: retryable,
		Cause:     cause,
	}
}

// ToExecutionError converts a Go error into a kernel.ExecutionError representation
func ToExecutionError(err error) *ExecutionError {
	if err == nil {
		return nil
	}

	if platErr, ok := err.(*PlatformError); ok {
		return &ExecutionError{
			Category:  string(platErr.Category),
			Message:   platErr.Message,
			Retryable: platErr.Retryable,
		}
	}

	// Fallback for default Go errors
	return &ExecutionError{
		Category:  string(ErrCategoryInternal),
		Message:   err.Error(),
		Retryable: false,
	}
}
