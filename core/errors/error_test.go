package errors

import (
	stdErrors "errors"
	"testing"
)

func TestPlatformError(t *testing.T) {
	cause := stdErrors.New("underlying network failure")
	platErr := NewError(ErrCategoryProvider, "provider endpoint unreachable", true, cause)

	expectedMsg := "[provider] provider endpoint unreachable: underlying network failure"
	if platErr.Error() != expectedMsg {
		t.Errorf("expected error string %q, got: %q", expectedMsg, platErr.Error())
	}

	if unwrapped := platErr.Unwrap(); unwrapped != cause {
		t.Errorf("expected unwrapped error to be cause, got: %v", unwrapped)
	}

	// Without cause
	platErrNoCause := NewError(ErrCategoryConfig, "config schema mismatch", false, nil)
	expectedMsgNoCause := "[config] config schema mismatch"
	if platErrNoCause.Error() != expectedMsgNoCause {
		t.Errorf("expected error string %q, got: %q", expectedMsgNoCause, platErrNoCause.Error())
	}
}

func TestToExecutionError(t *testing.T) {
	// Nil case
	if ToExecutionError(nil) != nil {
		t.Error("expected nil execution error for nil input error")
	}

	// PlatformError case
	platErr := NewError(ErrCategoryAuth, "invalid token key credentials", false, nil)
	execErr := ToExecutionError(platErr)
	if execErr == nil {
		t.Fatal("expected non-nil execution error")
	}
	if execErr.Category != "auth" {
		t.Errorf("expected category auth, got: %s", execErr.Category)
	}
	if execErr.Message != "invalid token key credentials" {
		t.Errorf("expected message mismatch, got: %s", execErr.Message)
	}
	if execErr.Retryable {
		t.Error("expected retryable to be false")
	}

	// Standard error case
	stdErr := stdErrors.New("database connection down")
	execErrStd := ToExecutionError(stdErr)
	if execErrStd == nil {
		t.Fatal("expected non-nil execution error")
	}
	if execErrStd.Category != string(ErrCategoryInternal) {
		t.Errorf("expected category internal, got: %s", execErrStd.Category)
	}
	if execErrStd.Message != "database connection down" {
		t.Errorf("expected message mismatch, got: %s", execErrStd.Message)
	}
	if execErrStd.Retryable {
		t.Error("expected retryable to be false")
	}
}
