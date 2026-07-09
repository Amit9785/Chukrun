package execution

import (
	"testing"
	"time"
	"chukrun/runtime/kernel"
)

func TestCalculateBackoff(t *testing.T) {
	policy := &kernel.RetryPolicy{
		BackoffStrategy: kernel.BackoffConstant,
		BaseDelay:       100 * time.Millisecond,
		MaxDelay:        500 * time.Millisecond,
		Jitter:          false,
	}

	d := CalculateBackoff(policy, 1)
	if d != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", d)
	}

	policy.BackoffStrategy = kernel.BackoffLinear
	d = CalculateBackoff(policy, 3)
	if d != 300*time.Millisecond {
		t.Errorf("expected 300ms, got %v", d)
	}

	policy.BackoffStrategy = kernel.BackoffExponential
	d = CalculateBackoff(policy, 4)
	if d != 500*time.Millisecond {
		t.Errorf("expected 500ms (max clamped), got %v", d)
	}

	d = CalculateBackoff(nil, 1)
	if d != 0 {
		t.Errorf("expected 0, got %v", d)
	}

	policy.BackoffStrategy = kernel.BackoffConstant
	d = CalculateBackoff(policy, -1)
	if d != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", d)
	}

	policy.Jitter = true
	CalculateBackoff(policy, 1)
}

func TestSafeRand(t *testing.T) {
	sr := NewSafeRand()
	v := sr.Intn(10)
	if v < 0 || v >= 10 {
		t.Errorf("unexpected random value: %d", v)
	}
}
