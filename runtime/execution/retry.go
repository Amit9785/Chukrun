package execution

import (
	"math/rand"
	"time"
	"sync"
	"chukrun/runtime/kernel"
)

type SafeRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func NewSafeRand() *SafeRand {
	return &SafeRand{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sr *SafeRand) Intn(n int) int {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.r.Intn(n)
}

var globalRand = NewSafeRand()

// CalculateBackoff returns the backoff delay for the given attempt.
func CalculateBackoff(policy *kernel.RetryPolicy, attempt int) time.Duration {
	if policy == nil {
		return 0
	}

	if attempt <= 0 {
		attempt = 1
	}

	var delay time.Duration

	switch policy.BackoffStrategy {
	case kernel.BackoffConstant:
		delay = policy.BaseDelay
	case kernel.BackoffLinear:
		delay = policy.BaseDelay * time.Duration(attempt)
	case kernel.BackoffExponential:
		multiplier := 1
		for i := 1; i < attempt; i++ {
			multiplier *= 2
			// Bound multiplier overflow
			if multiplier > 1000000 {
				multiplier = 1000000
				break
			}
		}
		delay = policy.BaseDelay * time.Duration(multiplier)
	default:
		delay = policy.BaseDelay
	}

	if policy.MaxDelay > 0 && delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}

	if policy.Jitter && delay > 0 {
		randVal := globalRand.Intn(int(delay))
		delay = time.Duration(randVal)
	}

	return delay
}
