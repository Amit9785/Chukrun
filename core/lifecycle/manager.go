package lifecycle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"chukrun/core/errors"
)

type State string

const (
	StateUninitialized State = "UNINITIALIZED"
	StateBooting       State = "BOOTING"
	StateReady         State = "READY"
	StateRunning       State = "RUNNING"
	StateDegraded      State = "DEGRADED"
	StateDraining      State = "DRAINING"
	StateStopped       State = "STOPPED"
	StateFailed        State = "FAILED"
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

// LifecycleManager manages thread-safe transitions and verification of runtime states.
type LifecycleManager struct {
	mu            sync.RWMutex
	stateVal      atomic.Value // holds State
	since         time.Time
	reason        string
	components    map[string]ComponentHealth
	lastRestart   time.Time
	cooldownMS    int
	degradedReady bool
}

func NewLifecycleManager() *LifecycleManager {
	lm := &LifecycleManager{
		since:         time.Now(),
		components:    make(map[string]ComponentHealth),
		cooldownMS:    1000,
		degradedReady: true,
	}
	lm.stateVal.Store(StateUninitialized)
	return lm
}

// Configure sets configuration values for lifecycle checks
func (lm *LifecycleManager) Configure(cooldownMS int, acceptWhenDegraded bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.cooldownMS = cooldownMS
	lm.degradedReady = acceptWhenDegraded
}

// GetState returns the current lifecycle state (lock-free)
func (lm *LifecycleManager) GetState() State {
	if v := lm.stateVal.Load(); v != nil {
		return v.(State)
	}
	return StateUninitialized
}

// IsReady returns true if the state is ready to serve traffic
func (lm *LifecycleManager) IsReady() bool {
	state := lm.GetState()
	if state == StateReady || state == StateRunning {
		return true
	}
	if state == StateDegraded {
		lm.mu.RLock()
		defer lm.mu.RUnlock()
		return lm.degradedReady
	}
	return false
}

// Transition transitions the state from current to target, enforcing rules
func (lm *LifecycleManager) Transition(to State) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	from := lm.GetState()
	if from == to {
		return nil
	}

	valid := false
	switch from {
	case StateUninitialized:
		valid = (to == StateBooting)
	case StateBooting:
		valid = (to == StateReady || to == StateFailed)
	case StateReady:
		valid = (to == StateRunning || to == StateDraining || to == StateFailed)
	case StateRunning:
		valid = (to == StateDegraded || to == StateFailed || to == StateDraining)
	case StateDegraded:
		valid = (to == StateRunning || to == StateFailed || to == StateDraining)
	case StateDraining:
		valid = (to == StateStopped)
	case StateFailed:
		valid = (to == StateBooting) // legal during Restart()
	case StateStopped:
		valid = false
	}

	if !valid {
		return errors.NewError(
			errors.ErrCategoryInternal,
			fmt.Sprintf("invalid lifecycle state transition from %s to %s", from, to),
			false,
			nil,
		)
	}

	lm.stateVal.Store(to)
	lm.since = time.Now()
	if to == StateBooting {
		// Clear components on boot or restart
		lm.components = make(map[string]ComponentHealth)
		lm.reason = ""
	}
	return nil
}

// CheckRestartCooldown enforces the cooldown window between restarts
func (lm *LifecycleManager) CheckRestartCooldown() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	if !lm.lastRestart.IsZero() && now.Sub(lm.lastRestart) < time.Duration(lm.cooldownMS)*time.Millisecond {
		return errors.NewError(
			errors.ErrCategoryInternal,
			fmt.Sprintf("restart rejected: cooldown active (minimum %dms between restarts)", lm.cooldownMS),
			false,
			nil,
		)
	}
	lm.lastRestart = now
	return nil
}

func (lm *LifecycleManager) handleComponentFailure(state State, component string, fatal bool, lastErrStr string) {
	if fatal {
		if state == StateRunning || state == StateDegraded || state == StateReady || state == StateBooting {
			lm.stateVal.Store(StateFailed)
			lm.since = time.Now()
			lm.reason = fmt.Sprintf("fatal component %s failed: %s", component, lastErrStr)
		}
	} else {
		if state == StateRunning || state == StateReady {
			lm.stateVal.Store(StateDegraded)
			lm.since = time.Now()
			lm.reason = fmt.Sprintf("non-fatal component %s failed: %s", component, lastErrStr)
		}
	}
}

func (lm *LifecycleManager) handleComponentRecovery(state State) {
	if state != StateDegraded {
		return
	}
	allHealthy := true
	var remainingErrors []string
	for name, ch := range lm.components {
		if ch.State == HealthUnhealthy {
			allHealthy = false
			remainingErrors = append(remainingErrors, fmt.Sprintf("%s: %s", name, ch.LastError))
		}
	}
	if allHealthy {
		lm.stateVal.Store(StateRunning)
		lm.since = time.Now()
		lm.reason = ""
	} else {
		lm.reason = fmt.Sprintf("degraded due to failed components: %v", remainingErrors)
	}
}

// ReportComponentHealth registers a component's health report asynchronously and evaluates state transitions
func (lm *LifecycleManager) ReportComponentHealth(component string, healthy bool, fatal bool, err error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	state := lm.GetState()
	// Do not process component updates if the runtime is in final/draining states
	if state == StateDraining || state == StateStopped {
		return
	}

	var hState HealthState = HealthHealthy
	var lastErrStr string
	if !healthy {
		hState = HealthUnhealthy
		if err != nil {
			lastErrStr = err.Error()
		} else {
			lastErrStr = "unknown error"
		}
	}

	lm.components[component] = ComponentHealth{
		State:     hState,
		Details:   lastErrStr,
		Fatal:     fatal,
		LastError: lastErrStr,
		CheckedAt: time.Now(),
	}

	if !healthy {
		lm.handleComponentFailure(state, component, fatal, lastErrStr)
	} else {
		lm.handleComponentRecovery(state)
	}
}

// HealthStatus returns a thread-safe snapshot of the health status
func (lm *LifecycleManager) HealthStatus() *HealthStatus {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	state := lm.GetState()
	var overall HealthState

	switch state {
	case StateUninitialized, StateBooting, StateStopped, StateFailed:
		overall = HealthUnhealthy
	case StateReady, StateRunning:
		overall = HealthHealthy
	case StateDegraded:
		overall = HealthDegraded
	case StateDraining:
		overall = HealthDraining
	}

	compCopy := make(map[string]ComponentHealth)
	for k, v := range lm.components {
		compCopy[k] = v
	}

	// Always report lifecycle component health explicitly
	compCopy["lifecycle"] = ComponentHealth{
		State:     overall,
		Details:   string(state),
		Fatal:     true,
		CheckedAt: lm.since,
	}

	return &HealthStatus{
		Overall:    overall,
		State:      state,
		Components: compCopy,
		Since:      lm.since,
		Reason:     lm.reason,
	}
}
