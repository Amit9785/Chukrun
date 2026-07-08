package kernel

import (
	"errors"
	"testing"
	"time"
)

func TestLifecycleTransitions(t *testing.T) {
	lm := NewLifecycleManager()
	if lm.GetState() != StateUninitialized {
		t.Errorf("expected initial state to be UNINITIALIZED, got: %s", lm.GetState())
	}
	if lm.IsReady() {
		t.Error("expected IsReady() to be false initially")
	}

	// Uninitialized -> Booting
	err := lm.Transition(StateBooting)
	if err != nil {
		t.Errorf("expected transition to BOOTING to succeed, got: %v", err)
	}
	if lm.GetState() != StateBooting {
		t.Errorf("expected state to be BOOTING, got: %s", lm.GetState())
	}

	// Booting -> Ready
	err = lm.Transition(StateReady)
	if err != nil {
		t.Errorf("expected transition to READY to succeed, got: %v", err)
	}
	if !lm.IsReady() {
		t.Error("expected IsReady() to be true in READY state")
	}

	// Ready -> Running
	err = lm.Transition(StateRunning)
	if err != nil {
		t.Errorf("expected transition to RUNNING to succeed, got: %v", err)
	}
	if !lm.IsReady() {
		t.Error("expected IsReady() to be true in RUNNING state")
	}

	// Running -> Degraded
	err = lm.Transition(StateDegraded)
	if err != nil {
		t.Errorf("expected transition to DEGRADED to succeed, got: %v", err)
	}
	if !lm.IsReady() {
		t.Error("expected IsReady() to be true in DEGRADED state by default")
	}

	// Degraded -> Running
	err = lm.Transition(StateRunning)
	if err != nil {
		t.Errorf("expected transition to RUNNING to succeed, got: %v", err)
	}

	// Running -> Draining
	err = lm.Transition(StateDraining)
	if err != nil {
		t.Errorf("expected transition to DRAINING to succeed, got: %v", err)
	}

	// Draining -> Stopped
	err = lm.Transition(StateStopped)
	if err != nil {
		t.Errorf("expected transition to STOPPED to succeed, got: %v", err)
	}
}

func TestLifecycleInvalidTransition(t *testing.T) {
	lm := NewLifecycleManager()

	// Direct transition UNINITIALIZED -> READY should fail
	err := lm.Transition(StateReady)
	if err == nil {
		t.Error("expected transition UNINITIALIZED -> READY to fail")
	}

	// Transition UNINITIALIZED -> BOOTING succeeds
	_ = lm.Transition(StateBooting)

	// Transition BOOTING -> STOPPED should fail
	err = lm.Transition(StateStopped)
	if err == nil {
		t.Error("expected transition BOOTING -> STOPPED to fail")
	}
}

func TestComponentHealthAndDegradedState(t *testing.T) {
	lm := NewLifecycleManager()
	_ = lm.Transition(StateBooting)
	_ = lm.Transition(StateReady)
	_ = lm.Transition(StateRunning)

	// Report non-fatal component unhealthy -> DEGRADED
	lm.ReportComponentHealth("telemetry", false, false, errors.New("conn error"))
	if lm.GetState() != StateDegraded {
		t.Errorf("expected state to be DEGRADED, got: %s", lm.GetState())
	}

	h := lm.HealthStatus()
	if h.Overall != HealthDegraded {
		t.Errorf("expected overall health DEGRADED, got: %s", h.Overall)
	}
	if h.Components["telemetry"].State != HealthUnhealthy || h.Components["telemetry"].LastError != "conn error" {
		t.Errorf("unexpected telemetry component health: %+v", h.Components["telemetry"])
	}

	// Report non-fatal component healthy -> RUNNING (recovers)
	lm.ReportComponentHealth("telemetry", true, false, nil)
	if lm.GetState() != StateRunning {
		t.Errorf("expected state to recover to RUNNING, got: %s", lm.GetState())
	}

	// Report fatal component unhealthy -> FAILED
	lm.ReportComponentHealth("event_bus", false, true, errors.New("bus error"))
	if lm.GetState() != StateFailed {
		t.Errorf("expected state to be FAILED, got: %s", lm.GetState())
	}
}

func TestRestartCooldownEnforcement(t *testing.T) {
	lm := NewLifecycleManager()
	lm.Configure(50, true)

	// Cooldown starts blank/zero
	err := lm.CheckRestartCooldown()
	if err != nil {
		t.Errorf("first check should succeed, got: %v", err)
	}

	// Rapid check should fail
	err = lm.CheckRestartCooldown()
	if err == nil {
		t.Error("expected restart check to fail due to active cooldown")
	}

	// Wait for cooldown to expire
	time.Sleep(60 * time.Millisecond)
	err = lm.CheckRestartCooldown()
	if err != nil {
		t.Errorf("check should succeed after sleep, got: %v", err)
	}
}
