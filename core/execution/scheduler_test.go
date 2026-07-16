package execution

import (
	"testing"

	"chukrun/core/context"
)

func TestPriorityScheduler(t *testing.T) {
	ps := NewPriorityScheduler()

	exec1 := &Execution{ID: "exec-1", Priority: context.PriorityClassHigh}
	exec2 := &Execution{ID: "exec-2", Priority: context.PriorityClassNormal}
	exec3 := &Execution{ID: "exec-3", Priority: context.PriorityClassCritical}

	ps.Enqueue(exec1)
	ps.Enqueue(exec2)
	ps.Enqueue(exec3)

	if ps.Size() != 3 {
		t.Errorf("expected size 3, got %d", ps.Size())
	}

	deq1 := ps.Dequeue(nil)
	if deq1 == nil || deq1.ID != "exec-3" {
		t.Errorf("expected exec-3, got %v", deq1)
	}

	if !ps.Remove("exec-1") {
		t.Error("expected Remove to return true")
	}

	if ps.Size() != 1 {
		t.Errorf("expected size 1, got %d", ps.Size())
	}

	deq2 := ps.Dequeue(nil)
	if deq2 == nil || deq2.ID != "exec-2" {
		t.Errorf("expected exec-2, got %v", deq2)
	}

	deq3 := ps.Dequeue(nil)
	if deq3 != nil {
		t.Errorf("expected nil dequeue, got %v", deq3)
	}
}
