package execution

import (
	"sync"

	rtcontext "chukrun/core/context"
)

type PriorityScheduler struct {
	mu             sync.Mutex
	queues         map[rtcontext.PriorityClass][]*Execution
	weights        map[rtcontext.PriorityClass]int
	currentCredits map[rtcontext.PriorityClass]int
}

func NewPriorityScheduler() *PriorityScheduler {
	weights := map[rtcontext.PriorityClass]int{
		rtcontext.PriorityClassCritical:   16,
		rtcontext.PriorityClassHigh:       8,
		rtcontext.PriorityClassNormal:     4,
		rtcontext.PriorityClassLow:        2,
		rtcontext.PriorityClassBackground: 1,
	}

	currentCredits := make(map[rtcontext.PriorityClass]int)
	for k, v := range weights {
		currentCredits[k] = v
	}

	return &PriorityScheduler{
		queues:         make(map[rtcontext.PriorityClass][]*Execution),
		weights:        weights,
		currentCredits: currentCredits,
	}
}

func (ps *PriorityScheduler) Enqueue(exec *Execution) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	p := exec.Priority
	if p == "" {
		p = rtcontext.PriorityClassNormal
	}
	ps.queues[p] = append(ps.queues[p], exec)
}

func (ps *PriorityScheduler) findAndRemove(p rtcontext.PriorityClass, canSchedule func(*Execution) bool) *Execution {
	q := ps.queues[p]
	for i, exec := range q {
		if canSchedule == nil || canSchedule(exec) {
			ps.queues[p] = append(q[:i], q[i+1:]...)
			ps.currentCredits[p]--
			return exec
		}
	}
	return nil
}

func (ps *PriorityScheduler) Dequeue(canSchedule func(exec *Execution) bool) *Execution {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	priorityOrder := []rtcontext.PriorityClass{
		rtcontext.PriorityClassCritical,
		rtcontext.PriorityClassHigh,
		rtcontext.PriorityClassNormal,
		rtcontext.PriorityClassLow,
		rtcontext.PriorityClassBackground,
	}

	for _, p := range priorityOrder {
		if len(ps.queues[p]) > 0 && ps.currentCredits[p] > 0 {
			if exec := ps.findAndRemove(p, canSchedule); exec != nil {
				return exec
			}
		}
	}

	hasCandidates := false
	for _, q := range ps.queues {
		if len(q) > 0 {
			hasCandidates = true
			break
		}
	}

	if !hasCandidates {
		return nil
	}

	for k, v := range ps.weights {
		ps.currentCredits[k] = v
	}

	for _, p := range priorityOrder {
		if exec := ps.findAndRemove(p, canSchedule); exec != nil {
			return exec
		}
	}

	return nil
}

func (ps *PriorityScheduler) Remove(execID string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for p, q := range ps.queues {
		for i, exec := range q {
			if exec.ID == execID {
				ps.queues[p] = append(q[:i], q[i+1:]...)
				return true
			}
		}
	}
	return false
}

func (ps *PriorityScheduler) Size() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	size := 0
	for _, q := range ps.queues {
		size += len(q)
	}
	return size
}
