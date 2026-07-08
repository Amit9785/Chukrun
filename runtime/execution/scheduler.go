package execution

import (
	"sync"
	"chukrun/runtime/kernel"
)

type PriorityScheduler struct {
	mu             sync.Mutex
	queues         map[kernel.PriorityClass][]*kernel.Execution
	weights        map[kernel.PriorityClass]int
	currentCredits map[kernel.PriorityClass]int
}

func NewPriorityScheduler() *PriorityScheduler {
	weights := map[kernel.PriorityClass]int{
		kernel.PriorityClassCritical:   16,
		kernel.PriorityClassHigh:       8,
		kernel.PriorityClassNormal:     4,
		kernel.PriorityClassLow:        2,
		kernel.PriorityClassBackground: 1,
	}

	currentCredits := make(map[kernel.PriorityClass]int)
	for k, v := range weights {
		currentCredits[k] = v
	}

	return &PriorityScheduler{
		queues:         make(map[kernel.PriorityClass][]*kernel.Execution),
		weights:        weights,
		currentCredits: currentCredits,
	}
}

func (ps *PriorityScheduler) Enqueue(exec *kernel.Execution) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	p := exec.Priority
	if p == "" {
		p = kernel.PriorityClassNormal
	}
	ps.queues[p] = append(ps.queues[p], exec)
}

func (ps *PriorityScheduler) findAndRemove(p kernel.PriorityClass, canSchedule func(*kernel.Execution) bool) *kernel.Execution {
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

func (ps *PriorityScheduler) Dequeue(canSchedule func(exec *kernel.Execution) bool) *kernel.Execution {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	priorityOrder := []kernel.PriorityClass{
		kernel.PriorityClassCritical,
		kernel.PriorityClassHigh,
		kernel.PriorityClassNormal,
		kernel.PriorityClassLow,
		kernel.PriorityClassBackground,
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
