package kernel

import (
	"context"
	"sync"
	"time"
)

// Event represents a system notification payload
type Event struct {
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// EventHandler represents the subscriber callback function
type EventHandler func(event Event)

// Unsubscribe is a handle to deregister a subscription
type Unsubscribe func()

// EventBus specifies pub/sub message flow interface
type EventBus interface {
	Publish(ctx context.Context, event Event) error
	Subscribe(eventType string, handler EventHandler) (Unsubscribe, error)
}

// InProcessEventBus is a thread-safe implementation of EventBus
type InProcessEventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]subEntry
	nextSubID   int64
}

type subEntry struct {
	id      int64
	handler EventHandler
}

func NewInProcessEventBus() *InProcessEventBus {
	return &InProcessEventBus{
		subscribers: make(map[string][]subEntry),
	}
}

func (eb *InProcessEventBus) Publish(ctx context.Context, event Event) error {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	// Direct event type subscribers
	if subs, ok := eb.subscribers[event.Type]; ok {
		for _, sub := range subs {
			go sub.handler(event)
		}
	}

	// Wildcard subscribers
	if subs, ok := eb.subscribers["*"]; ok {
		for _, sub := range subs {
			go sub.handler(event)
		}
	}

	return nil
}

func (eb *InProcessEventBus) Subscribe(eventType string, handler EventHandler) (Unsubscribe, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.nextSubID++
	id := eb.nextSubID
	entry := subEntry{id: id, handler: handler}

	eb.subscribers[eventType] = append(eb.subscribers[eventType], entry)

	unsubscribe := func() {
		eb.mu.Lock()
		defer eb.mu.Unlock()

		subs := eb.subscribers[eventType]
		for i, sub := range subs {
			if sub.id == id {
				eb.subscribers[eventType] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}

	return unsubscribe, nil
}
