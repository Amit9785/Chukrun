package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const TopicDeadLetter = "eventbus.dead_letter"

// Event represents a system notification payload conforming to PRD 1.5
type Event struct {
	Topic     string         `json:"topic"`
	Type      string         `json:"type"` // Backwards compatibility field
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   any            `json:"payload"`
	TraceID   string         `json:"trace_id"`
	SourceID  string         `json:"source_id"`
}

func (ev *Event) normalize() {
	if ev.Topic == "" {
		ev.Topic = ev.Type
	} else if ev.Type == "" {
		ev.Type = ev.Topic
	}
	if ev.ID == "" {
		ev.ID = fmt.Sprintf("evt-%016x", time.Now().UnixNano())
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
}

// EventHandler represents the subscriber callback function conforming to PRD 1.5
type EventHandler func(ctx context.Context, event Event) error

// Unsubscribe is a handle to deregister a subscription
type Unsubscribe func()

// EventBus specifies pub/sub message flow interface conforming to PRD 1.5
type EventBus interface {
	Publish(ctx context.Context, event Event) error
	Subscribe(pattern string, handler EventHandler) (Unsubscribe, error)
	SubscribeFiltered(pattern string, predicate func(Event) bool, handler EventHandler) (Unsubscribe, error)
	RegisterNamespace(prefix string, owner string) error
}

type OverflowPolicy int

const (
	DropOldest OverflowPolicy = iota
	DropNewest
)

type DeadLetterEntry struct {
	Event        Event     `json:"event"`
	SubscriberID string    `json:"subscriber_id"`
	Reason       string    `json:"reason"`
	Error        string    `json:"error"`
	OccurredAt   time.Time `json:"occurred_at"`
}

var (
	ErrNamespaceTaken = fmt.Errorf("namespace already registered")
	ErrUnknownTopic   = fmt.Errorf("unknown topic or unregistered namespace")
)

var canonicalTopics = map[string]bool{
	"runtime.starting":           true,
	"runtime.ready":              true,
	"runtime.state_changed":      true,
	"runtime.degraded":           true,
	"runtime.recovered":          true,
	"runtime.failed":             true,
	"runtime.restart_requested":  true,
	"runtime.restart_rejected":   true,
	"runtime.shutdown_started":   true,
	"runtime.shutdown_completed": true,

	"execution.queued":    true,
	"execution.started":   true,
	"execution.retrying":  true,
	"execution.succeeded": true,
	"execution.failed":    true,
	"execution.cancelled": true,
	"execution.timed_out": true,

	"health.degraded":  true,
	"health.recovered": true,

	TopicDeadLetter: true,
}

// InProcessEventBus is a thread-safe implementation of EventBus conforming to PRD 1.5
type InProcessEventBus struct {
	mu               sync.RWMutex
	subscribers      map[string][]*subscription
	namespaces       map[string]string // prefix -> owner
	nextSubID        int64
	queueCapacity    int
	overflowPolicy   OverflowPolicy
	strictValidation bool
	retryAttempts    int
}

type subscription struct {
	id        int64
	pattern   string
	predicate func(Event) bool
	handler   EventHandler
	queue     chan Event
	policy    OverflowPolicy
	eb        *InProcessEventBus
	unsubOnce sync.Once
}

func NewInProcessEventBus() *InProcessEventBus {
	eb := &InProcessEventBus{
		subscribers:      make(map[string][]*subscription),
		namespaces:       make(map[string]string),
		queueCapacity:    1000,
		overflowPolicy:   DropOldest,
		strictValidation: true,
		retryAttempts:    2,
	}

	// Register reserved core namespaces
	_ = eb.RegisterNamespace("runtime", "core")
	_ = eb.RegisterNamespace("execution", "engine")
	_ = eb.RegisterNamespace("health", "core")
	_ = eb.RegisterNamespace("eventbus", "core")

	return eb
}

func (eb *InProcessEventBus) SetQueueCapacity(cap int) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.queueCapacity = cap
}

func (eb *InProcessEventBus) SetOverflowPolicy(policy OverflowPolicy) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.overflowPolicy = policy
}

func (eb *InProcessEventBus) SetStrictMode(strict bool) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.strictValidation = strict
}

func (eb *InProcessEventBus) RegisterNamespace(prefix string, owner string) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	prefix = strings.TrimSuffix(prefix, ".")
	if _, exists := eb.namespaces[prefix]; exists {
		return ErrNamespaceTaken
	}
	eb.namespaces[prefix] = owner
	return nil
}

func getNamespace(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func matchTopic(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == topic {
		return true
	}
	pSegs := strings.Split(pattern, ".")
	tSegs := strings.Split(topic, ".")

	for i := 0; i < len(pSegs); i++ {
		p := pSegs[i]
		if p == "**" {
			return true
		}
		if i >= len(tSegs) {
			return false
		}
		if p == "*" {
			continue
		}
		if p != tSegs[i] {
			return false
		}
	}
	return len(pSegs) == len(tSegs)
}

func (eb *InProcessEventBus) validateTopic(topic string) error {
	ns := getNamespace(topic)
	eb.mu.RLock()
	_, nsRegistered := eb.namespaces[ns]
	eb.mu.RUnlock()

	if !nsRegistered {
		return ErrUnknownTopic
	}

	if eb.strictValidation {
		if ns == "runtime" || ns == "execution" || ns == "health" || ns == "eventbus" {
			if !canonicalTopics[topic] {
				return ErrUnknownTopic
			}
		}
	}
	return nil
}

func (eb *InProcessEventBus) getMatchingSubscribers(event Event) []*subscription {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	var matchingSubs []*subscription
	for pattern, subs := range eb.subscribers {
		if !matchTopic(pattern, event.Topic) {
			continue
		}
		for _, sub := range subs {
			if sub.predicate == nil || sub.predicate(event) {
				matchingSubs = append(matchingSubs, sub)
			}
		}
	}
	return matchingSubs
}

func (eb *InProcessEventBus) Publish(ctx context.Context, event Event) error {
	event.normalize()

	if err := eb.validateTopic(event.Topic); err != nil {
		return err
	}

	if event.TraceID == "" {
		if tid, ok := ctx.Value("trace_id").(string); ok {
			event.TraceID = tid
		}
	}

	matchingSubs := eb.getMatchingSubscribers(event)
	for _, sub := range matchingSubs {
		sub.enqueue(event)
	}

	return nil
}

func (eb *InProcessEventBus) Subscribe(pattern string, handler EventHandler) (Unsubscribe, error) {
	return eb.SubscribeFiltered(pattern, nil, handler)
}

func (eb *InProcessEventBus) SubscribeFiltered(pattern string, predicate func(Event) bool, handler EventHandler) (Unsubscribe, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.nextSubID++
	subID := eb.nextSubID

	sub := &subscription{
		id:        subID,
		pattern:   pattern,
		predicate: predicate,
		handler:   handler,
		queue:     make(chan Event, eb.queueCapacity),
		policy:    eb.overflowPolicy,
		eb:        eb,
	}

	eb.subscribers[pattern] = append(eb.subscribers[pattern], sub)
	sub.start(context.Background())

	unsubscribe := func() {
		sub.unsubOnce.Do(func() {
			eb.mu.Lock()
			defer eb.mu.Unlock()

			close(sub.queue)

			subs := eb.subscribers[pattern]
			for i, s := range subs {
				if s.id == subID {
					eb.subscribers[pattern] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
		})
	}

	return unsubscribe, nil
}

func (sub *subscription) start(ctx context.Context) {
	go func() {
		for ev := range sub.queue {
			sub.handleWithRecovery(ctx, ev)
		}
	}()
}

func (sub *subscription) handleWithRecovery(ctx context.Context, ev Event) {
	defer func() {
		if r := recover(); r != nil {
			errStr := fmt.Sprintf("handler panic: %v", r)
			sub.eb.deadLetter(ev, fmt.Sprintf("sub-%d", sub.id), "handler_panic", errStr)
		}
	}()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		lastErr = sub.handler(ctx, ev)
		if lastErr == nil {
			return
		}
	}

	sub.eb.deadLetter(ev, fmt.Sprintf("sub-%d", sub.id), "handler_error", lastErr.Error())
}

func (sub *subscription) enqueue(ev Event) {
	select {
	case sub.queue <- ev:
	default:
		if sub.policy == DropNewest {
			sub.eb.deadLetter(ev, fmt.Sprintf("sub-%d", sub.id), "queue_overflow", "queue is full (drop_newest)")
		} else {
			select {
			case oldest := <-sub.queue:
				sub.eb.deadLetter(oldest, fmt.Sprintf("sub-%d", sub.id), "queue_overflow", "queue overflow, oldest dropped")
			default:
			}
			select {
			case sub.queue <- ev:
			default:
				sub.eb.deadLetter(ev, fmt.Sprintf("sub-%d", sub.id), "queue_overflow", "queue is full after drop_oldest")
			}
		}
	}
}

func (eb *InProcessEventBus) deadLetter(ev Event, subID, reason, errStr string) {
	if ev.Topic == TopicDeadLetter {
		return
	}

	dl := DeadLetterEntry{
		Event:        ev,
		SubscriberID: subID,
		Reason:       reason,
		Error:        errStr,
		OccurredAt:   time.Now(),
	}

	_ = eb.Publish(context.Background(), Event{
		Topic:     TopicDeadLetter,
		Timestamp: time.Now(),
		Payload:   dl,
	})
}
