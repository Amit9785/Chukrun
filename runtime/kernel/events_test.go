package kernel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEventBusNamespaceValidation(t *testing.T) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	// 1. Unregistered namespace should fail
	err := eb.Publish(ctx, Event{Topic: "unregistered.event"})
	if !errors.Is(err, ErrUnknownTopic) {
		t.Errorf("expected ErrUnknownTopic, got %v", err)
	}

	// 2. Register namespace
	err = eb.RegisterNamespace("custom", "owner-1")
	if err != nil {
		t.Fatalf("failed to register namespace: %v", err)
	}

	// 3. Register duplicate namespace should fail
	err = eb.RegisterNamespace("custom", "owner-2")
	if !errors.Is(err, ErrNamespaceTaken) {
		t.Errorf("expected ErrNamespaceTaken, got %v", err)
	}

	// 4. Custom namespace should now succeed (permissive mode)
	var wg sync.WaitGroup
	wg.Add(1)
	unsub, err := eb.Subscribe("custom.my_event", func(ctx context.Context, ev Event) error {
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer unsub()

	err = eb.Publish(ctx, Event{Topic: "custom.my_event"})
	if err != nil {
		t.Errorf("expected publish to succeed, got %v", err)
	}

	wg.Wait()

	// 5. Turn strict validation off and publish unregistered topic under core namespace
	eb.SetStrictMode(false)
	err = eb.Publish(ctx, Event{Topic: "runtime.unknown"})
	if err != nil {
		t.Errorf("expected runtime.unknown to publish successfully when strict mode is off, got %v", err)
	}
}

func TestEventBusWildcardMatching(t *testing.T) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	var mu sync.Mutex
	receivedTopics := make(map[string]int)

	handler := func(topicPattern string) EventHandler {
		return func(ctx context.Context, ev Event) error {
			mu.Lock()
			receivedTopics[topicPattern]++
			mu.Unlock()
			return nil
		}
	}

	// Subscribe patterns
	unsub1, _ := eb.Subscribe("execution.started", handler("exact"))
	unsub2, _ := eb.Subscribe("execution.*", handler("single_wildcard"))
	unsub3, _ := eb.Subscribe("execution.**", handler("multi_wildcard"))
	unsub4, _ := eb.Subscribe("*", handler("all"))

	defer unsub1()
	defer unsub2()
	defer unsub3()
	defer unsub4()

	// Publish execution.started
	_ = eb.Publish(ctx, Event{Topic: "execution.started"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if receivedTopics["exact"] != 1 {
		t.Errorf("exact pattern match failed, got %d", receivedTopics["exact"])
	}
	if receivedTopics["single_wildcard"] != 1 {
		t.Errorf("single wildcard match failed, got %d", receivedTopics["single_wildcard"])
	}
	if receivedTopics["multi_wildcard"] != 1 {
		t.Errorf("multi wildcard match failed, got %d", receivedTopics["multi_wildcard"])
	}
	if receivedTopics["all"] != 1 {
		t.Errorf("all wildcard match failed, got %d", receivedTopics["all"])
	}
	mu.Unlock()
}

func TestEventBusOrderingAndNonBlocking(t *testing.T) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	_ = eb.RegisterNamespace("test", "test-owner")

	var mu sync.Mutex
	var received []int
	var wg sync.WaitGroup
	wg.Add(10)

	unsub, err := eb.Subscribe("test.order", func(ctx context.Context, ev Event) error {
		val := ev.Payload.(int)
		mu.Lock()
		received = append(received, val)
		mu.Unlock()
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer unsub()

	// Publish in order
	for i := 1; i <= 10; i++ {
		_ = eb.Publish(ctx, Event{Topic: "test.order", Payload: i})
	}

	wg.Wait()

	mu.Lock()
	for i, val := range received {
		if val != i+1 {
			t.Errorf("order mismatch: index %d got %d, expected %d", i, val, i+1)
		}
	}
	mu.Unlock()
}

func TestEventBusPanicRecoveryAndDeadLetter(t *testing.T) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	_ = eb.RegisterNamespace("test", "owner")

	var mu sync.Mutex
	var dlEntry DeadLetterEntry
	var dlWg sync.WaitGroup
	dlWg.Add(1)

	unsubDL, _ := eb.Subscribe("eventbus.dead_letter", func(ctx context.Context, ev Event) error {
		mu.Lock()
		dlEntry = ev.Payload.(DeadLetterEntry)
		mu.Unlock()
		dlWg.Done()
		return nil
	})
	defer unsubDL()

	unsubPanic, _ := eb.Subscribe("test.panic", func(ctx context.Context, ev Event) error {
		panic("boom")
	})
	defer unsubPanic()

	_ = eb.Publish(ctx, Event{Topic: "test.panic", Payload: "panic-data"})

	dlWg.Wait()

	mu.Lock()
	if dlEntry.Reason != "handler_panic" {
		t.Errorf("expected dead letter reason handler_panic, got %s", dlEntry.Reason)
	}
	if dlEntry.Error != "handler panic: boom" {
		t.Errorf("expected recovered panic text, got %s", dlEntry.Error)
	}
	if dlEntry.Event.Payload.(string) != "panic-data" {
		t.Errorf("expected original event payload, got %v", dlEntry.Event.Payload)
	}
	mu.Unlock()
}

func TestEventBusOverflowDropOldest(t *testing.T) {
	eb := NewInProcessEventBus()
	eb.SetQueueCapacity(2)
	eb.SetOverflowPolicy(DropOldest)

	ctx := context.Background()
	_ = eb.RegisterNamespace("test", "owner")

	var mu sync.Mutex
	var dls []DeadLetterEntry
	var dlWg sync.WaitGroup
	dlWg.Add(2)

	unsubDL, _ := eb.Subscribe("eventbus.dead_letter", func(ctx context.Context, ev Event) error {
		mu.Lock()
		dls = append(dls, ev.Payload.(DeadLetterEntry))
		mu.Unlock()
		dlWg.Done()
		return nil
	})
	defer unsubDL()

	started := make(chan struct{})
	blocker := make(chan struct{})
	unsubSlow, _ := eb.Subscribe("test.overflow", func(ctx context.Context, ev Event) error {
		if ev.Payload.(int) == 1 {
			close(started)
			<-blocker
		}
		return nil
	})
	defer unsubSlow()

	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 1})
	<-started // Wait for event 1 to execute and block

	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 2})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 3})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 4})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 5})

	close(blocker)

	dlWg.Wait()

	mu.Lock()
	if len(dls) != 2 {
		t.Errorf("expected 2 dead letters, got %d", len(dls))
	}
	if dls[0].Reason != "queue_overflow" || dls[0].Event.Payload.(int) != 2 {
		t.Errorf("expected first dead letter to be payload 2, got %v", dls[0].Event.Payload)
	}
	if dls[1].Reason != "queue_overflow" || dls[1].Event.Payload.(int) != 3 {
		t.Errorf("expected second dead letter to be payload 3, got %v", dls[1].Event.Payload)
	}
	mu.Unlock()
}

func TestEventBusOverflowDropNewest(t *testing.T) {
	eb := NewInProcessEventBus()
	eb.SetQueueCapacity(2)
	eb.SetOverflowPolicy(DropNewest)

	ctx := context.Background()
	_ = eb.RegisterNamespace("test", "owner")

	var mu sync.Mutex
	var dls []DeadLetterEntry
	var dlWg sync.WaitGroup
	dlWg.Add(2)

	unsubDL, _ := eb.Subscribe("eventbus.dead_letter", func(ctx context.Context, ev Event) error {
		mu.Lock()
		dls = append(dls, ev.Payload.(DeadLetterEntry))
		mu.Unlock()
		dlWg.Done()
		return nil
	})
	defer unsubDL()

	started := make(chan struct{})
	blocker := make(chan struct{})
	unsubSlow, _ := eb.Subscribe("test.overflow", func(ctx context.Context, ev Event) error {
		if ev.Payload.(int) == 1 {
			close(started)
			<-blocker
		}
		return nil
	})
	defer unsubSlow()

	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 1})
	<-started

	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 2})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 3})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 4})
	_ = eb.Publish(ctx, Event{Topic: "test.overflow", Payload: 5})

	close(blocker)

	dlWg.Wait()

	mu.Lock()
	if len(dls) != 2 {
		t.Errorf("expected 2 dead letters, got %d", len(dls))
	}
	if dls[0].Reason != "queue_overflow" || dls[0].Event.Payload.(int) != 4 {
		t.Errorf("expected first dead letter to be payload 4, got %v", dls[0].Event.Payload)
	}
	if dls[1].Reason != "queue_overflow" || dls[1].Event.Payload.(int) != 5 {
		t.Errorf("expected second dead letter to be payload 5, got %v", dls[1].Event.Payload)
	}
	mu.Unlock()
}

func BenchmarkEventBusPublish(b *testing.B) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	_ = eb.RegisterNamespace("test", "owner")
	unsub, _ := eb.Subscribe("test.bench", func(ctx context.Context, ev Event) error {
		return nil
	})
	defer unsub()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eb.Publish(ctx, Event{Topic: "test.bench"})
	}
}
