package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestEventBusPubSub(t *testing.T) {
	eb := NewInProcessEventBus()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	var receivedEvent1 Event
	var receivedEvent2 Event

	// Subscribe to specific event type
	unsub1, err := eb.Subscribe("test.event", func(ev Event) {
		receivedEvent1 = ev
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer unsub1()

	// Subscribe to wildcard
	unsub2, err := eb.Subscribe("*", func(ev Event) {
		receivedEvent2 = ev
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe wildcard: %v", err)
	}
	defer unsub2()

	testEvent := Event{
		Type:      "test.event",
		Timestamp: time.Now(),
		Payload:   map[string]any{"data": "hello"},
	}

	err = eb.Publish(ctx, testEvent)
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for goroutines to process
	wg.Wait()

	if receivedEvent1.Type != "test.event" {
		t.Errorf("expected received event type test.event, got: %s", receivedEvent1.Type)
	}
	if receivedEvent2.Type != "test.event" {
		t.Errorf("expected received wildcard event type test.event, got: %s", receivedEvent2.Type)
	}
	if receivedEvent1.Payload["data"] != "hello" {
		t.Errorf("expected payload data 'hello', got: %v", receivedEvent1.Payload["data"])
	}
}
