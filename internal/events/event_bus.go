package events

import (
	"fmt"
	"slices"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

type (
	Event interface{}

	Topic string

	EventBus struct {
		logger *logger.Logger

		mu          sync.RWMutex
		subscribers map[Topic][]chan Event
	}
)

func NewEventBus(log *logger.Logger) *EventBus {
	return &EventBus{
		logger:      log,
		subscribers: make(map[Topic][]chan Event),
	}
}

// Subscribe creates a channel, adds it to the subscribers list, and returns it to the caller.
func (bus *EventBus) Subscribe(topic Topic) <-chan Event {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	ch := make(chan Event, 1)
	bus.subscribers[topic] = append(bus.subscribers[topic], ch)
	return ch
}

// Publish sends the event to all subscribers.
// If the subscriber is busy then the event is dropped.
func (bus *EventBus) Publish(topic Topic, event Event) {
	bus.mu.RLock()
	defer bus.mu.RUnlock()

	subscribers, found := bus.subscribers[topic]
	if !found {
		bus.logger.Warn("Event not published, no subscriber found", "topic", topic)
		return
	}
	for _, sub := range subscribers {
		select {
		case sub <- event:
		default:
			bus.logger.Warn("Dropped event for a slow subscriber", "topic", topic, "event", event)
		}
	}
}

// Unsubscribe removes the subscriber from the subscribers list,
// returns error if the provided topic does not exist or the subscriber was not found.
func (bus *EventBus) Unsubscribe(topic Topic, sub <-chan Event) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	subs, found := bus.subscribers[topic]
	if !found {
		return fmt.Errorf("topic not found: %s", topic)
	}
	for i, s := range subs {
		if s == sub {
			bus.subscribers[topic] = slices.Delete(subs, i, i+1)
			return nil
		}
	}
	return fmt.Errorf("subscriber not found for topic: %s", topic)
}
