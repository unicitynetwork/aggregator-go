package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

func TestEventBusPubSub(t *testing.T) {
	bus := NewEventBus(newLogger(t))

	// Subscribe to the event topic
	ch := bus.Subscribe(TopicTest)

	// Publish an event
	event := TestEvent{}
	bus.Publish(TopicTest, event)

	// Verify the event was received
	select {
	case e := <-ch:
		require.Equal(t, event, e)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "Event was not received within timeout")
	}
}

func TestEventBusPublishToMultipleSubscribers(t *testing.T) {
	bus := NewEventBus(newLogger(t))

	// Subscribe to the event
	ch1 := bus.Subscribe(TopicTest)
	ch2 := bus.Subscribe(TopicTest)

	// Create and publish an event
	event := TestEvent{}
	bus.Publish(TopicTest, event)

	// Verify both subscribers received the event
	select {
	case e := <-ch1:
		require.Equal(t, event, e)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "First subscriber did not receive event within timeout")
	}

	select {
	case e := <-ch2:
		require.Equal(t, event, e)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "Second subscriber did not receive event within timeout")
	}
}

func TestEventBusUnsubscribe(t *testing.T) {
	bus := NewEventBus(newLogger(t))

	// Subscribe to the event topic
	ch1 := bus.Subscribe(TopicTest)
	ch2 := bus.Subscribe(TopicTest)
	ch3 := bus.Subscribe(TopicTest)

	// Unsubscribe ch2
	err := bus.Unsubscribe(TopicTest, ch2)
	require.NoError(t, err)

	// Try to unsubscribe ch2 again - should return error
	err = bus.Unsubscribe(TopicTest, ch2)
	require.ErrorContains(t, err, "subscriber not found")

	// Try to unsubscribe ch2 on wrong topic - should return error
	err = bus.Unsubscribe("invalid-topic", ch2)
	require.ErrorContains(t, err, "topic not found")

	// Publish an event
	event := TestEvent{}
	bus.Publish(TopicTest, event)

	// Verify ch1 and ch3 receive the event
	select {
	case e := <-ch1:
		require.Equal(t, event, e)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "ch1 should have received the event")
	}

	select {
	case e := <-ch3:
		require.Equal(t, event, e)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "ch3 should have received the event")
	}

	// Verify ch2 did not receive the event
	select {
	case <-ch2:
		require.Fail(t, "ch2 should not have received the event")
	default:
	}
}

func newLogger(t *testing.T) *logger.Logger {
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	return testLogger
}

const TopicTest Topic = "test_event"

type TestEvent struct {
}
