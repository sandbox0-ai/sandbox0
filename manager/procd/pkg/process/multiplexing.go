package process

import "sync"

// MultiplexedChannel provides a fan-out mechanism for process output.
// Multiple subscribers can receive the same events.
type MultiplexedChannel[T any] struct {
	mu          sync.RWMutex
	Source      chan T
	subscribers []chan T
	bufferSize  int
	history     []T
	historySize int
	closed      bool
}

// NewMultiplexedChannel creates a new multiplexed channel.
func NewMultiplexedChannel[T any](bufferSize int) *MultiplexedChannel[T] {
	mc := &MultiplexedChannel[T]{
		Source:      make(chan T, bufferSize),
		subscribers: make([]chan T, 0),
		bufferSize:  bufferSize,
		history:     make([]T, 0, bufferSize),
		historySize: bufferSize,
	}

	go mc.dispatch()

	return mc
}

func (mc *MultiplexedChannel[T]) dispatch() {
	for event := range mc.Source {
		mc.mu.RLock()
		for _, sub := range mc.subscribers {
			select {
			case sub <- event:
			default:
				// Subscriber buffer full, drop event
			}
		}
		mc.mu.RUnlock()
	}

	// Source closed, close all subscribers
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, sub := range mc.subscribers {
		close(sub)
	}
	mc.closed = true
}

// Fork creates a new subscription to the channel.
// Returns the subscription channel and a cancel function.
func (mc *MultiplexedChannel[T]) Fork() (<-chan T, func()) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	sub := make(chan T, mc.bufferSize)
replayHistory:
	for _, event := range mc.history {
		select {
		case sub <- event:
		default:
			break replayHistory
		}
	}

	if mc.closed {
		close(sub)
		return sub, func() {}
	}

	mc.subscribers = append(mc.subscribers, sub)

	cancel := func() {
		mc.Unsubscribe(sub)
	}

	return sub, cancel
}

// Unsubscribe removes a subscriber from the channel.
// It's safe to call multiple times or on already-closed channels.
func (mc *MultiplexedChannel[T]) Unsubscribe(sub chan T) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for i, s := range mc.subscribers {
		if s == sub {
			mc.subscribers = append(mc.subscribers[:i], mc.subscribers[i+1:]...)
			// Safely close the channel - use recover to handle already-closed channels
			defer func() {
				recover() // Ignore panic from closing an already-closed channel
			}()
			close(sub)
			return
		}
	}
}

// Publish sends an event to all subscribers.
// It's safe to call after Close - events will be silently dropped.
func (mc *MultiplexedChannel[T]) Publish(event T) {
	mc.mu.RLock()
	closed := mc.closed
	mc.mu.RUnlock()

	if closed {
		return // Channel is closed, drop the event
	}

	mc.mu.Lock()
	if mc.historySize > 0 {
		if len(mc.history) >= mc.historySize {
			copy(mc.history, mc.history[1:])
			mc.history[len(mc.history)-1] = event
		} else {
			mc.history = append(mc.history, event)
		}
	}
	mc.mu.Unlock()

	select {
	case mc.Source <- event:
	default:
		// Source buffer full
	}
}

// Close closes the multiplexed channel.
// It's safe to call multiple times.
func (mc *MultiplexedChannel[T]) Close() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return // Already closed
	}

	mc.closed = true
	close(mc.Source)
}

// SubscriberCount returns the number of active subscribers.
func (mc *MultiplexedChannel[T]) SubscriberCount() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.subscribers)
}
