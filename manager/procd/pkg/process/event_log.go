package process

import (
	"sync"
	"time"
)

const defaultEventLogCapacity = 4096

// EventLog is a bounded replay log plus live fan-out for process events.
type EventLog struct {
	mu          sync.Mutex
	nextSeq     int64
	capacity    int
	events      []ProcessEvent
	subscribers map[chan ProcessEvent]struct{}
	closed      bool
}

// NewEventLog creates a bounded event log.
func NewEventLog(capacity int) *EventLog {
	if capacity <= 0 {
		capacity = defaultEventLogCapacity
	}
	return &EventLog{
		nextSeq:     1,
		capacity:    capacity,
		events:      make([]ProcessEvent, 0, capacity),
		subscribers: make(map[chan ProcessEvent]struct{}),
	}
}

// Publish assigns a sequence number, stores the event, and broadcasts it.
func (l *EventLog) Publish(event ProcessEvent) ProcessEvent {
	l.mu.Lock()
	defer l.mu.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Seq = l.nextSeq
	l.nextSeq++

	if len(l.events) >= l.capacity {
		copy(l.events, l.events[1:])
		l.events[len(l.events)-1] = event
	} else {
		l.events = append(l.events, event)
	}

	for sub := range l.subscribers {
		select {
		case sub <- event:
		default:
		}
	}

	return event
}

// Snapshot returns replay metadata.
func (l *EventLog) Snapshot() EventLogSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshotLocked()
}

func (l *EventLog) snapshotLocked() EventLogSnapshot {
	oldestSeq := l.nextSeq
	if len(l.events) > 0 {
		oldestSeq = l.events[0].Seq
	}
	return EventLogSnapshot{
		NextSeq:   l.nextSeq,
		OldestSeq: oldestSeq,
		Capacity:  l.capacity,
	}
}

// Subscribe replays events newer than cursor and then streams live events.
func (l *EventLog) Subscribe(cursor int64) (<-chan ProcessEvent, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	sub := make(chan ProcessEvent, l.capacity+1)
	snapshot := l.snapshotLocked()
	if cursor > 0 && len(l.events) > 0 && cursor < snapshot.OldestSeq {
		sub <- ProcessEvent{
			Seq:       snapshot.OldestSeq - 1,
			Type:      EventTypeCursorLost,
			Timestamp: time.Now().UTC(),
			Payload: map[string]any{
				"requested_cursor": cursor,
				"oldest_seq":       snapshot.OldestSeq,
			},
		}
	}
	for _, event := range l.events {
		if cursor == 0 || event.Seq > cursor {
			sub <- event
		}
	}
	if l.closed {
		close(sub)
		return sub, func() {}
	}
	l.subscribers[sub] = struct{}{}
	cancel := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if _, ok := l.subscribers[sub]; ok {
			delete(l.subscribers, sub)
			close(sub)
		}
	}
	return sub, cancel
}

// Close closes all live subscriptions. History remains replayable.
func (l *EventLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for sub := range l.subscribers {
		close(sub)
		delete(l.subscribers, sub)
	}
}
