package session

import (
	"errors"
	"sync"
	"time"
)

// sessionJournal permits stopped sessions to retain only their persisted cursor
// during activation. Mutable sessions still use a fully recovered Journal.
type sessionJournal interface {
	Append(Event) (Event, error)
	Read(int64, int) (EventPage, error)
	Subscribe(int64) (*EventBacklog, <-chan Event, func(), EventCursor, error)
	Cursor() EventCursor
	Stats() JournalStats
	SetRetention(EventRetentionSpec) error
	Prune(time.Time) error
	Flush() error
	Close() error
}

// deferredJournal opens once on first use, or during the ordinary retention
// sweep. Close and Flush do not read an untouched journal: it has no new dirty
// state. A corrupt stopped journal fails its own operation, not sandbox startup.
type deferredJournal struct {
	mu        sync.Mutex
	path      string
	retention EventRetentionSpec
	cursor    EventCursor
	journal   *Journal
	closed    bool
}

func (d *deferredJournal) openLocked() (*Journal, error) {
	if d.closed {
		return nil, errors.New("session journal is closed")
	}
	if d.journal == nil {
		j, err := OpenJournal(d.path, d.retention, d.cursor)
		if err != nil {
			return nil, err
		}
		d.journal = j
	}
	return d.journal, nil
}

func (d *deferredJournal) Cursor() EventCursor {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.journal != nil {
		return d.journal.Cursor()
	}
	return d.cursor
}
func (d *deferredJournal) Stats() JournalStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.journal != nil {
		return d.journal.Stats()
	}
	return JournalStats{}
}
func (d *deferredJournal) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	if d.journal != nil {
		return d.journal.Close()
	}
	return nil
}
func (d *deferredJournal) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.journal != nil {
		return d.journal.Flush()
	}
	return nil
}

func (d *deferredJournal) Append(event Event) (Event, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, err := d.openLocked()
	if err != nil {
		return Event{}, err
	}
	return j.Append(event)
}

func (d *deferredJournal) Read(after int64, limit int) (EventPage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, err := d.openLocked()
	if err != nil {
		return EventPage{}, err
	}
	return j.Read(after, limit)
}

func (d *deferredJournal) Subscribe(after int64) (*EventBacklog, <-chan Event, func(), EventCursor, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, err := d.openLocked()
	if err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	return j.Subscribe(after)
}

func (d *deferredJournal) SetRetention(retention EventRetentionSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, err := d.openLocked()
	if err != nil {
		return err
	}
	return j.SetRetention(retention)
}

func (d *deferredJournal) Prune(now time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, err := d.openLocked()
	if err != nil {
		return err
	}
	return j.Prune(now)
}
