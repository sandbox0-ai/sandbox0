package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type journalSubscriber struct {
	ch chan Event
}

// Journal is a bounded, cursor-addressable event log with live subscriptions.
type Journal struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	retention   EventRetentionSpec
	events      []Event
	eventSizes  []int64
	totalBytes  int64
	nextSeq     int64
	subscribers map[uint64]*journalSubscriber
	nextSubID   uint64
	closed      bool
}

func OpenJournal(path string, retention EventRetentionSpec, lastSeq int64) (*Journal, error) {
	retention = normalizeSpec(SessionSpec{EventRetention: retention}).EventRetention
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create event journal directory: %w", err)
	}
	events, sizes, validBytes, err := loadJournal(path)
	if err != nil {
		return nil, err
	}
	if info, statErr := os.Stat(path); statErr == nil && info.Size() > validBytes {
		if err := os.Truncate(path, validBytes); err != nil {
			return nil, fmt.Errorf("truncate incomplete event journal tail: %w", err)
		}
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("stat event journal: %w", statErr)
	}
	latest := lastSeq
	var total int64
	for i := range events {
		if events[i].Seq > latest {
			latest = events[i].Seq
		}
		total += sizes[i]
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open event journal: %w", err)
	}
	journal := &Journal{
		path:        path,
		file:        file,
		retention:   retention,
		events:      events,
		eventSizes:  sizes,
		totalBytes:  total,
		nextSeq:     latest + 1,
		subscribers: map[uint64]*journalSubscriber{},
	}
	if journal.nextSeq <= 0 {
		journal.nextSeq = 1
	}
	if journal.trimLocked(time.Now()) {
		if err := journal.compactLocked(); err != nil {
			return nil, err
		}
	}
	return journal, nil
}

func loadJournal(path string) ([]Event, []int64, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, 0, nil
	}
	if err != nil {
		return nil, nil, 0, fmt.Errorf("open event journal for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64*1024)
	var events []Event
	var sizes []int64
	var previous int64
	var validBytes int64
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			if readErr == io.EOF && !bytes.HasSuffix(line, []byte{'\n'}) {
				break
			}
			var event Event
			if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
				return nil, nil, 0, fmt.Errorf("decode event journal: %w", err)
			}
			if event.Seq <= previous {
				return nil, nil, 0, fmt.Errorf("event journal sequence %d is not greater than %d", event.Seq, previous)
			}
			previous = event.Seq
			events = append(events, event)
			sizes = append(sizes, int64(len(line)))
		}
		if bytes.HasSuffix(line, []byte{'\n'}) {
			validBytes += int64(len(line))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, nil, 0, fmt.Errorf("read event journal: %w", readErr)
		}
	}
	return events, sizes, validBytes, nil
}

func (j *Journal) Append(event Event) (Event, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Event{}, errors.New("event journal is closed")
	}
	event.Seq = j.nextSeq
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	line, err := json.Marshal(event)
	if err != nil {
		return Event{}, fmt.Errorf("encode event: %w", err)
	}
	line = append(line, '\n')
	info, err := j.file.Stat()
	if err != nil {
		return Event{}, fmt.Errorf("stat event journal before append: %w", err)
	}
	written, err := j.file.Write(line)
	if err != nil || written != len(line) {
		writeErr := err
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if truncateErr := j.file.Truncate(info.Size()); truncateErr != nil {
			return Event{}, errors.Join(fmt.Errorf("append event journal: %w", writeErr), fmt.Errorf("rollback partial event journal append: %w", truncateErr))
		}
		return Event{}, fmt.Errorf("append event journal: %w", writeErr)
	}
	j.nextSeq++
	j.events = append(j.events, event)
	j.eventSizes = append(j.eventSizes, int64(len(line)))
	j.totalBytes += int64(len(line))
	trimmed := j.trimLocked(event.OccurredAt)
	if trimmed {
		if err := j.compactLocked(); err != nil {
			return event, err
		}
	}
	for id, subscriber := range j.subscribers {
		select {
		case subscriber.ch <- event:
		default:
			close(subscriber.ch)
			delete(j.subscribers, id)
		}
	}
	return event, nil
}

func (j *Journal) Read(after int64, limit int) (EventPage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.trimLocked(time.Now()) {
		if err := j.compactLocked(); err != nil {
			return EventPage{}, err
		}
	}
	if err := j.validateCursorLocked(after); err != nil {
		return EventPage{}, err
	}
	if limit <= 0 || limit > 10_000 {
		limit = 1000
	}
	events := make([]Event, 0, min(limit, len(j.events)))
	for _, event := range j.events {
		if event.Seq <= after {
			continue
		}
		events = append(events, event)
		if len(events) == limit {
			break
		}
	}
	return EventPage{Events: events, Cursor: j.cursorLocked()}, nil
}

func (j *Journal) Subscribe(after int64) ([]Event, <-chan Event, func(), EventCursor, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, nil, nil, EventCursor{}, errors.New("event journal is closed")
	}
	if j.trimLocked(time.Now()) {
		if err := j.compactLocked(); err != nil {
			return nil, nil, nil, EventCursor{}, err
		}
	}
	if err := j.validateCursorLocked(after); err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	backlog := make([]Event, 0)
	for _, event := range j.events {
		if event.Seq > after {
			backlog = append(backlog, event)
		}
	}
	j.nextSubID++
	id := j.nextSubID
	buffer := defaultSubscriptionBacklog
	if len(backlog) > buffer {
		buffer = len(backlog)
	}
	subscriber := &journalSubscriber{ch: make(chan Event, buffer)}
	j.subscribers[id] = subscriber
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			j.mu.Lock()
			defer j.mu.Unlock()
			current, ok := j.subscribers[id]
			if !ok {
				return
			}
			delete(j.subscribers, id)
			close(current.ch)
		})
	}
	return backlog, subscriber.ch, cancel, j.cursorLocked(), nil
}

func (j *Journal) Cursor() EventCursor {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cursorLocked()
}

func (j *Journal) SetRetention(retention EventRetentionSpec) error {
	retention = normalizeSpec(SessionSpec{EventRetention: retention}).EventRetention
	j.mu.Lock()
	defer j.mu.Unlock()
	j.retention = retention
	if !j.trimLocked(time.Now()) {
		return nil
	}
	return j.compactLocked()
}

// Prune applies time- and size-based retention without requiring a new event.
func (j *Journal) Prune(now time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed || !j.trimLocked(now) {
		return nil
	}
	return j.compactLocked()
}

func (j *Journal) Flush() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed || j.file == nil {
		return nil
	}
	return j.file.Sync()
}

func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	for id, subscriber := range j.subscribers {
		close(subscriber.ch)
		delete(j.subscribers, id)
	}
	if j.file == nil {
		return nil
	}
	if err := j.file.Sync(); err != nil {
		_ = j.file.Close()
		return err
	}
	return j.file.Close()
}

func (j *Journal) validateCursorLocked(after int64) error {
	if after < 0 {
		return errors.New("event cursor must be non-negative")
	}
	cursor := j.cursorLocked()
	if after > cursor.Latest {
		return fmt.Errorf("event cursor must not be greater than latest sequence %d", cursor.Latest)
	}
	if cursor.Earliest > 1 && after < cursor.Earliest-1 {
		return &CursorExpiredError{Earliest: cursor.Earliest}
	}
	return nil
}

func (j *Journal) cursorLocked() EventCursor {
	latest := j.nextSeq - 1
	if latest < 0 {
		latest = 0
	}
	earliest := int64(0)
	if len(j.events) > 0 {
		earliest = j.events[0].Seq
	} else if latest > 0 {
		// No event is retained, so the next sequence is the lower bound of the
		// retained window. A client can resume only after the current latest.
		earliest = j.nextSeq
	}
	return EventCursor{Earliest: earliest, Latest: latest}
}

func (j *Journal) trimLocked(now time.Time) bool {
	removed := 0
	if j.retention.MaxAgeSeconds > 0 {
		cutoff := now.Add(-time.Duration(j.retention.MaxAgeSeconds) * time.Second)
		for removed < len(j.events) && j.events[removed].OccurredAt.Before(cutoff) {
			j.totalBytes -= j.eventSizes[removed]
			removed++
		}
	}
	if j.retention.MaxBytes > 0 {
		for removed < len(j.events) && j.totalBytes > j.retention.MaxBytes {
			j.totalBytes -= j.eventSizes[removed]
			removed++
		}
	}
	if removed == 0 {
		return false
	}
	j.events = append([]Event(nil), j.events[removed:]...)
	j.eventSizes = append([]int64(nil), j.eventSizes[removed:]...)
	return true
}

func (j *Journal) compactLocked() error {
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close event journal before compaction: %w", err)
	}
	var data bytes.Buffer
	for _, event := range j.events {
		line, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode compacted event: %w", err)
		}
		data.Write(line)
		data.WriteByte('\n')
	}
	if err := writeFileAtomic(j.path, data.Bytes(), 0o600); err != nil {
		return fmt.Errorf("compact event journal: %w", err)
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen compacted event journal: %w", err)
	}
	j.file = file
	j.totalBytes = 0
	j.eventSizes = j.eventSizes[:0]
	for _, event := range j.events {
		line, _ := json.Marshal(event)
		size := int64(len(line) + 1)
		j.eventSizes = append(j.eventSizes, size)
		j.totalBytes += size
	}
	return nil
}
