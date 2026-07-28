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
	"sort"
	"sync"
	"time"
)

const (
	journalCompactionMinStaleBytes int64 = 1 << 20
	journalCompactionMaxStaleBytes int64 = 64 << 20
)

type journalSubscriber struct {
	ch chan Event
}

// journalEntry is the compact in-memory index for one persisted event.
type journalEntry struct {
	seq            int64
	offset         int64
	size           int64
	occurredAtNano int64
}

// EventBacklog streams a stable journal snapshot without retaining event
// payloads in the procd heap.
type EventBacklog struct {
	file    *os.File
	entries []journalEntry
	next    int
	closed  bool
}

// Next returns the next retained event in the snapshot.
func (b *EventBacklog) Next() (Event, bool, error) {
	if b == nil || b.closed || b.next >= len(b.entries) {
		return Event{}, false, nil
	}
	event, err := readJournalEvent(b.file, b.entries[b.next])
	if err != nil {
		return Event{}, false, err
	}
	b.next++
	return event, true, nil
}

// Close releases the snapshot file descriptor.
func (b *EventBacklog) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	if b.file == nil {
		return nil
	}
	return b.file.Close()
}

// Journal is a bounded, cursor-addressable event log with live subscriptions.
// Event payloads stay on disk; memory contains only a compact offset index and
// bounded subscriber channels.
type Journal struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	fileSize    int64
	retention   EventRetentionSpec
	entries     []journalEntry
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
	entries, validBytes, err := loadJournal(path)
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
	for _, entry := range entries {
		if entry.seq > latest {
			latest = entry.seq
		}
		total += entry.size
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open event journal: %w", err)
	}
	journal := &Journal{
		path:        path,
		file:        file,
		fileSize:    validBytes,
		retention:   retention,
		entries:     entries,
		totalBytes:  total,
		nextSeq:     latest + 1,
		subscribers: map[uint64]*journalSubscriber{},
	}
	if journal.nextSeq <= 0 {
		journal.nextSeq = 1
	}
	if journal.trimLocked(time.Now()) {
		if err := journal.compactLocked(); err != nil {
			_ = journal.file.Close()
			return nil, err
		}
	}
	return journal, nil
}

func loadJournal(path string) ([]journalEntry, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open event journal for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64*1024)
	var entries []journalEntry
	var previous int64
	var validBytes int64
	for {
		offset := validBytes
		line, readErr := reader.ReadBytes('\n')
		complete := bytes.HasSuffix(line, []byte{'\n'})
		if len(bytes.TrimSpace(line)) > 0 {
			if readErr == io.EOF && !complete {
				break
			}
			var event Event
			if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
				return nil, 0, fmt.Errorf("decode event journal: %w", err)
			}
			if event.Seq <= previous {
				return nil, 0, fmt.Errorf("event journal sequence %d is not greater than %d", event.Seq, previous)
			}
			previous = event.Seq
			entries = append(entries, journalEntry{
				seq:            event.Seq,
				offset:         offset,
				size:           int64(len(line)),
				occurredAtNano: event.OccurredAt.UnixNano(),
			})
		}
		if complete {
			validBytes += int64(len(line))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, fmt.Errorf("read event journal: %w", readErr)
		}
	}
	return entries, validBytes, nil
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
	offset := j.fileSize
	written, err := j.file.Write(line)
	if err != nil || written != len(line) {
		writeErr := err
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if truncateErr := j.file.Truncate(offset); truncateErr != nil {
			return Event{}, errors.Join(fmt.Errorf("append event journal: %w", writeErr), fmt.Errorf("rollback partial event journal append: %w", truncateErr))
		}
		return Event{}, fmt.Errorf("append event journal: %w", writeErr)
	}
	size := int64(len(line))
	j.fileSize += size
	j.nextSeq++
	j.entries = append(j.entries, journalEntry{
		seq:            event.Seq,
		offset:         offset,
		size:           size,
		occurredAtNano: event.OccurredAt.UnixNano(),
	})
	j.totalBytes += size
	if j.trimLocked(event.OccurredAt) {
		if err := j.maybeCompactLocked(); err != nil {
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
	if j.closed {
		j.mu.Unlock()
		return EventPage{}, errors.New("event journal is closed")
	}
	if j.trimLocked(time.Now()) {
		if err := j.maybeCompactLocked(); err != nil {
			j.mu.Unlock()
			return EventPage{}, err
		}
	}
	if err := j.validateCursorLocked(after); err != nil {
		j.mu.Unlock()
		return EventPage{}, err
	}
	if limit <= 0 || limit > 10_000 {
		limit = 1000
	}
	backlog, err := j.openBacklogLocked(after, limit)
	cursor := j.cursorLocked()
	j.mu.Unlock()
	if err != nil {
		return EventPage{}, err
	}
	defer backlog.Close()

	events := make([]Event, 0, len(backlog.entries))
	for {
		event, ok, err := backlog.Next()
		if err != nil {
			return EventPage{}, err
		}
		if !ok {
			break
		}
		events = append(events, event)
	}
	return EventPage{Events: events, Cursor: cursor}, nil
}

func (j *Journal) Subscribe(after int64) (*EventBacklog, <-chan Event, func(), EventCursor, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, nil, nil, EventCursor{}, errors.New("event journal is closed")
	}
	if j.trimLocked(time.Now()) {
		if err := j.maybeCompactLocked(); err != nil {
			return nil, nil, nil, EventCursor{}, err
		}
	}
	if err := j.validateCursorLocked(after); err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	backlog, err := j.openBacklogLocked(after, 0)
	if err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	j.nextSubID++
	id := j.nextSubID
	subscriber := &journalSubscriber{ch: make(chan Event, defaultSubscriptionBacklog)}
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
	return j.maybeCompactLocked()
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

func (j *Journal) openBacklogLocked(after int64, limit int) (*EventBacklog, error) {
	start := sort.Search(len(j.entries), func(i int) bool {
		return j.entries[i].seq > after
	})
	end := len(j.entries)
	if limit > 0 && end-start > limit {
		end = start + limit
	}
	entries := append([]journalEntry(nil), j.entries[start:end]...)
	if len(entries) == 0 {
		return &EventBacklog{}, nil
	}
	file, err := os.Open(j.path)
	if err != nil {
		return nil, fmt.Errorf("open event journal snapshot: %w", err)
	}
	return &EventBacklog{file: file, entries: entries}, nil
}

func readJournalEvent(file *os.File, entry journalEntry) (Event, error) {
	if file == nil {
		return Event{}, errors.New("event journal snapshot is closed")
	}
	if entry.size <= 0 || entry.size > int64(maxInt()) {
		return Event{}, fmt.Errorf("invalid event journal record size %d", entry.size)
	}
	line := make([]byte, int(entry.size))
	if _, err := file.ReadAt(line, entry.offset); err != nil {
		return Event{}, fmt.Errorf("read event journal record: %w", err)
	}
	var event Event
	if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
		return Event{}, fmt.Errorf("decode event journal record: %w", err)
	}
	if event.Seq != entry.seq {
		return Event{}, fmt.Errorf("event journal record sequence %d does not match index %d", event.Seq, entry.seq)
	}
	return event, nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
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
	if len(j.entries) > 0 {
		earliest = j.entries[0].seq
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
		cutoff := now.Add(-time.Duration(j.retention.MaxAgeSeconds) * time.Second).UnixNano()
		for removed < len(j.entries) && j.entries[removed].occurredAtNano < cutoff {
			j.totalBytes -= j.entries[removed].size
			removed++
		}
	}
	if j.retention.MaxBytes > 0 {
		for removed < len(j.entries) && j.totalBytes > j.retention.MaxBytes {
			j.totalBytes -= j.entries[removed].size
			removed++
		}
	}
	if removed == 0 {
		return false
	}
	j.entries = j.entries[removed:]
	return true
}

func (j *Journal) maybeCompactLocked() error {
	staleBytes := j.staleBytesLocked()
	if staleBytes == 0 {
		return nil
	}
	retainedBytes := j.fileSize - staleBytes
	threshold := j.retention.MaxBytes / 4
	if threshold < journalCompactionMinStaleBytes {
		threshold = journalCompactionMinStaleBytes
	}
	if threshold > journalCompactionMaxStaleBytes {
		threshold = journalCompactionMaxStaleBytes
	}
	if staleBytes < threshold && staleBytes < retainedBytes {
		return nil
	}
	return j.compactLocked()
}

func (j *Journal) staleBytesLocked() int64 {
	if len(j.entries) == 0 {
		return j.fileSize
	}
	return j.entries[0].offset
}

func (j *Journal) compactLocked() error {
	start := j.staleBytesLocked()
	if start == 0 {
		return nil
	}
	retainedBytes := j.fileSize - start
	tmp, err := os.CreateTemp(filepath.Dir(j.path), ".events-*.tmp")
	if err != nil {
		return fmt.Errorf("create compacted event journal: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod compacted event journal: %w", err)
	}
	if retainedBytes > 0 {
		if _, err := io.CopyN(tmp, io.NewSectionReader(j.file, start, retainedBytes), retainedBytes); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("copy compacted event journal: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync compacted event journal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close compacted event journal: %w", err)
	}
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close event journal before compaction: %w", err)
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		file, reopenErr := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
		if reopenErr == nil {
			j.file = file
		}
		return errors.Join(fmt.Errorf("replace compacted event journal: %w", err), reopenErr)
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen compacted event journal: %w", err)
	}
	j.file = file
	j.fileSize = retainedBytes
	for i := range j.entries {
		j.entries[i].offset -= start
	}
	return nil
}
