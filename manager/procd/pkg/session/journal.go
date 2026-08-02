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
	"strconv"
	"sync"
	"time"
)

type journalSubscriber struct {
	ch chan Event
}

// journalEntry is the compact on-disk index for one legacy JSONL event.
type journalEntry struct {
	seq            int64
	offset         int64
	size           int64
	occurredAtNano int64
}

type journalBacklogFormat uint8

const (
	journalBacklogLegacy journalBacklogFormat = iota + 1
	journalBacklogSegment
)

type journalBacklogSource struct {
	format  journalBacklogFormat
	file    *os.File
	path    string
	entries []journalEntry
	next    int
	offset  int64
	end     int64
}

// EventBacklog streams a stable journal snapshot without retaining event
// payloads in the procd heap.
type EventBacklog struct {
	mu         sync.Mutex
	sources    []journalBacklogSource
	nextSource int
	after      int64
	head       int64
	remaining  int
	release    func()
	closed     bool
}

// Next returns the next retained event in the snapshot.
func (b *EventBacklog) Next() (Event, bool, error) {
	if b == nil {
		return Event{}, false, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.remaining == 0 {
		return Event{}, false, nil
	}
	for b.nextSource < len(b.sources) {
		source := &b.sources[b.nextSource]
		switch source.format {
		case journalBacklogLegacy:
			for source.next < len(source.entries) {
				entry := source.entries[source.next]
				source.next++
				if entry.seq <= b.after || entry.seq < b.head {
					continue
				}
				event, err := readLegacyJournalEvent(source.file, entry)
				if err != nil {
					return Event{}, false, err
				}
				b.consumeOne()
				return event, true, nil
			}
		case journalBacklogSegment:
			for source.offset < source.end {
				meta, err := readJournalFrameMeta(source.file, source.offset, source.end)
				if err != nil {
					return Event{}, false, fmt.Errorf("read journal backlog metadata: %w", err)
				}
				source.offset += meta.frameSize
				if meta.seq <= b.after || meta.seq < b.head {
					continue
				}
				event, err := readJournalFrame(source.file, meta)
				if err != nil {
					return Event{}, false, err
				}
				b.consumeOne()
				return event, true, nil
			}
		default:
			return Event{}, false, errors.New("event journal snapshot has an invalid source")
		}
		b.nextSource++
	}
	return Event{}, false, nil
}

func (b *EventBacklog) consumeOne() {
	if b.remaining > 0 {
		b.remaining--
	}
}

// Close releases snapshot file descriptors and segment deletion pins.
func (b *EventBacklog) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	var closeErr error
	for i := range b.sources {
		if b.sources[i].file != nil {
			closeErr = errors.Join(closeErr, b.sources[i].file.Close())
		}
	}
	release := b.release
	b.release = nil
	b.mu.Unlock()
	if release != nil {
		release()
	}
	return closeErr
}

// JournalStats exposes bounded format-level diagnostics without session event
// payloads. It is intended for activation logs and tests.
type JournalStats struct {
	FormatVersion      int
	SealedSegments     int
	ActiveBytes        int64
	ActiveEvents       int
	RetainedBytes      int64
	RetainedBytesKnown bool
	LegacyDeferred     bool
}

// Journal is a bounded, cursor-addressable event log with live subscriptions.
// V2 stores immutable sealed segments plus one bounded active tail. Legacy
// JSONL is immutable and indexed lazily, keeping session activation independent
// of retained history size.
type Journal struct {
	mu sync.Mutex

	legacyPath   string
	dir          string
	manifestPath string
	manifest     journalManifest

	activeFile         *os.File
	activeSize         int64
	activeLogicalBytes int64
	activeEntries      []journalRecordMeta

	retention     EventRetentionSpec
	retainedBytes int64
	retainedKnown bool
	headSeq       int64
	nextSeq       int64

	legacyOnce    sync.Once
	legacyLoadErr error
	legacyEntries []journalEntry
	legacyLoaded  bool

	headCacheSource string
	headCache       []journalRecordMeta
	headCacheIndex  int

	subscribers   map[uint64]*journalSubscriber
	nextSubID     uint64
	pins          map[string]int
	pendingDelete map[string]struct{}
	dirty         bool
	fatalErr      error
	closed        bool
}

type recoveredJournalFile struct {
	name    string
	file    *os.File
	entries []journalRecordMeta
	size    int64
	sealed  bool
	meta    journalSegmentMeta
}

// OpenJournal opens or upgrades a session event journal. The persisted cursor
// is a sequence floor; segment files remain the journal authority.
func OpenJournal(path string, retention EventRetentionSpec, cursor EventCursor) (*Journal, error) {
	retention = normalizeSpec(SessionSpec{EventRetention: retention}).EventRetention
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create event journal directory: %w", err)
	}
	dir := journalDirectoryForPath(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create segmented journal directory: %w", err)
	}
	manifestPath := journalManifestPath(path)
	manifest, err := readJournalManifest(manifestPath)
	manifestExists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if !manifestExists {
		manifest = journalManifest{
			Version:              journalFormatVersion,
			SegmentTargetBytes:   normalizedJournalSegmentTarget(retention, 0),
			HeadSeq:              positiveSequence(cursor.Earliest),
			NextSeq:              maxInt64(1, cursor.Latest+1),
			RetainedLogicalBytes: 0,
			ActiveDataEnd:        segmentHeaderSize,
		}
		legacy, inspectErr := inspectLegacyJournal(path, cursor)
		if inspectErr != nil {
			return nil, inspectErr
		}
		manifest.Legacy = legacy
		if legacy != nil {
			manifest.NextSeq = maxInt64(manifest.NextSeq, legacy.LatestSeq+1)
			manifest.RetainedLogicalBytes = -1
		}
	}
	if manifest.SegmentTargetBytes <= 0 {
		manifest.SegmentTargetBytes = normalizedJournalSegmentTarget(retention, 0)
	}

	recovered, active, recoveryChanged, err := recoverJournalLayout(dir, manifest, manifestExists)
	if err != nil {
		return nil, err
	}
	closeActive := true
	defer func() {
		if closeActive && active != nil && active.file != nil {
			_ = active.file.Close()
		}
	}()

	originalActive := manifest.ActiveFile
	originalHead := manifest.HeadSeq
	manifest.Segments = recovered
	manifest.ActiveFile = active.name
	activeLogical := journalRecordLogicalBytes(active.entries)
	latest := maxInt64(cursor.Latest, segmentMetaLatest(recovered), latestRecordSequence(active.entries))
	if manifest.Legacy != nil {
		latest = maxInt64(latest, manifest.Legacy.LatestSeq)
	}
	manifest.NextSeq = maxInt64(1, manifest.NextSeq, latest+1)
	head := maxInt64(positiveSequence(manifest.HeadSeq), positiveSequence(cursor.Earliest))
	if first := firstJournalSequence(manifest.Legacy, recovered, active.entries); first > 0 && head < first {
		head = first
	}
	if head <= 0 {
		head = 1
	}

	retained, retainedKnown := retainedBytesFromManifest(
		manifest,
		originalActive,
		originalHead,
		head,
		activeLogical,
		recoveryChanged,
	)
	if !retainedKnown && manifest.Legacy == nil {
		retained, retainedKnown = retainedBytesWithoutSegmentScan(head, recovered, active.entries)
	}
	journal := &Journal{
		legacyPath:         path,
		dir:                dir,
		manifestPath:       manifestPath,
		manifest:           manifest,
		activeFile:         active.file,
		activeSize:         active.size,
		activeLogicalBytes: activeLogical,
		activeEntries:      active.entries,
		retention:          retention,
		retainedBytes:      retained,
		retainedKnown:      retainedKnown,
		headSeq:            head,
		nextSeq:            manifest.NextSeq,
		subscribers:        map[uint64]*journalSubscriber{},
		pins:               map[string]int{},
		pendingDelete:      map[string]struct{}{},
		dirty:              !manifestExists || recoveryChanged || originalHead != head,
	}
	closeActive = false
	if journal.dirty {
		if err := journal.checkpointLocked(); err != nil {
			_ = journal.activeFile.Close()
			return nil, err
		}
	}
	return journal, nil
}

// recoverJournalLayout keeps the normal resume path constant-sized and expands
// to a directory reconciliation only when the manifest's active tail is sealed
// or missing, which are crash-window states.
func recoverJournalLayout(dir string, manifest journalManifest, requireManifestFiles bool) ([]journalSegmentMeta, *recoveredJournalFile, bool, error) {
	// The normal activation path trusts the atomically published manifest and
	// opens only the bounded active tail. A sealed active file identifies the
	// narrow crash window between sealing a segment and publishing its successor;
	// only that recovery path enumerates segment footers.
	if requireManifestFiles {
		activePath := filepath.Join(dir, manifest.ActiveFile)
		file, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0)
		if err == nil {
			if err := validateJournalSegmentHeader(file); err != nil {
				_ = file.Close()
				return nil, nil, false, fmt.Errorf("validate active journal segment %s: %w", manifest.ActiveFile, err)
			}
			_, sealed, footerErr := readJournalSegmentFooter(file)
			if footerErr != nil {
				_ = file.Close()
				return nil, nil, false, fmt.Errorf("read active journal segment %s footer: %w", manifest.ActiveFile, footerErr)
			}
			if !sealed {
				before, statErr := file.Stat()
				if statErr != nil {
					_ = file.Close()
					return nil, nil, false, fmt.Errorf("stat active journal segment %s: %w", manifest.ActiveFile, statErr)
				}
				entries, size, scanErr := scanActiveJournalSegment(file)
				if scanErr != nil {
					_ = file.Close()
					return nil, nil, false, fmt.Errorf("recover active journal segment %s: %w", manifest.ActiveFile, scanErr)
				}
				return append([]journalSegmentMeta(nil), manifest.Segments...), &recoveredJournalFile{
					name: manifest.ActiveFile, file: file, entries: entries, size: size,
				}, before.Size() != size, nil
			}
			_ = file.Close()
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, false, fmt.Errorf("open active journal segment %s: %w", manifest.ActiveFile, err)
		}
	}

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, false, fmt.Errorf("read segmented journal directory: %w", err)
	}
	files := make([]*recoveredJournalFile, 0)
	seen := make(map[string]struct{})
	for _, entry := range dirEntries {
		if entry.IsDir() || !filepath.IsLocal(entry.Name()) || filepath.Ext(entry.Name()) != ".sjr" {
			continue
		}
		if err := validateJournalFileName(entry.Name()); err != nil {
			return nil, nil, false, err
		}
		file, err := os.OpenFile(filepath.Join(dir, entry.Name()), os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			return nil, nil, false, fmt.Errorf("open journal segment %s: %w", entry.Name(), err)
		}
		item := &recoveredJournalFile{name: entry.Name(), file: file}
		if err := validateJournalSegmentHeader(file); err != nil {
			_ = file.Close()
			return nil, nil, false, fmt.Errorf("validate journal segment %s: %w", entry.Name(), err)
		}
		footer, sealed, footerErr := readJournalSegmentFooter(file)
		if footerErr != nil {
			_ = file.Close()
			return nil, nil, false, fmt.Errorf("read journal segment %s footer: %w", entry.Name(), footerErr)
		}
		if sealed {
			if footer.File != "" && footer.File != entry.Name() {
				_ = file.Close()
				return nil, nil, false, fmt.Errorf("journal segment %s footer names %s", entry.Name(), footer.File)
			}
			footer.File = entry.Name()
			item.sealed = true
			item.meta = footer
			_ = file.Close()
			item.file = nil
		} else {
			entries, size, scanErr := scanActiveJournalSegment(file)
			if scanErr != nil {
				_ = file.Close()
				return nil, nil, false, fmt.Errorf("recover journal segment %s: %w", entry.Name(), scanErr)
			}
			item.entries = entries
			item.size = size
		}
		files = append(files, item)
		seen[entry.Name()] = struct{}{}
	}
	if requireManifestFiles {
		for _, segment := range manifest.Segments {
			if segment.LastSeq < manifest.HeadSeq {
				continue
			}
			if _, ok := seen[segment.File]; !ok {
				closeRecoveredFiles(files)
				return nil, nil, false, fmt.Errorf("journal segment %s referenced by the manifest is missing", segment.File)
			}
		}
		if manifest.ActiveFile != "" {
			if _, ok := seen[manifest.ActiveFile]; !ok {
				closeRecoveredFiles(files)
				return nil, nil, false, fmt.Errorf("active journal segment %s is missing", manifest.ActiveFile)
			}
		}
	}

	changed := false
	unsealed := make([]*recoveredJournalFile, 0)
	segments := make([]journalSegmentMeta, 0)
	for _, item := range files {
		if item.sealed {
			if item.meta.LastSeq >= manifest.HeadSeq || manifest.HeadSeq <= 0 {
				segments = append(segments, item.meta)
			} else {
				if err := os.Remove(filepath.Join(dir, item.name)); err != nil && !errors.Is(err, os.ErrNotExist) {
					closeRecoveredFiles(files)
					return nil, nil, false, fmt.Errorf("remove expired journal segment %s: %w", item.name, err)
				}
				changed = true
			}
			continue
		}
		unsealed = append(unsealed, item)
	}
	sort.Slice(unsealed, func(i, k int) bool {
		iLatest := latestRecordSequence(unsealed[i].entries)
		kLatest := latestRecordSequence(unsealed[k].entries)
		if iLatest == kLatest {
			if unsealed[i].name == manifest.ActiveFile {
				return false
			}
			if unsealed[k].name == manifest.ActiveFile {
				return true
			}
			return unsealed[i].name < unsealed[k].name
		}
		return iLatest < kLatest
	})
	var active *recoveredJournalFile
	if len(unsealed) > 0 {
		active = unsealed[len(unsealed)-1]
	}
	for _, item := range unsealed {
		if item == active {
			continue
		}
		if len(item.entries) > 0 {
			meta := segmentMetaFromEntries(item.name, item.entries, item.size)
			if err := writeJournalSegmentFooter(item.file, meta); err != nil {
				closeRecoveredFiles(files)
				return nil, nil, false, fmt.Errorf("seal recovered journal segment %s: %w", item.name, err)
			}
			segments = append(segments, meta)
		}
		_ = item.file.Close()
		item.file = nil
		changed = true
	}
	if active == nil {
		name, file, createErr := createJournalSegment(dir)
		if createErr != nil {
			closeRecoveredFiles(files)
			return nil, nil, false, createErr
		}
		active = &recoveredJournalFile{name: name, file: file, size: segmentHeaderSize}
		changed = true
	}
	if active.name != manifest.ActiveFile {
		changed = true
	}
	sort.Slice(segments, func(i, k int) bool { return segments[i].FirstSeq < segments[k].FirstSeq })
	previous := int64(0)
	for _, segment := range segments {
		if segment.EventCount <= 0 || segment.FirstSeq <= 0 || segment.LastSeq < segment.FirstSeq {
			closeRecoveredFiles(files)
			return nil, nil, false, fmt.Errorf("journal segment %s has invalid sequence metadata", segment.File)
		}
		if previous > 0 && segment.FirstSeq <= previous {
			closeRecoveredFiles(files)
			return nil, nil, false, fmt.Errorf("journal segment %s overlaps sequence %d", segment.File, previous)
		}
		previous = segment.LastSeq
	}
	if len(active.entries) > 0 && previous > 0 && active.entries[0].seq <= previous {
		closeRecoveredFiles(files)
		return nil, nil, false, fmt.Errorf("active journal segment %s overlaps sealed history", active.name)
	}
	return segments, active, changed || !sameSegmentLayout(manifest.Segments, segments), nil
}

func closeRecoveredFiles(files []*recoveredJournalFile) {
	for _, file := range files {
		if file.file != nil {
			_ = file.file.Close()
		}
	}
}

func sameSegmentLayout(left, right []journalSegmentMeta) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].File != right[i].File || left[i].FirstSeq != right[i].FirstSeq || left[i].LastSeq != right[i].LastSeq || left[i].DataEnd != right[i].DataEnd {
			return false
		}
	}
	return true
}

// inspectLegacyJournal reads only the final JSONL record boundary and sequence.
// It deliberately does not construct the legacy offset index during resume.
func inspectLegacyJournal(path string, cursor EventCursor) (*journalLegacyMeta, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open legacy event journal: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat legacy event journal: %w", err)
	}
	validBytes, lastLineStart, err := legacyValidTail(file, info.Size())
	if err != nil {
		return nil, err
	}
	if validBytes < info.Size() {
		if err := file.Truncate(validBytes); err != nil {
			return nil, fmt.Errorf("truncate incomplete legacy journal tail: %w", err)
		}
		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("sync truncated legacy journal: %w", err)
		}
	}
	if validBytes == 0 {
		return nil, nil
	}
	latest := cursor.Latest
	prefixSize := validBytes - lastLineStart
	if prefixSize > 4096 {
		prefixSize = 4096
	}
	prefix := make([]byte, int(prefixSize))
	if _, err := file.ReadAt(prefix, lastLineStart); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read legacy journal tail: %w", err)
	}
	if seq, seqErr := legacySequenceFromPrefix(prefix); seqErr == nil {
		latest = maxInt64(latest, seq)
	} else if latest <= 0 {
		return nil, seqErr
	}
	earliest := positiveSequence(cursor.Earliest)
	if earliest <= 0 {
		earliest = 1
	}
	return &journalLegacyMeta{
		File:                 filepath.Base(path),
		IndexFile:            journalLegacyIndexName,
		ValidBytes:           validBytes,
		EarliestSeq:          earliest,
		LatestSeq:            latest,
		RetainedLogicalBytes: -1,
	}, nil
}

func legacyValidTail(file *os.File, size int64) (int64, int64, error) {
	if size <= 0 {
		return 0, 0, nil
	}
	last := []byte{0}
	if _, err := file.ReadAt(last, size-1); err != nil {
		return 0, 0, fmt.Errorf("read legacy journal suffix: %w", err)
	}
	valid := size
	if last[0] != '\n' {
		newline, err := findPreviousNewline(file, size)
		if err != nil {
			return 0, 0, err
		}
		valid = newline + 1
	}
	if valid == 0 {
		return 0, 0, nil
	}
	previous, err := findPreviousNewline(file, valid-1)
	if err != nil {
		return 0, 0, err
	}
	return valid, previous + 1, nil
}

func findPreviousNewline(file *os.File, before int64) (int64, error) {
	const blockSize = int64(64 << 10)
	for end := before; end > 0; {
		start := end - blockSize
		if start < 0 {
			start = 0
		}
		buf := make([]byte, int(end-start))
		if _, err := file.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
			return -1, fmt.Errorf("scan legacy journal tail: %w", err)
		}
		if index := bytes.LastIndexByte(buf, '\n'); index >= 0 {
			return start + int64(index), nil
		}
		end = start
	}
	return -1, nil
}

func legacySequenceFromPrefix(prefix []byte) (int64, error) {
	marker := []byte(`"seq":`)
	index := bytes.Index(prefix, marker)
	if index < 0 {
		return 0, errors.New("legacy journal tail does not contain a sequence")
	}
	start := index + len(marker)
	end := start
	for end < len(prefix) && prefix[end] >= '0' && prefix[end] <= '9' {
		end++
	}
	if end == start {
		return 0, errors.New("legacy journal tail sequence is invalid")
	}
	seq, err := strconv.ParseInt(string(prefix[start:end]), 10, 64)
	if err != nil || seq <= 0 {
		return 0, errors.New("legacy journal tail sequence is invalid")
	}
	return seq, nil
}

func retainedBytesFromManifest(manifest journalManifest, originalActive string, originalHead, head, activeLogical int64, recoveryChanged bool) (int64, bool) {
	if recoveryChanged || manifest.RetainedLogicalBytes < 0 || originalHead != head || originalActive == "" || originalActive != manifest.ActiveFile {
		return 0, false
	}
	delta := activeLogical - manifest.ActiveLogicalBytes
	if delta < 0 || manifest.RetainedLogicalBytes+delta < 0 {
		return 0, false
	}
	return manifest.RetainedLogicalBytes + delta, true
}

func retainedBytesWithoutSegmentScan(head int64, segments []journalSegmentMeta, active []journalRecordMeta) (int64, bool) {
	total := int64(0)
	for _, segment := range segments {
		switch {
		case segment.LastSeq < head:
			continue
		case segment.FirstSeq >= head:
			total += segment.LogicalBytes
		default:
			return 0, false
		}
	}
	for _, entry := range active {
		if entry.seq >= head {
			total += entry.logicalSize
		}
	}
	return total, true
}

func (j *Journal) Append(event Event) (Event, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.writableErrorLocked(); err != nil {
		return Event{}, err
	}
	event.Seq = j.nextSeq
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	frame, meta, err := encodeJournalFrame(event)
	if err != nil {
		return Event{}, err
	}
	if len(j.activeEntries) > 0 && j.activeSize+int64(len(frame)) > j.manifest.SegmentTargetBytes {
		if err := j.sealActiveLocked(); err != nil {
			return Event{}, err
		}
	}
	offset := j.activeSize
	written, writeErr := j.activeFile.Write(frame)
	if writeErr != nil || written != len(frame) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if truncateErr := j.activeFile.Truncate(offset); truncateErr != nil {
			j.fatalErr = errors.Join(writeErr, truncateErr)
			return Event{}, fmt.Errorf("append segmented event journal: %w", j.fatalErr)
		}
		return Event{}, fmt.Errorf("append segmented event journal: %w", writeErr)
	}
	meta.offset = offset
	j.activeSize += meta.frameSize
	j.activeLogicalBytes += meta.logicalSize
	j.activeEntries = append(j.activeEntries, meta)
	j.nextSeq++
	j.manifest.NextSeq = j.nextSeq
	if j.retainedKnown {
		j.retainedBytes += meta.logicalSize
	}
	if j.headCacheSource == j.manifest.ActiveFile {
		j.headCache = append(j.headCache, meta)
	}
	j.dirty = true
	if j.retainedKnown {
		changed, trimErr := j.trimLocked(time.Now())
		if trimErr != nil {
			return event, trimErr
		}
		if changed && j.hasExpiredFilesLocked() {
			if err := j.checkpointLocked(); err != nil {
				return event, err
			}
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
	if err := j.ensureRetentionKnown(); err != nil {
		return EventPage{}, err
	}
	j.mu.Lock()
	if err := j.readableErrorLocked(); err != nil {
		j.mu.Unlock()
		return EventPage{}, err
	}
	changed, err := j.trimLocked(time.Now())
	if err != nil {
		j.mu.Unlock()
		return EventPage{}, err
	}
	if changed {
		if err := j.checkpointLocked(); err != nil {
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
	events := make([]Event, 0, min(limit, 1000))
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
	if err := j.ensureRetentionKnown(); err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.readableErrorLocked(); err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	changed, err := j.trimLocked(time.Now())
	if err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	if changed {
		if err := j.checkpointLocked(); err != nil {
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

func (j *Journal) Stats() JournalStats {
	j.mu.Lock()
	defer j.mu.Unlock()
	return JournalStats{
		FormatVersion:      journalFormatVersion,
		SealedSegments:     len(j.manifest.Segments),
		ActiveBytes:        j.activeSize,
		ActiveEvents:       len(j.activeEntries),
		RetainedBytes:      j.retainedBytes,
		RetainedBytesKnown: j.retainedKnown,
		LegacyDeferred:     j.manifest.Legacy != nil && !j.legacyLoaded,
	}
}

func (j *Journal) SetRetention(retention EventRetentionSpec) error {
	retention = normalizeSpec(SessionSpec{EventRetention: retention}).EventRetention
	if err := j.ensureRetentionKnown(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.retention = retention
	changed, err := j.trimLocked(time.Now())
	if err != nil {
		return err
	}
	if !changed {
		return j.cleanupOrphanSegmentsLocked()
	}
	return errors.Join(j.checkpointLocked(), j.cleanupOrphanSegmentsLocked())
}

// Prune applies time- and size-based retention without requiring a new event.
func (j *Journal) Prune(now time.Time) error {
	if err := j.ensureRetentionKnown(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	changed, err := j.trimLocked(now)
	if err != nil {
		return err
	}
	if changed {
		err = j.checkpointLocked()
	}
	return errors.Join(err, j.cleanupOrphanSegmentsLocked())
}

func (j *Journal) Flush() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed || j.activeFile == nil {
		return nil
	}
	if err := j.activeFile.Sync(); err != nil {
		return err
	}
	return j.checkpointLocked()
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
	var result error
	if j.activeFile != nil {
		result = errors.Join(result, j.activeFile.Sync())
		result = errors.Join(result, j.checkpointLocked())
		result = errors.Join(result, j.activeFile.Close())
		j.activeFile = nil
	}
	return result
}

// sealActiveLocked publishes an immutable footer before atomically switching
// the manifest to a freshly synced active segment.
func (j *Journal) sealActiveLocked() error {
	meta := segmentMetaFromEntries(j.manifest.ActiveFile, j.activeEntries, j.activeSize)
	if meta.EventCount == 0 {
		return nil
	}
	if err := writeJournalSegmentFooter(j.activeFile, meta); err != nil {
		j.fatalErr = err
		return err
	}
	if err := j.activeFile.Close(); err != nil {
		j.fatalErr = err
		return err
	}
	name, file, err := createJournalSegment(j.dir)
	if err != nil {
		j.fatalErr = err
		return err
	}
	j.manifest.Segments = append(j.manifest.Segments, meta)
	sort.Slice(j.manifest.Segments, func(i, k int) bool { return j.manifest.Segments[i].FirstSeq < j.manifest.Segments[k].FirstSeq })
	j.manifest.ActiveFile = name
	j.activeFile = file
	j.activeSize = segmentHeaderSize
	j.activeLogicalBytes = 0
	j.activeEntries = nil
	j.headCacheSource = ""
	j.headCache = nil
	j.headCacheIndex = 0
	j.dirty = true
	if err := j.checkpointLocked(); err != nil {
		j.fatalErr = err
		return err
	}
	return nil
}

// checkpointLocked advances the durable logical head before deleting whole
// expired files. A crash can therefore leak an orphan but cannot resurrect it.
func (j *Journal) checkpointLocked() error {
	next := j.manifest
	next.HeadSeq = j.headSeq
	next.NextSeq = j.nextSeq
	next.ActiveFile = j.manifest.ActiveFile
	next.ActiveDataEnd = j.activeSize
	next.ActiveLogicalBytes = j.activeLogicalBytes
	if j.retainedKnown {
		next.RetainedLogicalBytes = j.retainedBytes
	} else {
		next.RetainedLogicalBytes = -1
	}
	kept := make([]journalSegmentMeta, 0, len(j.manifest.Segments))
	deletePaths := make([]string, 0)
	for _, segment := range j.manifest.Segments {
		if segment.LastSeq < j.headSeq {
			deletePaths = append(deletePaths, filepath.Join(j.dir, segment.File))
			continue
		}
		kept = append(kept, segment)
	}
	next.Segments = kept
	if next.Legacy != nil && next.Legacy.LatestSeq < j.headSeq {
		deletePaths = append(deletePaths, j.legacyPath)
		if next.Legacy.IndexFile != "" {
			deletePaths = append(deletePaths, filepath.Join(j.dir, next.Legacy.IndexFile))
		}
		next.Legacy = nil
	}
	if err := writeJournalManifest(j.manifestPath, &next); err != nil {
		return err
	}
	j.manifest = next
	if next.Legacy == nil {
		j.legacyEntries = nil
	}
	j.dirty = false
	var deleteErr error
	for _, path := range deletePaths {
		deleteErr = errors.Join(deleteErr, j.removeOrDeferLocked(path))
	}
	return deleteErr
}

func (j *Journal) openBacklogLocked(after int64, limit int) (*EventBacklog, error) {
	backlog := &EventBacklog{after: after, head: j.headSeq, remaining: -1}
	if limit > 0 {
		backlog.remaining = limit
	}
	pinned := make([]string, 0)
	addSource := func(source journalBacklogSource) error {
		file, err := os.Open(source.path)
		if err != nil {
			return err
		}
		source.file = file
		j.pins[source.path]++
		pinned = append(pinned, source.path)
		backlog.sources = append(backlog.sources, source)
		return nil
	}
	rollback := func() {
		for i := range backlog.sources {
			_ = backlog.sources[i].file.Close()
		}
		for _, path := range pinned {
			j.pins[path]--
			if j.pins[path] == 0 {
				delete(j.pins, path)
			}
		}
	}
	if j.manifest.Legacy != nil && j.legacyLoaded && j.manifest.Legacy.LatestSeq > after && j.manifest.Legacy.LatestSeq >= j.headSeq {
		start := sort.Search(len(j.legacyEntries), func(i int) bool { return j.legacyEntries[i].seq > after && j.legacyEntries[i].seq >= j.headSeq })
		if start < len(j.legacyEntries) {
			entries := append([]journalEntry(nil), j.legacyEntries[start:]...)
			if err := addSource(journalBacklogSource{format: journalBacklogLegacy, path: j.legacyPath, entries: entries}); err != nil {
				rollback()
				return nil, fmt.Errorf("open legacy journal snapshot: %w", err)
			}
		}
	}
	for _, segment := range j.manifest.Segments {
		if segment.LastSeq <= after || segment.LastSeq < j.headSeq {
			continue
		}
		startSeq := maxInt64(after+1, j.headSeq)
		offset := journalSegmentStartOffset(segment, startSeq)
		path := filepath.Join(j.dir, segment.File)
		if err := addSource(journalBacklogSource{format: journalBacklogSegment, path: path, offset: offset, end: segment.DataEnd}); err != nil {
			rollback()
			return nil, fmt.Errorf("open journal segment snapshot: %w", err)
		}
	}
	if len(j.activeEntries) > 0 && j.activeEntries[len(j.activeEntries)-1].seq > after && j.activeEntries[len(j.activeEntries)-1].seq >= j.headSeq {
		path := filepath.Join(j.dir, j.manifest.ActiveFile)
		if err := addSource(journalBacklogSource{format: journalBacklogSegment, path: path, offset: segmentHeaderSize, end: j.activeSize}); err != nil {
			rollback()
			return nil, fmt.Errorf("open active journal snapshot: %w", err)
		}
	}
	backlog.release = func() { j.releasePins(pinned) }
	return backlog, nil
}

func (j *Journal) releasePins(paths []string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, path := range paths {
		j.pins[path]--
		if j.pins[path] > 0 {
			continue
		}
		delete(j.pins, path)
		if _, pending := j.pendingDelete[path]; pending {
			_ = os.Remove(path)
			delete(j.pendingDelete, path)
		}
	}
}

func (j *Journal) removeOrDeferLocked(path string) error {
	if j.pins[path] > 0 {
		j.pendingDelete[path] = struct{}{}
		return nil
	}
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (j *Journal) cleanupOrphanSegmentsLocked() error {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return fmt.Errorf("read journal directory for orphan cleanup: %w", err)
	}
	live := make(map[string]struct{}, len(j.manifest.Segments)+1)
	live[j.manifest.ActiveFile] = struct{}{}
	for _, segment := range j.manifest.Segments {
		live[segment.File] = struct{}{}
	}
	var cleanupErr error
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".sjr" {
			continue
		}
		if _, ok := live[name]; ok {
			continue
		}
		if err := validateJournalFileName(name); err != nil {
			continue
		}
		path := filepath.Join(j.dir, name)
		cleanupErr = errors.Join(cleanupErr, j.removeOrDeferLocked(path))
	}
	return cleanupErr
}

func readLegacyJournalEvent(file *os.File, entry journalEntry) (Event, error) {
	if file == nil {
		return Event{}, errors.New("legacy event journal snapshot is closed")
	}
	if entry.size <= 0 || entry.size > int64(maxInt()) {
		return Event{}, fmt.Errorf("invalid legacy event journal record size %d", entry.size)
	}
	line := make([]byte, int(entry.size))
	if _, err := file.ReadAt(line, entry.offset); err != nil {
		return Event{}, fmt.Errorf("read legacy event journal record: %w", err)
	}
	var event Event
	if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
		return Event{}, fmt.Errorf("decode legacy event journal record: %w", err)
	}
	if event.Seq != entry.seq {
		return Event{}, fmt.Errorf("legacy event journal sequence %d does not match index %d", event.Seq, entry.seq)
	}
	return event, nil
}

// ensureRetentionKnown performs deferred legacy indexing and, at most, scans
// frame headers in the one sealed segment containing the retained head.
func (j *Journal) ensureRetentionKnown() error {
	if err := j.ensureLegacyLoaded(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.retainedKnown || j.closed {
		return nil
	}
	total, err := j.computeRetainedBytesLocked()
	if err != nil {
		return err
	}
	j.retainedBytes = total
	j.retainedKnown = true
	j.dirty = true
	return nil
}

func (j *Journal) ensureLegacyLoaded() error {
	j.mu.Lock()
	hasLegacy := j.manifest.Legacy != nil
	j.mu.Unlock()
	if !hasLegacy {
		return nil
	}
	j.legacyOnce.Do(func() {
		j.mu.Lock()
		legacy := *j.manifest.Legacy
		j.mu.Unlock()
		indexPath := filepath.Join(j.dir, legacy.IndexFile)
		entries, err := readLegacyJournalIndex(indexPath, legacy.ValidBytes)
		if err != nil {
			entries, _, err = loadLegacyJournal(j.legacyPath, legacy.ValidBytes)
			if err == nil {
				err = writeLegacyJournalIndex(indexPath, legacy.ValidBytes, entries)
			}
		}
		if err != nil {
			j.legacyLoadErr = err
			return
		}
		j.mu.Lock()
		j.legacyEntries = entries
		j.legacyLoaded = true
		if len(entries) > 0 && j.manifest.Legacy != nil {
			updated := *j.manifest.Legacy
			updated.EarliestSeq = entries[0].seq
			updated.LatestSeq = entries[len(entries)-1].seq
			var retained int64
			for _, entry := range entries {
				if entry.seq >= j.headSeq {
					retained += entry.size
				}
			}
			updated.RetainedLogicalBytes = retained
			j.manifest.Legacy = &updated
		}
		j.mu.Unlock()
	})
	return j.legacyLoadErr
}

func loadLegacyJournal(path string, validLimit int64) ([]journalEntry, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open legacy event journal for replay: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReaderSize(io.LimitReader(file, validLimit), 64*1024)
	var entries []journalEntry
	var previous int64
	var validBytes int64
	for validBytes < validLimit {
		offset := validBytes
		line, readErr := reader.ReadBytes('\n')
		complete := bytes.HasSuffix(line, []byte{'\n'})
		if len(bytes.TrimSpace(line)) > 0 {
			if readErr == io.EOF && !complete {
				break
			}
			var event Event
			if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
				return nil, 0, fmt.Errorf("decode legacy event journal: %w", err)
			}
			if event.Seq <= previous {
				return nil, 0, fmt.Errorf("legacy event journal sequence %d is not greater than %d", event.Seq, previous)
			}
			previous = event.Seq
			entries = append(entries, journalEntry{seq: event.Seq, offset: offset, size: int64(len(line)), occurredAtNano: event.OccurredAt.UnixNano()})
		}
		if complete {
			validBytes += int64(len(line))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, 0, fmt.Errorf("read legacy event journal: %w", readErr)
		}
	}
	if validBytes != validLimit {
		return nil, validBytes, fmt.Errorf("legacy event journal index ended at %d, want %d", validBytes, validLimit)
	}
	return entries, validBytes, nil
}

func (j *Journal) computeRetainedBytesLocked() (int64, error) {
	total := int64(0)
	if j.manifest.Legacy != nil {
		if !j.legacyLoaded {
			return 0, errors.New("legacy event journal is not indexed")
		}
		for _, entry := range j.legacyEntries {
			if entry.seq >= j.headSeq {
				total += entry.size
			}
		}
	}
	for _, segment := range j.manifest.Segments {
		switch {
		case segment.LastSeq < j.headSeq:
			continue
		case segment.FirstSeq >= j.headSeq:
			total += segment.LogicalBytes
		default:
			file, err := os.Open(filepath.Join(j.dir, segment.File))
			if err != nil {
				return 0, err
			}
			entries, scanErr := scanJournalSegmentMetadata(file, journalSegmentStartOffset(segment, j.headSeq), segment.DataEnd)
			closeErr := file.Close()
			if scanErr != nil || closeErr != nil {
				return 0, errors.Join(scanErr, closeErr)
			}
			for _, entry := range entries {
				if entry.seq >= j.headSeq {
					total += entry.logicalSize
				}
			}
		}
	}
	for _, entry := range j.activeEntries {
		if entry.seq >= j.headSeq {
			total += entry.logicalSize
		}
	}
	return total, nil
}

func (j *Journal) trimLocked(now time.Time) (bool, error) {
	if !j.retainedKnown {
		return false, nil
	}
	changed := false
	if j.retention.MaxAgeSeconds > 0 {
		cutoff := now.Add(-time.Duration(j.retention.MaxAgeSeconds) * time.Second).UnixNano()
		for {
			entry, ok, err := j.peekHeadLocked()
			if err != nil {
				return changed, err
			}
			if !ok || entry.occurredAtNano >= cutoff {
				break
			}
			j.advanceHeadLocked(entry)
			changed = true
		}
	}
	if j.retention.MaxBytes > 0 {
		for j.retainedBytes > j.retention.MaxBytes {
			entry, ok, err := j.peekHeadLocked()
			if err != nil {
				return changed, err
			}
			if !ok {
				j.retainedBytes = 0
				break
			}
			j.advanceHeadLocked(entry)
			changed = true
		}
	}
	if changed {
		j.dirty = true
	}
	return changed, nil
}

func (j *Journal) peekHeadLocked() (journalRecordMeta, bool, error) {
	for {
		if j.headCacheIndex < len(j.headCache) {
			entry := j.headCache[j.headCacheIndex]
			if entry.seq >= j.headSeq {
				if entry.seq > j.headSeq {
					j.headSeq = entry.seq
				}
				return entry, true, nil
			}
			j.headCacheIndex++
			continue
		}
		j.headCacheSource = ""
		j.headCache = nil
		j.headCacheIndex = 0
		if j.manifest.Legacy != nil && j.legacyLoaded && j.headSeq <= j.manifest.Legacy.LatestSeq {
			start := sort.Search(len(j.legacyEntries), func(i int) bool { return j.legacyEntries[i].seq >= j.headSeq })
			if start < len(j.legacyEntries) {
				j.headCacheSource = "legacy"
				j.headCache = make([]journalRecordMeta, 0, len(j.legacyEntries)-start)
				for _, entry := range j.legacyEntries[start:] {
					j.headCache = append(j.headCache, journalRecordMeta{seq: entry.seq, offset: entry.offset, frameSize: entry.size, payloadSize: entry.size, logicalSize: entry.size, occurredAtNano: entry.occurredAtNano})
				}
				continue
			}
		}
		loaded := false
		for _, segment := range j.manifest.Segments {
			if segment.LastSeq < j.headSeq {
				continue
			}
			file, err := os.Open(filepath.Join(j.dir, segment.File))
			if err != nil {
				return journalRecordMeta{}, false, err
			}
			entries, scanErr := scanJournalSegmentMetadata(file, journalSegmentStartOffset(segment, j.headSeq), segment.DataEnd)
			closeErr := file.Close()
			if scanErr != nil || closeErr != nil {
				return journalRecordMeta{}, false, errors.Join(scanErr, closeErr)
			}
			start := sort.Search(len(entries), func(i int) bool { return entries[i].seq >= j.headSeq })
			j.headCacheSource = segment.File
			j.headCache = entries[start:]
			loaded = true
			break
		}
		if loaded {
			continue
		}
		start := sort.Search(len(j.activeEntries), func(i int) bool { return j.activeEntries[i].seq >= j.headSeq })
		if start < len(j.activeEntries) {
			j.headCacheSource = j.manifest.ActiveFile
			j.headCache = append([]journalRecordMeta(nil), j.activeEntries[start:]...)
			continue
		}
		j.headSeq = j.nextSeq
		return journalRecordMeta{}, false, nil
	}
}

func (j *Journal) advanceHeadLocked(entry journalRecordMeta) {
	j.retainedBytes -= entry.logicalSize
	if j.retainedBytes < 0 {
		j.retainedBytes = 0
	}
	j.headSeq = entry.seq + 1
	j.headCacheIndex++
}

func (j *Journal) hasExpiredFilesLocked() bool {
	for _, segment := range j.manifest.Segments {
		if segment.LastSeq < j.headSeq {
			return true
		}
	}
	return j.manifest.Legacy != nil && j.manifest.Legacy.LatestSeq < j.headSeq
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
	if latest <= 0 {
		return EventCursor{}
	}
	earliest := j.headSeq
	if earliest <= 0 {
		earliest = 1
	}
	if earliest > latest {
		earliest = j.nextSeq
	}
	return EventCursor{Earliest: earliest, Latest: latest}
}

func (j *Journal) readableErrorLocked() error {
	if j.closed {
		return errors.New("event journal is closed")
	}
	return nil
}

func (j *Journal) writableErrorLocked() error {
	if err := j.readableErrorLocked(); err != nil {
		return err
	}
	if j.fatalErr != nil {
		return fmt.Errorf("event journal requires recovery: %w", j.fatalErr)
	}
	return nil
}

func journalRecordLogicalBytes(entries []journalRecordMeta) int64 {
	var total int64
	for _, entry := range entries {
		total += entry.logicalSize
	}
	return total
}

func latestRecordSequence(entries []journalRecordMeta) int64 {
	if len(entries) == 0 {
		return 0
	}
	return entries[len(entries)-1].seq
}

func firstJournalSequence(legacy *journalLegacyMeta, segments []journalSegmentMeta, active []journalRecordMeta) int64 {
	first := int64(0)
	if legacy != nil && legacy.EarliestSeq > 0 {
		first = legacy.EarliestSeq
	}
	for _, segment := range segments {
		if segment.FirstSeq > 0 && (first == 0 || segment.FirstSeq < first) {
			first = segment.FirstSeq
		}
	}
	if len(active) > 0 && (first == 0 || active[0].seq < first) {
		first = active[0].seq
	}
	return first
}

func positiveSequence(value int64) int64 {
	if value > 0 {
		return value
	}
	return 0
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
