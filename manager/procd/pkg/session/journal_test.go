package session

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestJournalCursorRetentionAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	journal, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 350, MaxAgeSeconds: 3600}, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: "dGVzdC1ldmVudA=="}); err != nil {
			t.Fatal(err)
		}
	}
	cursor := journal.Cursor()
	if cursor.Latest != 8 || cursor.Earliest <= 1 {
		t.Fatalf("cursor = %#v, want retained history ending at 8", cursor)
	}
	if _, err := journal.Read(1, 100); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("Read() error = %v, want cursor expired", err)
	}
	backlog, live, cancel, _, err := journal.Subscribe(cursor.Latest)
	if err != nil {
		t.Fatal(err)
	}
	defer backlog.Close()
	if _, ok, err := backlog.Next(); err != nil || ok {
		t.Fatalf("backlog.Next() = (_, %t, %v), want empty backlog", ok, err)
	}
	appended, err := journal.Append(Event{SessionID: "ses-test", Type: "session.ready"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-live:
		if got.Seq != appended.Seq {
			t.Fatalf("live seq = %d, want %d", got.Seq, appended.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live event")
	}
	cancel()
	if _, ok := <-live; ok {
		t.Fatal("subscription remained open after cancellation")
	}
	closeCursor := journal.Cursor()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 350, MaxAgeSeconds: 3600}, closeCursor)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := reopened.Append(Event{SessionID: "ses-test", Type: "session.updated"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != appended.Seq+1 {
		t.Fatalf("reopened seq = %d, want %d", next.Seq, appended.Seq+1)
	}
}

func TestJournalCursorWhenRetentionRemovesEveryEvent(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 1, MaxAgeSeconds: 3600},
		EventCursor{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := journal.Append(Event{SessionID: "ses-test", Type: "session.created"}); err != nil {
		t.Fatal(err)
	}
	cursor := journal.Cursor()
	if cursor.Earliest != 2 || cursor.Latest != 1 {
		t.Fatalf("cursor = %#v, want empty retained window [2, 1]", cursor)
	}
	if _, err := journal.Read(0, 100); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("Read(0) error = %v, want cursor expired", err)
	}
	page, err := journal.Read(1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("Read(1) returned %d events, want 0", len(page.Events))
	}
	if _, err := journal.Read(2, 100); err == nil {
		t.Fatal("Read(2) succeeded with a cursor newer than the journal")
	}
}

func TestJournalReadPrunesEventsThatExpiredWhileIdle(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 1},
		EventCursor{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := journal.Append(Event{SessionID: "ses-test", Type: "session.created", OccurredAt: time.Now().Add(-2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Read(0, 100); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("Read(0) error = %v, want cursor expired after idle age pruning", err)
	}
	if cursor := journal.Cursor(); cursor.Earliest != 2 || cursor.Latest != 1 {
		t.Fatalf("cursor = %#v, want empty retained window [2, 1]", cursor)
	}
}

func TestJournalReopenTruncatesIncompleteActiveTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Append(Event{SessionID: "ses-test", Type: "session.created"}); err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(journal.dir, journal.manifest.ActiveFile)
	cursor := journal.Cursor()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0x20, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, retention, cursor)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reopened.Append(Event{SessionID: "ses-test", Type: "session.updated"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Seq != 2 {
		t.Fatalf("second sequence = %d, want 2", second.Seq)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	verified, err := OpenJournal(path, retention, EventCursor{Earliest: 1, Latest: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	page, err := verified.Read(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[1].Seq != 2 {
		t.Fatalf("events = %#v, want two complete records", page.Events)
	}
}

func TestJournalReopenDropsCorruptFinalFrameWithoutReusingSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	activePath := filepath.Join(journal.dir, journal.manifest.ActiveFile)
	corruptOffset := journal.activeEntries[1].offset + segmentFrameHeaderSize
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte{0}
	if _, err := file.ReadAt(value, corruptOffset); err != nil {
		t.Fatal(err)
	}
	value[0] ^= 0xff
	if _, err := file.WriteAt(value, corruptOffset); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, retention, EventCursor{Earliest: 1, Latest: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := reopened.Append(Event{SessionID: "ses-test", Type: "session.ready"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != 3 {
		t.Fatalf("sequence after corrupt tail = %d, want 3", next.Seq)
	}
	page, err := reopened.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[0].Seq != 1 || page.Events[1].Seq != 3 {
		t.Fatalf("events after corrupt tail recovery = %#v, want sequences 1 and 3", page.Events)
	}
}

func TestJournalRetainedPayloadDoesNotStayInHeap(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 32 << 20, MaxAgeSeconds: 3600},
		EventCursor{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := journal.Append(Event{SessionID: "ses-test", Type: "session.created"}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	const (
		eventCount = 64
		eventBytes = 256 << 10
	)
	for i := 0; i < eventCount; i++ {
		payload := strings.Repeat(string(rune('a'+i%26)), eventBytes)
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: payload}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 4<<20 {
		t.Fatalf("journal retained %d heap bytes after storing %d bytes of event payload", retained, eventCount*eventBytes)
	}
	stats := journal.Stats()
	if stats.SealedSegments == 0 {
		t.Fatal("journal did not seal any segments")
	}
	indexedEvents := int64(stats.ActiveEvents)
	for _, segment := range journal.manifest.Segments {
		indexedEvents += segment.EventCount
	}
	if indexedEvents != eventCount+1 {
		t.Fatalf("indexed event count = %d, want %d", indexedEvents, eventCount+1)
	}
}

func TestJournalSubscribeStreamsBacklogWithBoundedLiveBuffer(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600},
		EventCursor{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const count = defaultSubscriptionBacklog + 100
	for i := 0; i < count; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: "dGVzdA=="}); err != nil {
			t.Fatal(err)
		}
	}
	backlog, live, cancel, _, err := journal.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer backlog.Close()
	defer cancel()
	if cap(live) != defaultSubscriptionBacklog {
		t.Fatalf("live buffer capacity = %d, want %d", cap(live), defaultSubscriptionBacklog)
	}
	for i := 1; i <= count; i++ {
		event, ok, err := backlog.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok || event.Seq != int64(i) {
			t.Fatalf("backlog event %d = (%#v, %t), want seq %d", i, event, ok, i)
		}
	}
	if _, ok, err := backlog.Next(); err != nil || ok {
		t.Fatalf("backlog.Next() = (_, %t, %v), want end of backlog", ok, err)
	}
}

func TestJournalBacklogSnapshotSurvivesSegmentDeletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	journal, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	journal.manifest.SegmentTargetBytes = minimumJournalSegmentBytes
	for i := 0; i < 12; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: strings.Repeat("x", 12<<10)}); err != nil {
			t.Fatal(err)
		}
	}
	if len(journal.manifest.Segments) == 0 {
		t.Fatal("test requires sealed segments")
	}
	firstSegmentPath := filepath.Join(journal.dir, journal.manifest.Segments[0].File)
	backlog, _, cancel, _, err := journal.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer backlog.Close()
	defer cancel()
	if err := journal.SetRetention(EventRetentionSpec{MaxBytes: 1, MaxAgeSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(firstSegmentPath); err != nil {
		t.Fatalf("pinned segment was removed before backlog close: %v", err)
	}
	for i := 1; i <= 12; i++ {
		event, ok, err := backlog.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok || event.Seq != int64(i) {
			t.Fatalf("backlog event %d = (%#v, %t), want seq %d", i, event, ok, i)
		}
	}
	if err := backlog.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(firstSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment stat error = %v, want removal after backlog close", err)
	}
}

func TestJournalSegmentRetentionKeepsPhysicalFilesBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	const maxBytes = int64(32 << 10)
	journal, err := OpenJournal(path, EventRetentionSpec{MaxBytes: maxBytes, MaxAgeSeconds: 3600}, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for i := 0; i < 300; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: strings.Repeat("x", 1024)}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(journal.dir)
	if err != nil {
		t.Fatal(err)
	}
	var physical int64
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".sjr" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		physical += info.Size()
	}
	if physical > 3*minimumJournalSegmentBytes {
		t.Fatalf("journal segment bytes = %d, want at most %d", physical, 3*minimumJournalSegmentBytes)
	}
	if journal.retainedBytes > maxBytes {
		t.Fatalf("retained bytes = %d, want at most %d", journal.retainedBytes, maxBytes)
	}
}

func TestJournalLegacyMigrationIsLazyAndPreservesCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	writeLegacyEvents(t, path, 1, 128, 4096)
	retention := EventRetentionSpec{MaxBytes: 4 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, EventCursor{Earliest: 1, Latest: 128})
	if err != nil {
		t.Fatal(err)
	}
	if !journal.Stats().LegacyDeferred {
		t.Fatal("legacy journal was indexed during activation")
	}
	if _, err := os.Stat(filepath.Join(journal.dir, journalLegacyIndexName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy index stat error = %v, want deferred index", err)
	}
	appended, err := journal.Append(Event{SessionID: "ses-test", Type: "session.updated"})
	if err != nil {
		t.Fatal(err)
	}
	if appended.Seq != 129 {
		t.Fatalf("appended sequence = %d, want 129", appended.Seq)
	}
	page, err := journal.Read(126, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 3 || page.Events[0].Seq != 127 || page.Events[2].Seq != 129 {
		t.Fatalf("migration page = %#v, want sequences 127..129", page.Events)
	}
	if journal.Stats().LegacyDeferred {
		t.Fatal("legacy journal remained deferred after history read")
	}
	if _, err := os.Stat(filepath.Join(journal.dir, journalLegacyIndexName)); err != nil {
		t.Fatalf("legacy index was not persisted: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, retention, EventCursor{Earliest: 1, Latest: 129})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.Stats().LegacyDeferred {
		t.Fatal("reopen eagerly loaded the persisted legacy index")
	}
	next, err := reopened.Append(Event{SessionID: "ses-test", Type: "session.ready"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != 130 {
		t.Fatalf("reopened sequence = %d, want 130", next.Seq)
	}
	reopenedPage, err := reopened.Read(128, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopenedPage.Events) != 2 || reopenedPage.Events[0].Seq != 129 || reopenedPage.Events[1].Seq != 130 {
		t.Fatalf("indexed legacy reopen page = %#v, want sequences 129 and 130", reopenedPage.Events)
	}
}

func TestJournalPruneIndexesAndExpiresLegacyHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	writeLegacyEvents(t, path, 1, 20, 256)
	journal, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 1}, EventCursor{Earliest: 1, Latest: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if !journal.Stats().LegacyDeferred {
		t.Fatal("legacy journal was indexed during activation")
	}
	if err := journal.Prune(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if cursor := journal.Cursor(); cursor.Earliest != 21 || cursor.Latest != 20 {
		t.Fatalf("cursor after legacy prune = %#v, want empty retained window [21, 20]", cursor)
	}
	if journal.manifest.Legacy != nil {
		t.Fatal("expired legacy journal remained in the manifest")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy journal stat error = %v, want removal", err)
	}
}

func TestJournalRecoversSealedActiveBeforeManifestUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	activeName := journal.manifest.ActiveFile
	activePath := filepath.Join(journal.dir, activeName)
	entries := append([]journalRecordMeta(nil), journal.activeEntries...)
	dataEnd := journal.activeSize
	cursor := journal.Cursor()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJournalSegmentFooter(file, segmentMetaFromEntries(activeName, entries, dataEnd)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenJournal(path, retention, cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if len(recovered.manifest.Segments) != 1 || recovered.manifest.Segments[0].File != activeName {
		t.Fatalf("recovered segments = %#v, want sealed former active", recovered.manifest.Segments)
	}
	if recovered.manifest.ActiveFile == activeName {
		t.Fatal("recovery did not create a new active segment")
	}
	next, err := recovered.Append(Event{SessionID: "ses-test", Type: "session.ready"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != 5 {
		t.Fatalf("recovered append sequence = %d, want 5", next.Seq)
	}
	page, err := recovered.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 5 {
		t.Fatalf("recovered event count = %d, want 5", len(page.Events))
	}
}

func TestJournalReopenTruncatesPartialSegmentFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, EventCursor{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	activePath := filepath.Join(journal.dir, journal.manifest.ActiveFile)
	dataEnd := journal.activeSize
	cursor := journal.Cursor()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	partialFooter := append(append([]byte(nil), segmentFooterMagic[:]...), []byte(`{"file":`)...)
	if _, err := file.Write(partialFooter); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, retention, cursor)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.activeSize != dataEnd {
		t.Fatalf("recovered active size = %d, want %d", reopened.activeSize, dataEnd)
	}
	next, err := reopened.Append(Event{SessionID: "ses-test", Type: "session.ready"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != 5 {
		t.Fatalf("sequence after partial footer = %d, want 5", next.Seq)
	}
}

func writeLegacyEvents(t *testing.T, path string, first, count, payloadBytes int) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		event := Event{Seq: int64(first + i), SessionID: "ses-test", Type: "output", DataBase64: strings.Repeat("x", payloadBytes), OccurredAt: time.Now().UTC()}
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		line = append(line, '\n')
		if _, err := file.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
