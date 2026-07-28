package session

import (
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
	journal, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 350, MaxAgeSeconds: 3600}, 0)
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
		t.Fatalf("cursor = %#v, want compacted history ending at 8", cursor)
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
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, EventRetentionSpec{MaxBytes: 350, MaxAgeSeconds: 3600}, cursor.Latest)
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
		0,
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
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := journal.Append(Event{
		SessionID:  "ses-test",
		Type:       "session.created",
		OccurredAt: time.Now().Add(-2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Read(0, 100); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("Read(0) error = %v, want cursor expired after idle age pruning", err)
	}
	if cursor := journal.Cursor(); cursor.Earliest != 2 || cursor.Latest != 1 {
		t.Fatalf("cursor = %#v, want empty retained window [2, 1]", cursor)
	}
}

func TestJournalReopenTruncatesIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600}
	journal, err := OpenJournal(path, retention, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Append(Event{SessionID: "ses-test", Type: "session.created"}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"seq":2`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path, retention, 1)
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

	verified, err := OpenJournal(path, retention, 2)
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

func TestJournalRetainedPayloadDoesNotStayInHeap(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 32 << 20, MaxAgeSeconds: 3600},
		0,
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
		if _, err := journal.Append(Event{
			SessionID:  "ses-test",
			Type:       "output",
			DataBase64: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 4<<20 {
		t.Fatalf("journal retained %d heap bytes after storing %d bytes of event payload", retained, eventCount*eventBytes)
	}
	if len(journal.entries) != eventCount+1 {
		t.Fatalf("entry count = %d, want %d", len(journal.entries), eventCount+1)
	}
}

func TestJournalSubscribeStreamsBacklogWithBoundedLiveBuffer(t *testing.T) {
	journal, err := OpenJournal(
		filepath.Join(t.TempDir(), "events.jsonl"),
		EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600},
		0,
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
		if !ok {
			t.Fatalf("backlog ended at event %d, want %d events", i-1, count)
		}
		if event.Seq != int64(i) {
			t.Fatalf("event seq = %d, want %d", event.Seq, i)
		}
	}
	if _, ok, err := backlog.Next(); err != nil || ok {
		t.Fatalf("backlog.Next() = (_, %t, %v), want end of backlog", ok, err)
	}
}

func TestJournalBacklogSnapshotSurvivesCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	journal, err := OpenJournal(
		path,
		EventRetentionSpec{MaxBytes: 1 << 20, MaxAgeSeconds: 3600},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	for i := 0; i < 10; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-test", Type: "output", DataBase64: strings.Repeat("eA==", 128)}); err != nil {
			t.Fatal(err)
		}
	}
	backlog, _, cancel, _, err := journal.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer backlog.Close()
	defer cancel()

	if err := journal.SetRetention(EventRetentionSpec{MaxBytes: 1, MaxAgeSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		event, ok, err := backlog.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok || event.Seq != int64(i) {
			t.Fatalf("backlog event %d = (%#v, %t), want seq %d", i, event, ok, i)
		}
	}
}

func TestJournalCompactionKeepsFileBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	const maxBytes = int64(32 << 10)
	journal, err := OpenJournal(
		path,
		EventRetentionSpec{MaxBytes: maxBytes, MaxAgeSeconds: 3600},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	for i := 0; i < 300; i++ {
		if _, err := journal.Append(Event{
			SessionID:  "ses-test",
			Type:       "output",
			DataBase64: strings.Repeat("eA==", 256),
		}); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > maxBytes*2 {
		t.Fatalf("journal file size = %d, want at most %d", info.Size(), maxBytes*2)
	}
	if journal.totalBytes > maxBytes {
		t.Fatalf("retained bytes = %d, want at most %d", journal.totalBytes, maxBytes)
	}
}
