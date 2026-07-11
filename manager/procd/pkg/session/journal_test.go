package session

import (
	"errors"
	"os"
	"path/filepath"
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
	if len(backlog) != 0 {
		t.Fatalf("backlog length = %d, want 0", len(backlog))
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
