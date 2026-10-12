package session

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func testReceipt(index int) InputReceipt {
	return InputReceipt{
		InputID: fmt.Sprintf("input-%d", index), AttemptID: "attempt-one",
		Digest: fmt.Sprintf("digest-%d", index), AcceptedAt: time.Unix(1_700_000_000, int64(index)).UTC(),
		Sequence: int64(index + 1),
	}
}

func TestInputReceiptsRecoverWithoutRewritingSessionState(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	record := Session{ID: "ses-inputs", Attempt: &Attempt{ID: "attempt-one"}, InputReceipts: []InputReceipt{testReceipt(1)}}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, record.ID, sessionStateFile)
	before, _ := os.ReadFile(path)
	if err := store.saveInputReceipt(record.ID, testReceipt(2)); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("input rewrote the full session state")
	}
	// A new store has no in-memory journal/dedup cache.
	reopened, _ := NewFileStore(root)
	loaded, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || !reflect.DeepEqual(loaded[0].InputReceipts, []InputReceipt{testReceipt(1), testReceipt(2)}) {
		t.Fatalf("loaded receipts = %#v", loaded)
	}
	if !loaded[0].LastActivityAt.Equal(testReceipt(2).AcceptedAt) {
		t.Fatal("lost input activity time")
	}
	// A lifecycle checkpoint can overlap receipts already in the sidecar.
	if err := reopened.Save(loaded[0]); err != nil {
		t.Fatal(err)
	}
	loaded, err = reopened.Load()
	if err != nil || len(loaded[0].InputReceipts) != 2 {
		t.Fatalf("duplicated checkpoint receipts: %#v, %v", loaded, err)
	}
}

func TestInputReceiptJournalRepairsOnlyIncompleteTail(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	record := Session{ID: "ses-tail"}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.saveInputReceipt(record.ID, testReceipt(1)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.root, record.ID, inputReceiptsFile)
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = file.WriteString(`{"input_id":"interrupted`)
	_ = file.Close()
	reopened, _ := NewFileStore(store.root)
	if _, err := reopened.Load(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.saveInputReceipt(record.ID, testReceipt(2)); err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Load()
	if err != nil || len(loaded[0].InputReceipts) != 2 {
		t.Fatalf("tail repair: %#v, %v", loaded, err)
	}
	file, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = file.WriteString("corrupt-complete-record\n")
	_ = file.Close()
	if _, err := reopened.Load(); err == nil {
		t.Fatal("complete corrupt receipt silently ignored")
	}
}

func TestInputReceiptJournalCompactsAndRetainsNewestDedupWindow(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	if err := store.Save(Session{ID: "ses-bounded"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2*maxInputReceipts+10; i++ {
		if err := store.saveInputReceipt("ses-bounded", testReceipt(i)); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	receipts := loaded[0].InputReceipts
	if len(receipts) != maxInputReceipts || receipts[0].InputID != testReceipt(maxInputReceipts+11).InputID ||
		receipts[len(receipts)-1].InputID != testReceipt(2*maxInputReceipts+10).InputID {
		t.Fatalf("wrong retained window: %d receipts", len(receipts))
	}
	data, _ := os.ReadFile(filepath.Join(store.root, "ses-bounded", inputReceiptsFile))
	if len(data) > 400_000 {
		t.Fatalf("receipt journal grew to %d bytes", len(data))
	}
}

func TestInputReceiptsLoadLegacyStateAndRejectConflictingIDs(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	dir := filepath.Join(store.root, "ses-legacy")
	_ = os.MkdirAll(dir, 0o700)
	data, _ := json.Marshal(persistedSession{FormatVersion: 1, Session: Session{ID: "ses-legacy"}, InputReceipts: []InputReceipt{testReceipt(1)}})
	_ = os.WriteFile(filepath.Join(dir, sessionStateFile), data, 0o600)
	if err := store.saveInputReceipt("ses-legacy", testReceipt(2)); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded[0].InputReceipts) != 2 {
		t.Fatalf("legacy receipt recovery: %#v, %v", loaded, err)
	}
	conflict := testReceipt(1)
	conflict.Digest = "different"
	if err := store.saveInputReceipt("ses-legacy", conflict); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("conflicting input IDs silently accepted")
	}
}

func TestInputReceiptIDsCanBeReusedAfterTheDedupWindowExpires(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	if err := store.Save(Session{ID: "ses-reused"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxInputReceipts; i++ {
		if err := store.saveInputReceipt("ses-reused", testReceipt(i)); err != nil {
			t.Fatal(err)
		}
	}
	reused := testReceipt(maxInputReceipts + 1)
	reused.InputID = testReceipt(0).InputID
	if err := store.saveInputReceipt("ses-reused", reused); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	last := loaded[0].InputReceipts[maxInputReceipts-1]
	if last != reused {
		t.Fatalf("reused ID not retained: %#v", last)
	}
}

func TestInputReceiptRecoveryPreservesOrderAcrossWallClockChanges(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	if err := store.Save(Session{ID: "ses-clock"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		receipt := testReceipt(i)
		receipt.AcceptedAt = receipt.AcceptedAt.Add(-time.Duration(i) * time.Hour)
		if err := store.saveInputReceipt("ses-clock", receipt); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	for index, receipt := range loaded[0].InputReceipts {
		if receipt.InputID != testReceipt(index+1).InputID {
			t.Fatalf("clock adjustment reordered receipts: %#v", loaded[0].InputReceipts)
		}
	}
}

func TestInputPersistenceDoesNotHoldPTYOutputLock(t *testing.T) {
	s := newTestSupervisor(t)
	value, _, err := s.Create(SessionSpec{Command: []string{"/bin/cat"}, IO: IOSpec{Mode: IOModePipes}}, "")
	if err != nil {
		t.Fatal(err)
	}
	value, _ = s.Get(value.ID)
	j := s.store.inputJournal(value.ID)
	j.mu.Lock()
	released := false
	defer func() {
		if !released {
			j.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := s.WriteInput(value.ID, InputRequest{InputID: "slow-disk", ExpectedAttemptID: value.Attempt.ID,
			DataBase64: base64.StdEncoding.EncodeToString([]byte("echo-now\n"))})
		done <- err
	}()
	output := make(chan bool, 1)
	go func() {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			page, _ := s.Events(value.ID, 0, 100)
			for _, event := range page.Events {
				if event.Type == "output" {
					output <- true
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
		output <- false
	}()
	select {
	case seen := <-output:
		if !seen {
			t.Fatal("PTY output stalled behind receipt persistence")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PTY output lock held during receipt persistence")
	}
	select {
	case <-done:
		t.Fatal("input acknowledged before durable persistence")
	default:
	}
	j.mu.Unlock()
	released = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInputPersistenceRetryNeverResendsAcceptedBytes(t *testing.T) {
	s := newTestSupervisor(t)
	value, _, err := s.Create(SessionSpec{Command: []string{"/bin/cat"}, IO: IOSpec{Mode: IOModePipes}}, "")
	if err != nil {
		t.Fatal(err)
	}
	value, _ = s.Get(value.ID)
	path := filepath.Join(s.store.root, value.ID, inputReceiptsFile)
	_ = os.Mkdir(path, 0o700) // Force a receipt I/O failure after PTY acceptance.
	request := InputRequest{InputID: "retry-after-io-error", ExpectedAttemptID: value.Attempt.ID,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("once\n"))}
	if _, err := s.WriteInput(value.ID, request); err == nil {
		t.Fatal("acknowledged failed persistence")
	}
	_ = os.Remove(path)
	response, err := s.WriteInput(value.ID, request)
	if err != nil || !response.Duplicate {
		t.Fatalf("retry = %#v, %v", response, err)
	}
	if _, err := s.WriteInput(value.ID, InputRequest{InputID: "eof", ExpectedAttemptID: value.Attempt.ID, EOF: true}); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, s, value.ID, PhaseExited)
	page, _ := s.Events(value.ID, 0, 1000)
	var text string
	for _, event := range page.Events {
		if event.Stream == "stdout" {
			data, _ := base64.StdEncoding.DecodeString(event.DataBase64)
			text += string(data)
		}
	}
	if text != "once\n" {
		t.Fatalf("retry duplicated native input: %q", text)
	}
}

func BenchmarkInputPersistence(b *testing.B) {
	for _, incremental := range []bool{false, true} {
		name := "full-state-1024"
		if incremental {
			name = "incremental-receipt"
		}
		b.Run(name, func(b *testing.B) {
			store, _ := NewFileStore(b.TempDir())
			record := Session{ID: "ses-bench"}
			for i := range maxInputReceipts {
				record.InputReceipts = append(record.InputReceipts, testReceipt(i))
			}
			if err := store.Save(record); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if incremental {
					err = store.saveInputReceipt(record.ID, testReceipt(i+maxInputReceipts))
				} else {
					err = store.Save(record)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
