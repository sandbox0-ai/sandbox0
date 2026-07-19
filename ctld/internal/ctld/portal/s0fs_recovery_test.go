package portal

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestPersistProcessLocalS0FSHandleStateDropsLegacyDirectoryHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	want := volume.HandleState{
		NextHandleID:  3,
		FileHandles:   map[uint64]uint64{2: 20},
		UnlinkedFiles: []uint64{20},
	}
	legacy := want
	legacy.DirHandles = map[uint64]uint64{3: 30}
	if err := persistProcessLocalS0FSHandleState(path, "vol-1", legacy); err != nil {
		t.Fatalf("persistProcessLocalS0FSHandleState() error = %v", err)
	}

	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadS0FSHandleState() = %#v, want %#v", got, want)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.Contains(string(payload), "dir_handles") {
		t.Fatalf("persisted recovery state retained directory handles: %s", payload)
	}
}

func TestLoadS0FSHandleStateReplaysJournalAndClearsFinalUnlinkedHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	initial := volume.HandleState{
		NextHandleID:  2,
		FileHandles:   map[uint64]uint64{1: 10, 2: 10},
		UnlinkedFiles: []uint64{10},
	}
	if err := persistS0FSHandleState(path, "vol-1", initial); err != nil {
		t.Fatalf("persistS0FSHandleState() error = %v", err)
	}
	journal, err := openS0FSHandleStateJournal(path)
	if err != nil {
		t.Fatalf("openS0FSHandleStateJournal() error = %v", err)
	}
	for _, event := range []s0fsHandleJournalEvent{
		{Version: 1, VolumeID: "vol-1", Operation: "close", HandleID: 1},
		{Version: 1, VolumeID: "vol-1", Operation: "open", HandleID: 3, Inode: 20},
		{Version: 1, VolumeID: "vol-1", Operation: "close", HandleID: 2},
	} {
		if err := journal.Append(event); err != nil {
			t.Fatalf("Append(%+v) error = %v", event, err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	want := volume.HandleState{
		NextHandleID: 3,
		FileHandles:  map[uint64]uint64{3: 20},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadS0FSHandleState() = %#v, want %#v", got, want)
	}
}

func TestLoadS0FSHandleStateIgnoresPartialTrailingJournalEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	if err := persistS0FSHandleState(path, "vol-1", volume.HandleState{}); err != nil {
		t.Fatalf("persistS0FSHandleState() error = %v", err)
	}
	payload := strings.Join([]string{
		`{"version":1,"volume_id":"vol-1","operation":"open","handle_id":1,"inode":10}`,
		`{"version":1,"volume_id":"vol-1","operation":"open"`,
	}, "\n")
	if err := os.WriteFile(s0fsHandleJournalPath(path), []byte(payload), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if got.NextHandleID != 1 || !reflect.DeepEqual(got.FileHandles, map[uint64]uint64{1: 10}) {
		t.Fatalf("loadS0FSHandleState() = %#v, want first complete event only", got)
	}
}

func TestCompactS0FSHandleStateResetsJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	journal, err := openS0FSHandleStateJournal(path)
	if err != nil {
		t.Fatalf("openS0FSHandleStateJournal() error = %v", err)
	}
	if err := journal.Append(s0fsHandleJournalEvent{
		Version: 1, VolumeID: "vol-1", Operation: "open", HandleID: 1, Inode: 10,
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	want := volume.HandleState{NextHandleID: 2, FileHandles: map[uint64]uint64{2: 20}}
	if err := compactS0FSHandleState(path, "vol-1", want, false, journal); err != nil {
		t.Fatalf("compactS0FSHandleState() error = %v", err)
	}
	if journal.events != 0 {
		t.Fatalf("journal event count = %d, want 0", journal.events)
	}
	info, err := os.Stat(s0fsHandleJournalPath(path))
	if err != nil {
		t.Fatalf("Stat(journal) error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("journal size = %d, want 0", info.Size())
	}
	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadS0FSHandleState() = %#v, want %#v", got, want)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLoadS0FSHandleStateReplaysPreCompactionJournalIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles.json")
	final := volume.HandleState{
		NextHandleID: 2,
		FileHandles:  map[uint64]uint64{2: 20},
	}
	if err := persistS0FSHandleState(path, "vol-1", final); err != nil {
		t.Fatalf("persistS0FSHandleState() error = %v", err)
	}
	journal, err := openS0FSHandleStateJournal(path)
	if err != nil {
		t.Fatalf("openS0FSHandleStateJournal() error = %v", err)
	}
	for _, event := range []s0fsHandleJournalEvent{
		{Version: 1, VolumeID: "vol-1", Operation: "open", HandleID: 1, Inode: 10},
		{Version: 1, VolumeID: "vol-1", Operation: "close", HandleID: 1},
		{Version: 1, VolumeID: "vol-1", Operation: "open", HandleID: 2, Inode: 20},
	} {
		if err := journal.Append(event); err != nil {
			t.Fatalf("Append(%+v) error = %v", event, err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, err := loadS0FSHandleState(path, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if !reflect.DeepEqual(got, final) {
		t.Fatalf("loadS0FSHandleState() = %#v, want %#v", got, final)
	}
}

func BenchmarkPersistS0FSHandleState(b *testing.B) {
	for _, count := range []int{64, 1024, 10_000} {
		handles := benchmarkS0FSHandleState(count)
		b.Run("Snapshot/"+strconv.Itoa(count), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "handles.json")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := persistProcessLocalS0FSHandleState(path, "vol-1", handles); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("JournalAppend", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "handles.json")
		journal, err := openS0FSHandleStateJournal(path)
		if err != nil {
			b.Fatal(err)
		}
		defer journal.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			handleID := uint64(index + 1)
			if err := journal.Append(s0fsHandleJournalEvent{
				Version: 1, VolumeID: "vol-1", Operation: "open", HandleID: handleID, Inode: handleID + 1000,
			}); err != nil {
				b.Fatal(err)
			}
			if journal.ShouldCompact() {
				if err := journal.Reset(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func benchmarkS0FSHandleState(count int) volume.HandleState {
	handles := volume.HandleState{
		NextHandleID: uint64(count),
		FileHandles:  make(map[uint64]uint64, count),
	}
	for handle := 1; handle <= count; handle++ {
		handles.FileHandles[uint64(handle)] = uint64(1000 + handle)
	}
	return handles
}
