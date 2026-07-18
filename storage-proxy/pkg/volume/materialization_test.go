package volume

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

type recordingStorageObserver struct {
	volumeID  string
	teamID    string
	sizeBytes int64
	calls     int
	err       error
}

func (o *recordingStorageObserver) ObserveVolumeState(
	_ context.Context,
	volumeID, teamID string,
	state *s0fs.SnapshotState,
	_ time.Time,
) error {
	o.volumeID = volumeID
	o.teamID = teamID
	o.sizeBytes = s0fs.StateStorageBytes(state)
	o.calls++
	return o.err
}

func TestVolumeContextSyncMaterializeObservesStorageState(t *testing.T) {
	volCtx := newMaterializationTestVolume(t)
	observer := &recordingStorageObserver{}
	volCtx.Observer = observer
	writeMaterializationTestFile(t, volCtx.S0FS, "first.txt", "hello world")

	result, err := volCtx.SyncMaterialize(context.Background())
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if result.Manifest == nil {
		t.Fatal("SyncMaterialize() returned no manifest")
	}
	if result.ObservationError != nil {
		t.Fatalf("SyncMaterialize() observation error = %v", result.ObservationError)
	}
	if observer.calls != 1 {
		t.Fatalf("observer calls = %d, want 1", observer.calls)
	}
	if observer.volumeID != "vol-1" || observer.teamID != "team-1" {
		t.Fatalf("observer identity = %q/%q, want vol-1/team-1", observer.volumeID, observer.teamID)
	}
	if observer.sizeBytes != int64(len("hello world")) {
		t.Fatalf("observer size = %d, want %d", observer.sizeBytes, len("hello world"))
	}

	cleanResult, err := volCtx.SyncMaterialize(context.Background())
	if err != nil {
		t.Fatalf("second SyncMaterialize() error = %v", err)
	}
	if cleanResult.Manifest != nil {
		t.Fatal("second SyncMaterialize() unexpectedly returned a manifest")
	}
	if observer.calls != 1 {
		t.Fatalf("observer calls after clean sync = %d, want 1", observer.calls)
	}
}

func TestVolumeContextSyncMaterializeSeparatesObservationFailure(t *testing.T) {
	volCtx := newMaterializationTestVolume(t)
	observationErr := errors.New("metering unavailable")
	volCtx.Observer = &recordingStorageObserver{err: observationErr}
	writeMaterializationTestFile(t, volCtx.S0FS, "first.txt", "payload")

	result, err := volCtx.SyncMaterialize(context.Background())
	if err != nil {
		t.Fatalf("SyncMaterialize() durable error = %v", err)
	}
	if result.Manifest == nil {
		t.Fatal("SyncMaterialize() returned no manifest")
	}
	if !errors.Is(result.ObservationError, observationErr) {
		t.Fatalf("SyncMaterialize() observation error = %v, want %v", result.ObservationError, observationErr)
	}
}

func TestVolumeContextCompactObservesInitialMaterialization(t *testing.T) {
	volCtx := newMaterializationTestVolume(t)
	observer := &recordingStorageObserver{}
	volCtx.Observer = observer
	writeMaterializationTestFile(t, volCtx.S0FS, "first.txt", "payload")

	result, _, err := volCtx.Compact(context.Background(), s0fs.CompactionOptions{})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if result.Manifest == nil {
		t.Fatal("Compact() returned no materialized manifest")
	}
	if result.ObservationError != nil {
		t.Fatalf("Compact() observation error = %v", result.ObservationError)
	}
	if observer.calls != 1 {
		t.Fatalf("observer calls = %d, want 1", observer.calls)
	}
}

func newMaterializationTestVolume(t *testing.T) *VolumeContext {
	t.Helper()
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:    "vol-1",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: objectstore.NewMemoryStore(t.Name()),
	})
	if err != nil {
		t.Fatalf("s0fs.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return &VolumeContext{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		Backend:  BackendS0FS,
		S0FS:     engine,
	}
}

func writeMaterializationTestFile(t *testing.T, engine *s0fs.Engine, name, contents string) {
	t.Helper()
	node, err := engine.CreateFile(s0fs.RootInode, name, 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(contents)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
}
