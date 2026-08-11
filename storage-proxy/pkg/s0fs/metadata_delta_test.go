package s0fs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

func TestMetadataDeltaPublicationIsSmallAndReconstructsState(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-metadata-delta"
	dir := t.TempDir()
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "writer.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	var changed uint64
	for index := 0; index < 1000; index++ {
		node, err := engine.CreateFile(RootInode, fmt.Sprintf("file-%04d", index), 0o644)
		if err != nil {
			t.Fatalf("CreateFile(%d) error = %v", index, err)
		}
		if index == 500 {
			changed = node.Inode
		}
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(base) error = %v", err)
	}
	if first.Delta != nil {
		t.Fatal("initial publication unexpectedly used a delta")
	}
	baseBytes := lastManifestPutSize(t, store.putCalls())

	store.resetCalls()
	if err := engine.SetMode(changed, 0o600); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}
	if err := engine.SetXattr(changed, "user.s0fs-test", []byte("delta"), XattrCreate); err != nil {
		t.Fatalf("SetXattr() error = %v", err)
	}
	metadataBeforeDelta := engine.metadata
	second, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(delta) error = %v", err)
	}
	if second.Delta == nil || second.DeltaDepth != 1 {
		t.Fatalf("second manifest delta = %#v depth = %d, want depth-1 delta", second.Delta, second.DeltaDepth)
	}
	deltaBytes := lastManifestPutSize(t, store.putCalls())
	t.Logf("full checkpoint = %d bytes, delta publication = %d bytes", baseBytes, deltaBytes)
	if deltaBytes*5 >= baseBytes {
		t.Fatalf("delta publication = %d bytes, base = %d bytes; want delta below 20%% of base", deltaBytes, baseBytes)
	}
	if engine.metadata != metadataBeforeDelta {
		t.Fatal("delta materialization rebuilt the complete metadata store")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	store.resetCalls()
	readerDir := t.TempDir()
	reopened, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(readerDir, "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("Open(remote delta) error = %v", err)
	}
	defer reopened.Close()
	attr, err := reopened.GetAttr(changed)
	if err != nil || attr.Mode != 0o600 {
		t.Fatalf("GetAttr(changed) = %+v, %v; want mode 0600", attr, err)
	}
	value, err := reopened.GetXattr(changed, "user.s0fs-test")
	if err != nil || string(value) != "delta" {
		t.Fatalf("GetXattr() = %q, %v; want delta", value, err)
	}
	if _, err := reopened.Lookup(RootInode, "file-0999"); err != nil {
		t.Fatalf("Lookup(unchanged base entry) error = %v", err)
	}
	if gets := store.calls(); len(gets) > 3 {
		t.Fatalf("cold open object GETs = %d (%+v), want bounded delta head reads", len(gets), gets)
	} else {
		t.Logf("cold open object GETs = %d", len(gets))
	}
}

func TestLazyOpenSkipsSegmentInventoryAndScrubFindsFailure(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-lazy-segment-inventory"
	dir := t.TempDir()
	store := &failingSegmentHeadStore{Store: objectstore.NewMemoryStore(t.Name())}
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "writer.wal"), ObjectStore: store, HeadStore: heads,
		SegmentTargetSize: 8,
	})
	if err != nil {
		t.Fatalf("Open(writer) error = %v", err)
	}
	for index := 0; index < 64; index++ {
		node, err := engine.CreateFile(RootInode, fmt.Sprintf("segment-%02d", index), 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Write(node.Inode, 0, []byte("12345678")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	store.setFailure(errTransientSegmentHead)
	readerDir := t.TempDir()
	reopened, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(readerDir, "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("lazy Open() unexpectedly validated segment inventory: %v", err)
	}
	defer reopened.Close()
	if err := reopened.ScrubSegments(ctx); !errors.Is(err, errTransientSegmentHead) {
		t.Fatalf("ScrubSegments() error = %v, want provider failure", err)
	}
}

func TestMetadataDeltaChainPublishesBoundedCheckpoint(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-bounded-metadata-delta"
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(t.TempDir(), "writer.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatal(err)
	}
	node, err := engine.CreateFile(RootInode, "file", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatal(err)
	}
	for generation := uint32(1); generation <= maxManifestDeltaDepth; generation++ {
		if err := engine.SetMode(node.Inode, 0o600+generation%2); err != nil {
			t.Fatal(err)
		}
		manifest, err := engine.SyncMaterialize(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Delta == nil || manifest.DeltaDepth != generation {
			t.Fatalf("generation %d delta = %#v depth = %d", generation, manifest.Delta, manifest.DeltaDepth)
		}
	}
	if err := engine.SetMode(node.Inode, 0o640); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Delta != nil || checkpoint.DeltaDepth != 0 {
		t.Fatalf("bounded checkpoint retained delta depth %d", checkpoint.DeltaDepth)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	store.resetCalls()
	reopened, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(t.TempDir(), "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if gets := store.calls(); len(gets) > 2 {
		t.Fatalf("checkpoint cold open object GETs = %d (%+v), want constant", len(gets), gets)
	}
}

func lastManifestPutSize(t *testing.T, calls []putCall) int {
	t.Helper()
	for index := len(calls) - 1; index >= 0; index-- {
		if strings.HasPrefix(calls[index].key, manifestDir+"/") {
			return calls[index].size
		}
	}
	t.Fatalf("put calls = %+v, want manifest publication", calls)
	return 0
}
