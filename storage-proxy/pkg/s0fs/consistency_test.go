package s0fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

func TestCommittedHeadCASRequiresExactIdentity(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHeadStore()
	current := &CommittedHead{
		VolumeID: "vol-exact-cas", ManifestSeq: 2, CheckpointSeq: 2,
		ManifestKey: "manifests/00000000000000000002-a.json", ManifestDigest: "digest-a",
		CommitID: "a", Generation: 1,
	}
	if err := store.CompareAndSwapCommittedHead(ctx, current.VolumeID, nil, current); err != nil {
		t.Fatalf("CompareAndSwapCommittedHead(insert) error = %v", err)
	}
	next := &CommittedHead{
		VolumeID: current.VolumeID, ManifestSeq: 4, CheckpointSeq: 4,
		ManifestKey: "manifests/00000000000000000004-b.json", ManifestDigest: "digest-b",
		CommitID: "b", Generation: 2,
	}
	for name, mutate := range map[string]func(*CommittedHead){
		"manifest key":    func(head *CommittedHead) { head.ManifestKey += ".stale" },
		"manifest digest": func(head *CommittedHead) { head.ManifestDigest += "-stale" },
		"commit id":       func(head *CommittedHead) { head.CommitID += "-stale" },
		"generation":      func(head *CommittedHead) { head.Generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			expected := cloneCommittedHead(current)
			mutate(expected)
			if err := store.CompareAndSwapCommittedHead(ctx, current.VolumeID, expected, next); !errors.Is(err, ErrCommittedHeadConflict) {
				t.Fatalf("CompareAndSwapCommittedHead() error = %v, want ErrCommittedHeadConflict", err)
			}
		})
	}
	loaded, err := store.LoadCommittedHead(ctx, current.VolumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if !sameCommittedHeadIdentity(loaded, current) {
		t.Fatalf("committed head changed after rejected CAS: got %+v want %+v", loaded, current)
	}
}

func TestOpenUsesManifestWhenLocalCheckpointDigestDiverges(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-manifest-authority"
	dir := t.TempDir()
	walPath := filepath.Join(dir, "engine.wal")
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()

	engine, err := Open(ctx, Config{VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "authority.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("manifest")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	divergent := cloneState(manifest.State)
	divergent.Data[node.Inode] = []byte("local-only")
	delete(divergent.ColdFiles, node.Inode)
	divergent.Nodes[node.Inode].Size = uint64(len(divergent.Data[node.Inode]))
	if err := saveSnapshotState(headStatePath(walPath), volumeID, "head", divergent, nil, StateFormatV1); err != nil {
		t.Fatalf("saveSnapshotState(divergent) error = %v", err)
	}

	reopened, err := Open(ctx, Config{VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads})
	if err != nil {
		t.Fatalf("Open(reconciled) error = %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Read(node.Inode, 0, 32)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(got) != "manifest" {
		t.Fatalf("Read() = %q, want committed manifest bytes", got)
	}
}

func TestMissingCommittedSegmentPoisonsEngineWithoutRequestStorm(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-terminal-integrity"
	dir := t.TempDir()
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, node, manifest := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(dir, "writer.wal"), store, heads, "committed")
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}

	reader, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("Open(reader) error = %v", err)
	}
	defer reader.Close()
	segment := onlyManifestSegment(t, manifest)
	if err := store.Delete(segment.Key); err != nil {
		t.Fatalf("Delete(segment) error = %v", err)
	}
	store.resetCalls()

	if _, err := reader.Read(node.Inode, 0, 32); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Read() error = %v, want ErrCommittedStateIntegrity", err)
	}
	requestsAfterFailure := len(store.calls()) + len(store.putCalls())
	if requestsAfterFailure == 0 {
		t.Fatal("integrity failure did not attempt the referenced segment read")
	}
	if _, err := reader.SyncMaterialize(ctx); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("SyncMaterialize(after failure) error = %v, want terminal integrity error", err)
	}
	if _, _, err := reader.Compact(ctx, CompactionOptions{Force: true}); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Compact(after failure) error = %v, want terminal integrity error", err)
	}
	if _, err := reader.Read(node.Inode, 0, 32); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Read(after failure) error = %v, want terminal integrity error", err)
	}
	if got := len(store.calls()) + len(store.putCalls()); got != requestsAfterFailure {
		t.Fatalf("provider requests after terminal failure = %d, want %d", got, requestsAfterFailure)
	}
}

func TestCorruptCommittedSegmentPoisonsEngine(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-corrupt-integrity"
	dir := t.TempDir()
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, node, manifest := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(dir, "writer.wal"), store, heads, "committed")
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}
	reader, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("Open(reader) error = %v", err)
	}
	defer reader.Close()
	segment := onlyManifestSegment(t, manifest)
	if err := store.Put(segment.Key, bytes.NewReader([]byte("corrupted"))); err != nil {
		t.Fatalf("Put(corrupt segment) error = %v", err)
	}
	if _, err := reader.Read(node.Inode, 0, 32); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Read() error = %v, want ErrCommittedStateIntegrity", err)
	}
	if _, err := reader.Lookup(RootInode, "authority.txt"); !errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Lookup(after failure) error = %v, want terminal integrity error", err)
	}
}

func TestTransientSegmentValidationFailureIsNotIntegrityFailure(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-transient-segment-head"
	dir := t.TempDir()
	store := &failingSegmentHeadStore{Store: objectstore.NewMemoryStore(t.Name())}
	heads := newMemoryHeadStore()
	engine, _, _ := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(dir, "writer.wal"), store, heads, "committed")
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}
	store.setFailure(errTransientSegmentHead)
	_, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "reader.wal"), ObjectStore: store, HeadStore: heads,
	})
	if !errors.Is(err, errTransientSegmentHead) || errors.Is(err, ErrCommittedStateIntegrity) {
		t.Fatalf("Open() error = %v, want transient provider error without integrity classification", err)
	}
}

func TestRepairCommittedStateAdvancesBrokenHead(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-explicit-repair"
	dir := t.TempDir()
	store := newPrefixedRecordingStore(t, volumeID)
	heads := newMemoryHeadStore()
	engine, node, broken := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(dir, "writer.wal"), store, heads, "broken")
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}
	if err := store.Delete(onlyManifestSegment(t, broken).Key); err != nil {
		t.Fatalf("Delete(broken segment) error = %v", err)
	}

	now := time.Now().UTC()
	repairState := &SnapshotState{
		NextSeq: broken.ManifestSeq + 2, NextInode: node.Inode + 1,
		Nodes: map[uint64]*Node{
			RootInode:  {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 2, Atime: now, Mtime: now, Ctime: now},
			node.Inode: {Inode: node.Inode, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 8, Atime: now, Mtime: now, Ctime: now},
		},
		Children:  map[uint64]map[string]uint64{RootInode: {"authority.txt": node.Inode}},
		Data:      map[uint64][]byte{node.Inode: []byte("repaired")},
		ColdFiles: map[uint64][]FileExtent{}, Segments: map[string]*Segment{},
	}
	repaired, err := RepairCommittedState(ctx, Config{
		VolumeID: volumeID, ObjectStore: store, HeadStore: heads, StateFormatVersion: StateFormatV2,
	}, repairState)
	if err != nil {
		t.Fatalf("RepairCommittedState() error = %v", err)
	}
	if repaired.ManifestSeq <= broken.ManifestSeq || repaired.ParentCommitID != broken.CommitID {
		t.Fatalf("repaired manifest = %+v, want an exact child of broken head", repaired)
	}

	reopened, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(dir, "repaired.wal"), ObjectStore: store, HeadStore: heads,
	})
	if err != nil {
		t.Fatalf("Open(repaired) error = %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Read(node.Inode, 0, 32)
	if err != nil {
		t.Fatalf("Read(repaired) error = %v", err)
	}
	if string(got) != "repaired" {
		t.Fatalf("Read(repaired) = %q, want repaired", got)
	}
}

func TestRefreshMaterializedNeverBindsOldStateToNewHead(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-refresh-exact-head"
	store := newPrefixedRecordingStore(t, volumeID)
	underlyingHeads := newMemoryHeadStore()
	readerHeads := &blockingLoadHeadStore{memoryHeadStore: underlyingHeads}

	reader, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(t.TempDir(), "reader.wal"), ObjectStore: store, HeadStore: readerHeads,
	})
	if err != nil {
		t.Fatalf("Open(reader) error = %v", err)
	}
	defer reader.Close()

	writerOne, firstNode, _ := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(t.TempDir(), "writer-one.wal"), store, underlyingHeads, "first")
	if err := writerOne.Close(); err != nil {
		t.Fatalf("Close(writer one) error = %v", err)
	}
	writerTwo, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: filepath.Join(t.TempDir(), "writer-two.wal"), ObjectStore: store, HeadStore: underlyingHeads,
	})
	if err != nil {
		t.Fatalf("Open(writer two) error = %v", err)
	}
	secondNode, err := writerTwo.CreateFile(RootInode, "second.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(second) error = %v", err)
	}
	if _, err := writerTwo.Write(secondNode.Inode, 0, []byte("second")); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}

	entered, release := readerHeads.blockLoad(2)
	refreshDone := make(chan struct {
		refreshed bool
		err       error
	}, 1)
	go func() {
		refreshed, err := reader.RefreshMaterialized(ctx)
		refreshDone <- struct {
			refreshed bool
			err       error
		}{refreshed: refreshed, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("RefreshMaterialized() did not reach exact-head recheck")
	}
	if _, err := writerTwo.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(writer two) error = %v", err)
	}
	close(release)
	result := <-refreshDone
	if result.err != nil || result.refreshed {
		t.Fatalf("RefreshMaterialized(raced) = %v, %v; want false, nil", result.refreshed, result.err)
	}
	if _, err := reader.Lookup(RootInode, "authority.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("raced refresh installed stale state: Lookup() error = %v", err)
	}

	refreshed, err := reader.RefreshMaterialized(ctx)
	if err != nil || !refreshed {
		t.Fatalf("RefreshMaterialized(retry) = %v, %v; want true, nil", refreshed, err)
	}
	for name, item := range map[string]struct {
		inode uint64
		want  string
	}{
		"first":  {inode: firstNode.Inode, want: "first"},
		"second": {inode: secondNode.Inode, want: "second"},
	} {
		got, err := reader.Read(item.inode, 0, 32)
		if err != nil || string(got) != item.want {
			t.Fatalf("Read(%s) = %q, %v; want %q", name, got, err, item.want)
		}
	}
	if err := writerTwo.Close(); err != nil {
		t.Fatalf("Close(writer two) error = %v", err)
	}
}

func TestCompactionDiscardsPreparedCandidateAfterConcurrentMutation(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-compaction-superseded"
	dir := t.TempDir()
	store := &blockingPutStore{Store: objectstore.NewMemoryStore(t.Name())}
	heads := newMemoryHeadStore()
	engine, firstNode, _ := materializeConsistencyFixture(t, ctx, volumeID, filepath.Join(dir, "engine.wal"), store, heads, "first")
	defer engine.Close()

	entered, release := store.blockNext(segmentDir + "/")
	compactDone := make(chan struct {
		manifest *Manifest
		result   *CompactionResult
		err      error
	}, 1)
	go func() {
		manifest, result, err := engine.Compact(ctx, CompactionOptions{Force: true})
		compactDone <- struct {
			manifest *Manifest
			result   *CompactionResult
			err      error
		}{manifest: manifest, result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Compact() did not reach candidate upload")
	}
	secondNode, err := engine.CreateFile(RootInode, "second.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(concurrent) error = %v", err)
	}
	if _, err := engine.Write(secondNode.Inode, 0, []byte("second")); err != nil {
		t.Fatalf("Write(concurrent) error = %v", err)
	}
	close(release)
	result := <-compactDone
	if result.err != nil || result.manifest != nil || result.result != nil {
		t.Fatalf("Compact(superseded) = %+v, %+v, %v; want nil candidate without error", result.manifest, result.result, result.err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(after superseded compaction) error = %v", err)
	}
	for name, item := range map[string]struct {
		inode uint64
		want  string
	}{
		"first":  {inode: firstNode.Inode, want: "first"},
		"second": {inode: secondNode.Inode, want: "second"},
	} {
		got, err := engine.Read(item.inode, 0, 32)
		if err != nil || string(got) != item.want {
			t.Fatalf("Read(%s) = %q, %v; want %q", name, got, err, item.want)
		}
	}
}

func TestCompactionLocalInstallFailureReopensFromCommittedManifest(t *testing.T) {
	ctx := context.Background()
	const volumeID = "vol-compaction-local-failure"
	dir := t.TempDir()
	walPath := filepath.Join(dir, "engine.wal")
	guard := &LocalDiskGuard{Path: dir, MaxBytes: 1 << 30}
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads, LocalDiskGuard: guard,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "durable.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("durable")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	before, err := heads.LoadCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead(before compaction) error = %v", err)
	}

	guard.MaxBytes = 1
	if _, _, err := engine.Compact(ctx, CompactionOptions{Force: true}); !errors.Is(err, ErrNoSpace) || !errors.Is(err, ErrCommittedHeadConflict) {
		t.Fatalf("Compact() error = %v, want local space failure after committed-head advance", err)
	}
	after, err := heads.LoadCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead(after compaction) error = %v", err)
	}
	if after.Generation != before.Generation+1 || after.ManifestKey == before.ManifestKey {
		t.Fatalf("committed head did not advance before local install failure: before=%+v after=%+v", before, after)
	}
	if _, err := engine.Read(node.Inode, 0, 32); !errors.Is(err, ErrCommittedHeadConflict) {
		t.Fatalf("Read(after terminal install failure) error = %v, want ErrCommittedHeadConflict", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(failed engine) error = %v", err)
	}

	guard.MaxBytes = 1 << 30
	reopened, err := Open(ctx, Config{
		VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads, LocalDiskGuard: guard,
	})
	if err != nil {
		t.Fatalf("Open(authoritative manifest) error = %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Read(node.Inode, 0, 32)
	if err != nil || string(got) != "durable" {
		t.Fatalf("Read(authoritative manifest) = %q, %v; want durable", got, err)
	}
}

func TestGarbageCollectionRevalidatesBeforeEveryDelete(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	for _, key := range []string{"segments/one.bin", "segments/two.bin"} {
		if err := store.Put(key, bytes.NewReader([]byte(key))); err != nil {
			t.Fatalf("Put(%s) error = %v", key, err)
		}
	}
	checks := 0
	plan := &GarbageCollectionPlan{
		store: store, Segments: []string{"segments/one.bin", "segments/two.bin"},
		deleteGuard: func(context.Context) error {
			checks++
			if checks == 2 {
				return ErrCommittedHeadConflict
			}
			return nil
		},
	}
	result, err := plan.Apply(ctx)
	if !errors.Is(err, ErrCommittedHeadConflict) {
		t.Fatalf("Apply() error = %v, want ErrCommittedHeadConflict", err)
	}
	if checks != 2 || len(result.DeletedSegments) != 1 || result.DeletedSegments[0] != "segments/one.bin" {
		t.Fatalf("guard checks/deletions = %d/%v, want 2/[segments/one.bin]", checks, result.DeletedSegments)
	}
	if _, err := store.Head("segments/two.bin"); err != nil {
		t.Fatalf("second segment was deleted after guard failure: %v", err)
	}
}

func TestInterruptedObjectPublicationReopensFromWALAndPreservesBytes(t *testing.T) {
	tests := []struct {
		name       string
		format     int
		encryption *EncryptionConfig
		failPrefix string
	}{
		{name: "v1 plain segment upload", format: StateFormatV1, failPrefix: segmentDir + "/"},
		{name: "v1 encrypted manifest upload", format: StateFormatV1, encryption: testEncryptionConfig(8), failPrefix: manifestDir + "/"},
		{name: "v2 plain manifest upload", format: StateFormatV2, failPrefix: manifestDir + "/"},
		{name: "v2 encrypted segment upload", format: StateFormatV2, encryption: testEncryptionConfig(8), failPrefix: segmentDir + "/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			volumeID := "vol-interrupted-" + strings.NewReplacer(" ", "-", "/", "-").Replace(test.name)
			walPath := filepath.Join(t.TempDir(), "engine.wal")
			base := objectstore.NewMemoryStore(t.Name())
			store := &failOncePutStore{Store: base, prefix: test.failPrefix}
			heads := newMemoryHeadStore()
			cfg := Config{
				VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads,
				StateFormatVersion: test.format, Encryption: test.encryption,
			}
			engine, err := Open(ctx, cfg)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			node, err := engine.CreateFile(RootInode, "durable.txt", 0o644)
			if err != nil {
				t.Fatalf("CreateFile() error = %v", err)
			}
			want := []byte("bytes-survive-every-publication-boundary")
			if _, err := engine.Write(node.Inode, 0, want); err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if _, err := engine.SyncMaterialize(ctx); !errors.Is(err, errInjectedObjectPut) {
				t.Fatalf("SyncMaterialize(interrupted) error = %v, want injected put failure", err)
			}
			if _, err := heads.LoadCommittedHead(ctx, volumeID); !errors.Is(err, ErrCommittedHeadNotFound) {
				t.Fatalf("LoadCommittedHead(after interrupted publish) error = %v, want no committed head", err)
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("Close(after interruption) error = %v", err)
			}

			store.disableFailure()
			recovered, err := Open(ctx, cfg)
			if err != nil {
				t.Fatalf("Open(WAL recovery) error = %v", err)
			}
			got, err := recovered.Read(node.Inode, 0, uint64(len(want)+1))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("Read(WAL recovery) = %q, %v; want %q", got, err, want)
			}
			if _, err := recovered.SyncMaterialize(ctx); err != nil {
				t.Fatalf("SyncMaterialize(recovered) error = %v", err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatalf("Close(recovered) error = %v", err)
			}

			coldCfg := cfg
			coldCfg.WALPath = filepath.Join(t.TempDir(), "cold.wal")
			cold, err := Open(ctx, coldCfg)
			if err != nil {
				t.Fatalf("Open(cold) error = %v", err)
			}
			defer cold.Close()
			got, err = cold.Read(node.Inode, 0, uint64(len(want)+1))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("Read(cold) = %q, %v; want %q", got, err, want)
			}
		})
	}
}

type blockingLoadHeadStore struct {
	*memoryHeadStore

	mu      sync.Mutex
	calls   int
	blockOn int
	entered chan struct{}
	release chan struct{}
}

var errInjectedObjectPut = errors.New("injected object put failure")
var errTransientSegmentHead = errors.New("transient segment HEAD failure")

type failOncePutStore struct {
	objectstore.Store

	mu       sync.Mutex
	prefix   string
	disabled bool
	failed   bool
}

type blockingPutStore struct {
	objectstore.Store

	mu      sync.Mutex
	prefix  string
	entered chan struct{}
	release chan struct{}
}

type failingSegmentHeadStore struct {
	objectstore.Store

	mu  sync.Mutex
	err error
}

func (s *failingSegmentHeadStore) setFailure(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *failingSegmentHeadStore) Head(key string) (objectstore.Info, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil && strings.HasPrefix(key, segmentDir+"/") {
		return objectstore.Info{}, err
	}
	return s.Store.Head(key)
}

func (s *blockingPutStore) blockNext(prefix string) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefix = prefix
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

func (s *blockingPutStore) Put(key string, reader io.Reader) error {
	s.mu.Lock()
	blocked := s.prefix != "" && strings.HasPrefix(key, s.prefix)
	entered := s.entered
	release := s.release
	if blocked {
		s.prefix = ""
	}
	s.mu.Unlock()
	if blocked {
		close(entered)
		<-release
	}
	return s.Store.Put(key, reader)
}

func (s *failOncePutStore) Put(key string, reader io.Reader) error {
	s.mu.Lock()
	shouldFail := !s.disabled && !s.failed && strings.HasPrefix(key, s.prefix)
	if shouldFail {
		s.failed = true
	}
	s.mu.Unlock()
	if shouldFail {
		return errInjectedObjectPut
	}
	return s.Store.Put(key, reader)
}

func (s *failOncePutStore) disableFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disabled = true
}

func (s *blockingLoadHeadStore) blockLoad(call int) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = 0
	s.blockOn = call
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

func (s *blockingLoadHeadStore) LoadCommittedHead(ctx context.Context, volumeID string) (*CommittedHead, error) {
	s.mu.Lock()
	s.calls++
	blocked := s.blockOn > 0 && s.calls == s.blockOn
	entered := s.entered
	release := s.release
	if blocked {
		s.blockOn = 0
	}
	s.mu.Unlock()
	if blocked {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.memoryHeadStore.LoadCommittedHead(ctx, volumeID)
}

func materializeConsistencyFixture(t *testing.T, ctx context.Context, volumeID, walPath string, store objectstore.Store, heads HeadStore, payload string) (*Engine, *Node, *Manifest) {
	t.Helper()
	engine, err := Open(ctx, Config{VolumeID: volumeID, WALPath: walPath, ObjectStore: store, HeadStore: heads})
	if err != nil {
		t.Fatalf("Open(fixture) error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "authority.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(fixture) error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte(payload)); err != nil {
		t.Fatalf("Write(fixture) error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(fixture) error = %v", err)
	}
	return engine, node, manifest
}

func onlyManifestSegment(t *testing.T, manifest *Manifest) *Segment {
	t.Helper()
	if manifest == nil || manifest.State == nil || len(manifest.State.Segments) != 1 {
		t.Fatalf("manifest segments = %+v, want exactly one", manifest)
	}
	for _, segment := range manifest.State.Segments {
		return segment
	}
	return nil
}
