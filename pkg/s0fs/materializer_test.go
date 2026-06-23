package s0fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

type memoryHeadStore struct {
	mu    sync.Mutex
	heads map[string]*CommittedHead
}

func newMemoryHeadStore() *memoryHeadStore {
	return &memoryHeadStore{heads: make(map[string]*CommittedHead)}
}

func (s *memoryHeadStore) LoadCommittedHead(_ context.Context, volumeID string) (*CommittedHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	head := s.heads[volumeID]
	if head == nil {
		return nil, ErrCommittedHeadNotFound
	}
	clone := *head
	return &clone, nil
}

func (s *memoryHeadStore) CompareAndSwapCommittedHead(_ context.Context, volumeID string, expectedManifestSeq uint64, head *CommittedHead) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.heads[volumeID]
	if current == nil {
		if expectedManifestSeq != 0 {
			return ErrCommittedHeadConflict
		}
		clone := *head
		s.heads[volumeID] = &clone
		return nil
	}
	if current.ManifestSeq != expectedManifestSeq || head.ManifestSeq <= current.ManifestSeq {
		return ErrCommittedHeadConflict
	}
	clone := *head
	s.heads[volumeID] = &clone
	return nil
}

type blockingHeadStore struct {
	*memoryHeadStore
	calls        atomic.Int32
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func newBlockingHeadStore() *blockingHeadStore {
	return &blockingHeadStore{
		memoryHeadStore: newMemoryHeadStore(),
		firstEntered:    make(chan struct{}),
		releaseFirst:    make(chan struct{}),
	}
}

func (s *blockingHeadStore) CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *CommittedHead) error {
	if s.calls.Add(1) == 1 {
		close(s.firstEntered)
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.memoryHeadStore.CompareAndSwapCommittedHead(ctx, volumeID, expectedManifestSeq, head)
}

type getCall struct {
	key   string
	off   int64
	limit int64
}

type putCall struct {
	key  string
	size int
}

type recordingStore struct {
	objectstore.Store

	mu   sync.Mutex
	gets []getCall
	puts []putCall
}

func (s *recordingStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets = append(s.gets, getCall{key: key, off: off, limit: limit})
	s.mu.Unlock()
	return s.Store.Get(key, off, limit)
}

func (s *recordingStore) Put(key string, in io.Reader) error {
	payload, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.puts = append(s.puts, putCall{key: key, size: len(payload)})
	s.mu.Unlock()
	return s.Store.Put(key, bytes.NewReader(payload))
}

func (s *recordingStore) calls() []getCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]getCall, len(s.gets))
	copy(out, s.gets)
	return out
}

func (s *recordingStore) putCalls() []putCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]putCall, len(s.puts))
	copy(out, s.puts)
	return out
}

func (s *recordingStore) resetCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = nil
	s.puts = nil
}

func TestEngineMaterializeRecoversViaColdRangeRead(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-1")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello world")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.Remove(headStatePath(walPath)); err != nil {
		t.Fatalf("Remove(head) error = %v", err)
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatalf("Remove(wal) error = %v", err)
	}

	recovered, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	defer recovered.Close()

	recoveredNode, err := recovered.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	data, err := recovered.Read(recoveredNode.Inode, 6, 5)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(data) != "world" {
		t.Fatalf("Read() = %q, want %q", data, "world")
	}

	var segmentRead *getCall
	for _, call := range store.calls() {
		if strings.HasPrefix(call.key, segmentDir+"/") {
			segmentRead = &call
		}
	}
	if segmentRead == nil {
		t.Fatal("expected a cold segment Get call")
	}
	if segmentRead.off != 0 || segmentRead.limit != 11 {
		t.Fatalf("segment cache read = %+v, want full segment read", *segmentRead)
	}
}

func TestEngineRecoversFromManifestAndRetainedWAL(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-1")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	base, err := engine.CreateFile(RootInode, "base.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(base) error = %v", err)
	}
	if _, err := engine.Write(base.Inode, 0, []byte("base")); err != nil {
		t.Fatalf("Write(base) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}

	delta, err := engine.CreateFile(RootInode, "delta.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(delta) error = %v", err)
	}
	if _, err := engine.Write(delta.Inode, 0, []byte("delta")); err != nil {
		t.Fatalf("Write(delta) error = %v", err)
	}
	if err := engine.Fsync(delta.Inode); err != nil {
		t.Fatalf("Fsync(delta) error = %v", err)
	}

	if err := os.Remove(headStatePath(walPath)); err != nil {
		t.Fatalf("Remove(head) error = %v", err)
	}

	recovered, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	defer recovered.Close()
	defer engine.Close()

	if _, err := recovered.Lookup(RootInode, "base.txt"); err != nil {
		t.Fatalf("Lookup(base) error = %v", err)
	}
	deltaNode, err := recovered.Lookup(RootInode, "delta.txt")
	if err != nil {
		t.Fatalf("Lookup(delta) error = %v", err)
	}
	payload, err := recovered.Read(deltaNode.Inode, 0, 5)
	if err != nil {
		t.Fatalf("Read(delta) error = %v", err)
	}
	if string(payload) != "delta" {
		t.Fatalf("Read(delta) = %q, want %q", payload, "delta")
	}
}

func TestEngineRefreshMaterializedLoadsNewerManifest(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-refresh")
	heads := newMemoryHeadStore()

	reader, err := Open(ctx, Config{
		VolumeID:    "vol-refresh",
		WALPath:     filepath.Join(t.TempDir(), "reader.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(reader) error = %v", err)
	}
	defer reader.Close()
	if _, err := reader.Lookup(RootInode, "late.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(late before refresh) err = %v, want ErrNotFound", err)
	}

	writer, err := Open(ctx, Config{
		VolumeID:    "vol-refresh",
		WALPath:     filepath.Join(t.TempDir(), "writer.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(writer) error = %v", err)
	}
	node, err := writer.CreateFile(RootInode, "late.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(late) error = %v", err)
	}
	if _, err := writer.Write(node.Inode, 0, []byte("late data")); err != nil {
		t.Fatalf("Write(late) error = %v", err)
	}
	if _, err := writer.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(writer) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}

	refreshed, err := reader.RefreshMaterialized(ctx)
	if err != nil {
		t.Fatalf("RefreshMaterialized() error = %v", err)
	}
	if !refreshed {
		t.Fatal("RefreshMaterialized() refreshed = false, want true")
	}
	refreshedNode, err := reader.Lookup(RootInode, "late.txt")
	if err != nil {
		t.Fatalf("Lookup(late after refresh) error = %v", err)
	}
	payload, err := reader.Read(refreshedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read(late after refresh) error = %v", err)
	}
	if string(payload) != "late data" {
		t.Fatalf("Read(late after refresh) = %q, want %q", payload, "late data")
	}
}

func TestEngineOpenPrefersNewerMaterializedManifestOverStaleHead(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-stale-head")
	heads := newMemoryHeadStore()
	staleWALPath := filepath.Join(t.TempDir(), "stale.wal")

	stale, err := Open(ctx, Config{
		VolumeID:    "vol-stale-head",
		WALPath:     staleWALPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(stale) error = %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("Close(stale) error = %v", err)
	}

	writer, err := Open(ctx, Config{
		VolumeID:    "vol-stale-head",
		WALPath:     filepath.Join(t.TempDir(), "writer.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(writer) error = %v", err)
	}
	node, err := writer.CreateFile(RootInode, "remote.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(remote) error = %v", err)
	}
	if _, err := writer.Write(node.Inode, 0, []byte("remote data")); err != nil {
		t.Fatalf("Write(remote) error = %v", err)
	}
	if _, err := writer.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(writer) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}

	reopened, err := Open(ctx, Config{
		VolumeID:    "vol-stale-head",
		WALPath:     staleWALPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(reopened) error = %v", err)
	}
	defer reopened.Close()

	reopenedNode, err := reopened.Lookup(RootInode, "remote.txt")
	if err != nil {
		t.Fatalf("Lookup(remote) error = %v", err)
	}
	payload, err := reopened.Read(reopenedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read(remote) error = %v", err)
	}
	if string(payload) != "remote data" {
		t.Fatalf("Read(remote) = %q, want %q", payload, "remote data")
	}
}

func TestMaterializerCoalescesSmallFilesAndKeepsManifestMonotonic(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-1")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	for i := 0; i < 64; i++ {
		node, err := engine.CreateFile(RootInode, fileName(i), 0o644)
		if err != nil {
			t.Fatalf("CreateFile(%d) error = %v", i, err)
		}
		if _, err := engine.Write(node.Inode, 0, []byte{byte(i)}); err != nil {
			t.Fatalf("Write(%d) error = %v", i, err)
		}
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(first) error = %v", err)
	}
	if first == nil {
		t.Fatal("first manifest is nil")
	}
	if first.ManifestSeq != first.CheckpointSeq {
		t.Fatalf("first manifest seq/checkpoint = %d/%d, want equal", first.ManifestSeq, first.CheckpointSeq)
	}

	node, err := engine.CreateFile(RootInode, "tail.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(tail) error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("tail")); err != nil {
		t.Fatalf("Write(tail) error = %v", err)
	}
	second, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(second) error = %v", err)
	}
	if second == nil {
		t.Fatal("second manifest is nil")
	}
	if second.ManifestSeq <= first.ManifestSeq {
		t.Fatalf("second manifest seq = %d, want > %d", second.ManifestSeq, first.ManifestSeq)
	}

	materializer := NewMaterializer("vol-1", store, heads)
	latest, err := materializer.LoadLatestManifest(ctx)
	if err != nil {
		t.Fatalf("LoadLatestManifest() error = %v", err)
	}
	if latest.ManifestSeq != second.ManifestSeq {
		t.Fatalf("latest manifest seq = %d, want %d", latest.ManifestSeq, second.ManifestSeq)
	}
	if _, err := store.Head(manifestKey(first.ManifestSeq)); err != nil {
		t.Fatalf("Head(manifest %d) error = %v", first.ManifestSeq, err)
	}
	if _, err := store.Head(manifestKey(second.ManifestSeq)); err != nil {
		t.Fatalf("Head(manifest %d) error = %v", second.ManifestSeq, err)
	}

	objects, _, _, err := store.List("", "", "", "", 1000)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objects) >= 12 {
		t.Fatalf("object count = %d, want < 12 for 64 files", len(objects))
	}
}

func TestMaterializerRetainsColdFilesAndWritesOnlyHotData(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-retain")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-retain",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	baseA, err := engine.CreateFile(RootInode, "a.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(a) error = %v", err)
	}
	if _, err := engine.Write(baseA.Inode, 0, []byte("alpha")); err != nil {
		t.Fatalf("Write(a) error = %v", err)
	}
	baseB, err := engine.CreateFile(RootInode, "b.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(b) error = %v", err)
	}
	if _, err := engine.Write(baseB.Inode, 0, []byte("bravo")); err != nil {
		t.Fatalf("Write(b) error = %v", err)
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(first) error = %v", err)
	}
	if first == nil || len(first.State.Segments) != 1 {
		t.Fatalf("first manifest segments = %+v", first)
	}
	firstSegmentID := first.State.ColdFiles[baseA.Inode][0].SegmentID
	store.resetCalls()

	tail, err := engine.CreateFile(RootInode, "tail.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(tail) error = %v", err)
	}
	if _, err := engine.Write(tail.Inode, 0, []byte("tail")); err != nil {
		t.Fatalf("Write(tail) error = %v", err)
	}
	second, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(second) error = %v", err)
	}
	if second == nil {
		t.Fatal("second manifest is nil")
	}
	if second.ManifestSeq <= first.ManifestSeq {
		t.Fatalf("second manifest seq = %d, want > %d", second.ManifestSeq, first.ManifestSeq)
	}
	if len(second.State.Segments) != 2 {
		t.Fatalf("second manifest segment count = %d, want 2", len(second.State.Segments))
	}
	if got := second.State.ColdFiles[baseA.Inode][0].SegmentID; got != firstSegmentID {
		t.Fatalf("a.txt segment = %s, want retained %s", got, firstSegmentID)
	}
	if got := second.State.ColdFiles[tail.Inode][0].SegmentID; got == firstSegmentID {
		t.Fatalf("tail.txt reused base segment %s", got)
	}

	var segmentGets int
	for _, call := range store.calls() {
		if strings.HasPrefix(call.key, segmentDir+"/") {
			segmentGets++
		}
	}
	if segmentGets != 0 {
		t.Fatalf("segment Get calls during incremental materialize = %d, want 0", segmentGets)
	}
	var segmentPuts []putCall
	for _, call := range store.putCalls() {
		if strings.HasPrefix(call.key, segmentDir+"/") {
			segmentPuts = append(segmentPuts, call)
		}
	}
	if len(segmentPuts) != 1 || segmentPuts[0].size != len("tail") {
		t.Fatalf("segment Put calls = %+v, want one hot segment of size 4", segmentPuts)
	}
}

func TestMaterializerWithCommittedHeadStoreSkipsLegacyLatestObject(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-headstore")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-headstore",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if manifest == nil {
		t.Fatal("SyncMaterialize() returned nil manifest")
	}

	if _, err := store.Head(manifestLatestKey); err == nil {
		t.Fatalf("Head(%s) unexpectedly succeeded", manifestLatestKey)
	}
	for _, call := range store.putCalls() {
		if call.key == manifestLatestKey {
			t.Fatalf("legacy latest manifest Put call = %+v, want none", call)
		}
	}
	head, err := heads.LoadCommittedHead(ctx, "vol-headstore")
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if head.ManifestSeq != manifest.ManifestSeq || head.ManifestKey != manifestKey(manifest.ManifestSeq) {
		t.Fatalf("committed head = %+v, want manifest seq %d key %s", head, manifest.ManifestSeq, manifestKey(manifest.ManifestSeq))
	}
}

func TestEngineSyncMaterializeDetectsCommittedHeadConflicts(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-conflict")
	heads := newMemoryHeadStore()

	first, err := Open(ctx, Config{
		VolumeID:    "vol-conflict",
		WALPath:     filepath.Join(t.TempDir(), "first.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	defer first.Close()
	second, err := Open(ctx, Config{
		VolumeID:    "vol-conflict",
		WALPath:     filepath.Join(t.TempDir(), "second.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(second) error = %v", err)
	}
	defer second.Close()

	firstNode, err := first.CreateFile(RootInode, "first.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(first) error = %v", err)
	}
	if _, err := first.Write(firstNode.Inode, 0, []byte("one")); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}

	secondNode, err := second.CreateFile(RootInode, "second.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(second) error = %v", err)
	}
	if _, err := second.Write(secondNode.Inode, 0, []byte("two")); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}

	if _, err := first.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(first) error = %v", err)
	}
	if _, err := second.SyncMaterialize(ctx); !errors.Is(err, ErrCommittedHeadConflict) {
		t.Fatalf("SyncMaterialize(second) err = %v, want %v", err, ErrCommittedHeadConflict)
	}
}

func TestEngineSyncMaterializeSerializesSameEngineCommits(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-serial-materialize")
	heads := newBlockingHeadStore()

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-serial-materialize",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := engine.SyncMaterialize(ctx)
		firstDone <- err
	}()

	select {
	case <-heads.firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first SyncMaterialize did not reach committed head CAS")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := engine.SyncMaterialize(ctx)
		secondDone <- err
	}()

	select {
	case err := <-secondDone:
		t.Fatalf("second SyncMaterialize completed before first commit finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(heads.releaseFirst)

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first SyncMaterialize error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first SyncMaterialize did not complete")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second SyncMaterialize error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second SyncMaterialize did not complete")
	}
}

func TestEngineSyncMaterializeAdvancesCommittedHeadWhenDirtyDuringCommit(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-dirty-during-materialize")
	heads := newBlockingHeadStore()

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-dirty-during-materialize",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	first, err := engine.CreateFile(RootInode, "first.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(first) error = %v", err)
	}
	if _, err := engine.Write(first.Inode, 0, []byte("first")); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}

	materialized := make(chan error, 1)
	go func() {
		_, err := engine.SyncMaterialize(ctx)
		materialized <- err
	}()

	select {
	case <-heads.firstEntered:
	case <-time.After(time.Second):
		t.Fatal("SyncMaterialize did not reach committed head CAS")
	}

	second, err := engine.CreateFile(RootInode, "second.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(second) error = %v", err)
	}
	if _, err := engine.Write(second.Inode, 0, []byte("second")); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}

	close(heads.releaseFirst)
	select {
	case err := <-materialized:
		if err != nil {
			t.Fatalf("SyncMaterialize(first) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SyncMaterialize(first) did not complete")
	}

	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(second) error = %v", err)
	}
	head, err := heads.LoadCommittedHead(ctx, "vol-dirty-during-materialize")
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if head.ManifestSeq < 2 {
		t.Fatalf("committed manifest seq = %d, want at least 2", head.ManifestSeq)
	}
}

func TestEngineColdSmallFileReadsUseSegmentCache(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-cache")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-cache",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, item := range []struct {
		name string
		body string
	}{
		{name: "a.txt", body: "alpha"},
		{name: "b.txt", body: "bravo"},
		{name: "c.txt", body: "charlie"},
	} {
		node, err := engine.CreateFile(RootInode, item.name, 0o644)
		if err != nil {
			t.Fatalf("CreateFile(%s) error = %v", item.name, err)
		}
		if _, err := engine.Write(node.Inode, 0, []byte(item.body)); err != nil {
			t.Fatalf("Write(%s) error = %v", item.name, err)
		}
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(headStatePath(walPath)); err != nil {
		t.Fatalf("Remove(head) error = %v", err)
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatalf("Remove(wal) error = %v", err)
	}

	recovered, err := Open(ctx, Config{
		VolumeID:    "vol-cache",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	defer recovered.Close()
	store.resetCalls()

	for _, item := range []struct {
		name string
		body string
	}{
		{name: "a.txt", body: "alpha"},
		{name: "b.txt", body: "bravo"},
		{name: "c.txt", body: "charlie"},
	} {
		node, err := recovered.Lookup(RootInode, item.name)
		if err != nil {
			t.Fatalf("Lookup(%s) error = %v", item.name, err)
		}
		payload, err := recovered.Read(node.Inode, 0, node.Size)
		if err != nil {
			t.Fatalf("Read(%s) error = %v", item.name, err)
		}
		if string(payload) != item.body {
			t.Fatalf("Read(%s) = %q, want %q", item.name, payload, item.body)
		}
	}

	var segmentGets []getCall
	for _, call := range store.calls() {
		if strings.HasPrefix(call.key, segmentDir+"/") {
			segmentGets = append(segmentGets, call)
		}
	}
	if len(segmentGets) != 1 {
		t.Fatalf("segment Get calls = %+v, want exactly one cached segment read", segmentGets)
	}
	if segmentGets[0].off != 0 || segmentGets[0].limit <= 0 {
		t.Fatalf("segment cache read = %+v, want full segment read", segmentGets[0])
	}
}

func TestEngineForkStateReadsParentSegmentsAndWritesChildSegments(t *testing.T) {
	ctx := context.Background()
	base := objectstore.NewMemoryStore("s0fs-fork-state-test-" + t.Name())
	sourceStore := &recordingStore{Store: objectstore.Prefix(base, "sandboxvolumes/team-a/source/s0fs/")}
	childStore := &recordingStore{Store: objectstore.Prefix(base, "sandboxvolumes/team-a/child/s0fs/")}
	heads := newMemoryHeadStore()
	resolver := func(volumeID string) (objectstore.Store, error) {
		switch volumeID {
		case "source":
			return sourceStore, nil
		case "child":
			return childStore, nil
		default:
			return nil, ErrNotFound
		}
	}

	source, err := Open(ctx, Config{
		VolumeID:             "source",
		WALPath:              filepath.Join(t.TempDir(), "source.wal"),
		ObjectStore:          sourceStore,
		ObjectStoreForVolume: resolver,
		HeadStore:            heads,
	})
	if err != nil {
		t.Fatalf("Open(source) error = %v", err)
	}
	defer source.Close()

	node, err := source.CreateFile(RootInode, "pkg.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(source) error = %v", err)
	}
	if _, err := source.Write(node.Inode, 0, []byte("parent-data")); err != nil {
		t.Fatalf("Write(source) error = %v", err)
	}
	if _, err := source.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(source) error = %v", err)
	}

	sourceState, _, err := NewMaterializer("source", sourceStore, heads, resolver).LoadLatestState(ctx)
	if err != nil {
		t.Fatalf("LoadLatestState(source) error = %v", err)
	}
	forkState, err := PrepareForkState(sourceState, "source")
	if err != nil {
		t.Fatalf("PrepareForkState() error = %v", err)
	}
	if len(forkState.Data) != 0 {
		t.Fatalf("fork state data = %+v, want empty", forkState.Data)
	}
	for _, segment := range forkState.Segments {
		if segment.VolumeID != "source" {
			t.Fatalf("fork segment volume = %q, want source", segment.VolumeID)
		}
	}

	child, err := Open(ctx, Config{
		VolumeID:             "child",
		WALPath:              filepath.Join(t.TempDir(), "child.wal"),
		ObjectStore:          childStore,
		ObjectStoreForVolume: resolver,
		HeadStore:            heads,
	})
	if err != nil {
		t.Fatalf("Open(child) error = %v", err)
	}
	defer child.Close()
	if err := child.ReplaceState(forkState); err != nil {
		t.Fatalf("ReplaceState(child) error = %v", err)
	}
	if _, err := child.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(child fork) error = %v", err)
	}

	sourceStore.resetCalls()
	childStore.resetCalls()
	childNode, err := child.Lookup(RootInode, "pkg.txt")
	if err != nil {
		t.Fatalf("Lookup(child) error = %v", err)
	}
	payload, err := child.Read(childNode.Inode, 0, childNode.Size)
	if err != nil {
		t.Fatalf("Read(child inherited) error = %v", err)
	}
	if string(payload) != "parent-data" {
		t.Fatalf("Read(child inherited) = %q, want parent-data", payload)
	}
	if !hasSegmentGet(sourceStore.calls()) {
		t.Fatalf("source segment reads = %+v, want inherited segment read", sourceStore.calls())
	}
	if hasSegmentGet(childStore.calls()) {
		t.Fatalf("child segment reads = %+v, want no child segment read for inherited data", childStore.calls())
	}

	if _, err := child.Write(childNode.Inode, 0, []byte("child-data")); err != nil {
		t.Fatalf("Write(child) error = %v", err)
	}
	if _, err := child.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(child write) error = %v", err)
	}
	childState, _, err := NewMaterializer("child", childStore, heads, resolver).LoadLatestState(ctx)
	if err != nil {
		t.Fatalf("LoadLatestState(child) error = %v", err)
	}
	var childSegmentCount int
	for _, segment := range childState.Segments {
		if segment.VolumeID == "child" {
			childSegmentCount++
		}
	}
	if childSegmentCount == 0 {
		t.Fatalf("child segments = %+v, want at least one child-owned segment", childState.Segments)
	}

	sourceNode, err := source.Lookup(RootInode, "pkg.txt")
	if err != nil {
		t.Fatalf("Lookup(source) after child write error = %v", err)
	}
	sourcePayload, err := source.Read(sourceNode.Inode, 0, sourceNode.Size)
	if err != nil {
		t.Fatalf("Read(source) after child write error = %v", err)
	}
	if string(sourcePayload) != "parent-data" {
		t.Fatalf("Read(source) after child write = %q, want parent-data", sourcePayload)
	}
}

func TestMaterializerSegmentCacheIsVolumeQualified(t *testing.T) {
	base := objectstore.NewMemoryStore("s0fs-segment-cache-volume-test-" + t.Name())
	sourceA := objectstore.Prefix(base, "sandboxvolumes/team-a/source-a/s0fs/")
	sourceB := objectstore.Prefix(base, "sandboxvolumes/team-a/source-b/s0fs/")
	child := objectstore.Prefix(base, "sandboxvolumes/team-a/child/s0fs/")
	if err := sourceA.Put("segments/shared.bin", bytes.NewReader([]byte("alpha"))); err != nil {
		t.Fatalf("Put(source-a) error = %v", err)
	}
	if err := sourceB.Put("segments/shared.bin", bytes.NewReader([]byte("bravo"))); err != nil {
		t.Fatalf("Put(source-b) error = %v", err)
	}
	materializer := NewMaterializer("child", child, nil, func(volumeID string) (objectstore.Store, error) {
		switch volumeID {
		case "source-a":
			return sourceA, nil
		case "source-b":
			return sourceB, nil
		default:
			return nil, ErrNotFound
		}
	})

	first, err := materializer.ReadSegmentRange(&Segment{ID: "shared", VolumeID: "source-a", Key: "segments/shared.bin", Length: 5}, 0, 5)
	if err != nil {
		t.Fatalf("ReadSegmentRange(source-a) error = %v", err)
	}
	second, err := materializer.ReadSegmentRange(&Segment{ID: "shared", VolumeID: "source-b", Key: "segments/shared.bin", Length: 5}, 0, 5)
	if err != nil {
		t.Fatalf("ReadSegmentRange(source-b) error = %v", err)
	}
	if string(first) != "alpha" || string(second) != "bravo" {
		t.Fatalf("segment reads = %q/%q, want alpha/bravo", first, second)
	}
}

func TestPrepareForkStatePreservesInlineData(t *testing.T) {
	now := time.Now()
	state := &SnapshotState{
		NextSeq:   3,
		NextInode: 3,
		Nodes: map[uint64]*Node{
			RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 2, Atime: now, Mtime: now, Ctime: now},
			2:         {Inode: 2, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 6, Atime: now, Mtime: now, Ctime: now},
		},
		Children: map[uint64]map[string]uint64{
			RootInode: {"inline.txt": 2},
		},
		Data: map[uint64][]byte{
			2: []byte("inline"),
		},
		ColdFiles: map[uint64][]FileExtent{},
		Segments:  map[string]*Segment{},
	}

	forkState, err := PrepareForkState(state, "source")
	if err != nil {
		t.Fatalf("PrepareForkState() error = %v", err)
	}
	if got := string(forkState.Data[2]); got != "inline" {
		t.Fatalf("fork inline data = %q, want inline", got)
	}
}

func TestCreateSnapshotPreservesColdSegments(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-snapshot")
	heads := newMemoryHeadStore()
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-snapshot",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "snap.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("snapshot-data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}

	state, err := engine.CreateSnapshot("snap-1")
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if len(state.Data) != 0 {
		t.Fatalf("snapshot inline data = %+v, want none", state.Data)
	}
	if len(state.ColdFiles) == 0 {
		t.Fatal("snapshot state has no cold files")
	}
	for _, segment := range state.Segments {
		if segment.VolumeID != "vol-snapshot" {
			t.Fatalf("snapshot segment volume = %q, want vol-snapshot", segment.VolumeID)
		}
	}
	snapNode, err := state.Lookup(RootInode, "snap.txt")
	if err != nil {
		t.Fatalf("snapshot Lookup() error = %v", err)
	}
	reader := NewSnapshotReader(state, engine.materializer)
	payload, err := reader.Read(snapNode.Inode, 0, snapNode.Size)
	if err != nil {
		t.Fatalf("SnapshotReader.Read() error = %v", err)
	}
	if string(payload) != "snapshot-data" {
		t.Fatalf("snapshot data = %q, want snapshot-data", payload)
	}

	localSnapshots, err := LoadLocalSnapshots(ctx, Config{
		VolumeID:    "vol-snapshot",
		WALPath:     walPath,
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("LoadLocalSnapshots() error = %v", err)
	}
	if len(localSnapshots) != 1 {
		t.Fatalf("local snapshots = %d, want 1", len(localSnapshots))
	}
}

func TestMaterializerGarbageCollectionPlanDeletesUnreferencedObjects(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-gc-plan")
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID:    "vol-gc-plan",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "state.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("old")); err != nil {
		t.Fatalf("Write(old) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(old) error = %v", err)
	}
	if err := engine.Truncate(node.Inode, 0); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("new")); err != nil {
		t.Fatalf("Write(new) error = %v", err)
	}
	latest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(new) error = %v", err)
	}

	retained := map[string]struct{}{
		manifestKey(latest.ManifestSeq): {},
	}
	plan, err := engine.materializer.PlanGarbageCollection(ctx, []*SnapshotState{latest.State}, retained)
	if err != nil {
		t.Fatalf("PlanGarbageCollection() error = %v", err)
	}
	if got, want := plan.Segments, []string{"segments/00000000000000000002-0.bin"}; !equalStrings(got, want) {
		t.Fatalf("plan segments = %v, want %v", got, want)
	}
	if got, want := plan.Manifests, []string{"manifests/00000000000000000002.json"}; !equalStrings(got, want) {
		t.Fatalf("plan manifests = %v, want %v", got, want)
	}
	if _, err := plan.Apply(ctx); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if _, err := store.Head("segments/00000000000000000002-0.bin"); err == nil {
		t.Fatal("old segment still exists after GC")
	}
	if _, err := store.Head("manifests/00000000000000000002.json"); err == nil {
		t.Fatal("old manifest still exists after GC")
	}
	if _, err := store.Head("segments/00000000000000000004-0.bin"); err != nil {
		t.Fatalf("current segment missing after GC: %v", err)
	}
	if _, err := store.Head("manifests/00000000000000000004.json"); err != nil {
		t.Fatalf("current manifest missing after GC: %v", err)
	}
}

func TestCompactionGarbageCollectionRetainsSnapshotSegments(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-compact-snapshot")
	engine, err := Open(ctx, Config{
		VolumeID:          "vol-compact-snapshot",
		WALPath:           filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:       store,
		HeadStore:         newMemoryHeadStore(),
		SegmentTargetSize: 1024,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write(initial) error = %v", err)
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(initial) error = %v", err)
	}
	var oldSegmentKey string
	for _, segment := range first.State.Segments {
		oldSegmentKey = segment.Key
	}
	if oldSegmentKey == "" {
		t.Fatal("initial materialization did not create a segment")
	}

	snapshotState, err := engine.CreateSnapshot("snap-before-overwrite")
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 1, []byte("Z")); err != nil {
		t.Fatalf("Write(overwrite) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(overwrite) error = %v", err)
	}
	compacted, _, err := engine.Compact(ctx, CompactionOptions{Force: true})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	latest, err := engine.Read(node.Inode, 0, 6)
	if err != nil {
		t.Fatalf("Read(latest) error = %v", err)
	}
	if string(latest) != "aZcdef" {
		t.Fatalf("latest payload = %q, want aZcdef", latest)
	}
	snapshotPayload, err := NewSnapshotReader(snapshotState, engine.materializer).Read(node.Inode, 0, 6)
	if err != nil {
		t.Fatalf("SnapshotReader.Read() error = %v", err)
	}
	if string(snapshotPayload) != "abcdef" {
		t.Fatalf("snapshot payload = %q, want abcdef", snapshotPayload)
	}

	retainedManifests := map[string]struct{}{manifestKey(compacted.ManifestSeq): {}}
	withSnapshot, err := engine.materializer.PlanGarbageCollection(ctx, []*SnapshotState{compacted.State, snapshotState}, retainedManifests)
	if err != nil {
		t.Fatalf("PlanGarbageCollection(with snapshot) error = %v", err)
	}
	for _, key := range withSnapshot.Segments {
		if key == oldSegmentKey {
			t.Fatalf("old snapshot segment %s was planned for deletion", oldSegmentKey)
		}
	}
	withoutSnapshot, err := engine.materializer.PlanGarbageCollection(ctx, []*SnapshotState{compacted.State}, retainedManifests)
	if err != nil {
		t.Fatalf("PlanGarbageCollection(without snapshot) error = %v", err)
	}
	if !slices.Contains(withoutSnapshot.Segments, oldSegmentKey) {
		t.Fatalf("old segment %s was not collectible without snapshot retention: %v", oldSegmentKey, withoutSnapshot.Segments)
	}
}

func TestExtentWriteDoesNotRewriteUnmodifiedColdData(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-extent-overwrite")
	heads := newMemoryHeadStore()
	engine, err := Open(ctx, Config{
		VolumeID:          "vol-extent-overwrite",
		WALPath:           filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:       store,
		HeadStore:         heads,
		SegmentTargetSize: 1024,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	a, err := engine.CreateFile(RootInode, "a.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(a) error = %v", err)
	}
	b, err := engine.CreateFile(RootInode, "b.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(b) error = %v", err)
	}
	aPayload := bytes.Repeat([]byte("a"), 64)
	bPayload := bytes.Repeat([]byte("b"), 64)
	if _, err := engine.Write(a.Inode, 0, aPayload); err != nil {
		t.Fatalf("Write(a) error = %v", err)
	}
	if _, err := engine.Write(b.Inode, 0, bPayload); err != nil {
		t.Fatalf("Write(b) error = %v", err)
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(first) error = %v", err)
	}
	if len(first.State.Segments) != 1 {
		t.Fatalf("first segment count = %d, want 1", len(first.State.Segments))
	}

	store.resetCalls()
	if _, err := engine.Write(a.Inode, 10, []byte("Z")); err != nil {
		t.Fatalf("overwrite a byte: %v", err)
	}
	second, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(second) error = %v", err)
	}
	if gets := store.calls(); len(gets) != 0 {
		t.Fatalf("overwrite materialization read cold segments = %+v, want none", gets)
	}
	if len(second.State.Segments) != 2 {
		t.Fatalf("second segment count = %d, want old segment plus 1-byte segment", len(second.State.Segments))
	}
	if got, want := StateStorageBytes(second.State), int64(len(aPayload)+len(bPayload)+1); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
	updated, err := engine.Read(a.Inode, 0, uint64(len(aPayload)))
	if err != nil {
		t.Fatalf("Read(a) error = %v", err)
	}
	wantPayload := bytes.Clone(aPayload)
	wantPayload[10] = 'Z'
	if !bytes.Equal(updated, wantPayload) {
		t.Fatalf("updated payload mismatch")
	}
	bRead, err := engine.Read(b.Inode, 0, uint64(len(bPayload)))
	if err != nil {
		t.Fatalf("Read(b) error = %v", err)
	}
	if !bytes.Equal(bRead, bPayload) {
		t.Fatalf("b payload changed")
	}
}

func TestMaterializerSplitsDirtyDataAtSegmentTargetSize(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-target-size")
	engine, err := Open(ctx, Config{
		VolumeID:          "vol-target-size",
		WALPath:           filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:       store,
		HeadStore:         newMemoryHeadStore(),
		SegmentTargetSize: 4,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "large.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("0123456789")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	var lengths []uint64
	for _, segment := range manifest.State.Segments {
		lengths = append(lengths, segment.Length)
	}
	slices.Sort(lengths)
	if got, want := lengths, []uint64{2, 4, 4}; !slices.Equal(got, want) {
		t.Fatalf("segment lengths = %v, want %v", got, want)
	}
}

func TestSparseExtentWriteReadsZerosWithoutStoringHoleBytes(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-sparse")
	engine, err := Open(ctx, Config{
		VolumeID:    "vol-sparse",
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: store,
		HeadStore:   newMemoryHeadStore(),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "sparse.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 4, []byte("x")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	payload, err := engine.Read(node.Inode, 0, 5)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got, want := payload, []byte{0, 0, 0, 0, 'x'}; !bytes.Equal(got, want) {
		t.Fatalf("payload = %v, want %v", got, want)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if got, want := StateStorageBytes(manifest.State), int64(1); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
}

func TestCompactionRewritesLiveRangesFromFragmentedSegments(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-compact-fragment")
	engine, err := Open(ctx, Config{
		VolumeID:          "vol-compact-fragment",
		WALPath:           filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:       store,
		HeadStore:         newMemoryHeadStore(),
		SegmentTargetSize: 1024,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	a, err := engine.CreateFile(RootInode, "a.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(a) error = %v", err)
	}
	b, err := engine.CreateFile(RootInode, "b.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(b) error = %v", err)
	}
	aPayload := bytes.Repeat([]byte("a"), 64)
	bPayload := bytes.Repeat([]byte("b"), 64)
	if _, err := engine.Write(a.Inode, 0, aPayload); err != nil {
		t.Fatalf("Write(a) error = %v", err)
	}
	if _, err := engine.Write(b.Inode, 0, bPayload); err != nil {
		t.Fatalf("Write(b) error = %v", err)
	}
	first, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize(first) error = %v", err)
	}
	var oldSegmentID string
	for id := range first.State.Segments {
		oldSegmentID = id
	}
	if _, err := engine.Write(a.Inode, 10, []byte("Z")); err != nil {
		t.Fatalf("overwrite a byte: %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(fragmented) error = %v", err)
	}

	manifest, result, err := engine.Compact(ctx, CompactionOptions{
		MinDeadRatio:    0.001,
		MinReclaimBytes: 1,
	})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if result == nil || !slices.Equal(result.CompactedSegments, []string{oldSegmentID}) {
		t.Fatalf("compacted segments = %+v, want %s", result, oldSegmentID)
	}
	if _, ok := manifest.State.Segments[oldSegmentID]; ok {
		t.Fatalf("old segment %s is still referenced after compaction", oldSegmentID)
	}
	if got, want := StateStorageBytes(manifest.State), int64(len(aPayload)+len(bPayload)); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
	updated, err := engine.Read(a.Inode, 0, uint64(len(aPayload)))
	if err != nil {
		t.Fatalf("Read(a) error = %v", err)
	}
	wantPayload := bytes.Clone(aPayload)
	wantPayload[10] = 'Z'
	if !bytes.Equal(updated, wantPayload) {
		t.Fatalf("compacted a payload mismatch")
	}
}

func TestLiveSegmentKeysForVolumeIgnoresChildOwnedSegments(t *testing.T) {
	state := &SnapshotState{
		ColdFiles: map[uint64][]FileExtent{
			2: {{SegmentID: "parent"}},
			3: {{SegmentID: "child"}},
		},
		Segments: map[string]*Segment{
			"parent": {ID: "parent", VolumeID: "vol-parent", Key: "segments/parent.bin"},
			"child":  {ID: "child", VolumeID: "vol-child", Key: "segments/child.bin"},
		},
	}
	live := liveSegmentKeysForVolume("vol-parent", state)
	if _, ok := live["segments/parent.bin"]; !ok {
		t.Fatal("parent segment was not marked live")
	}
	if _, ok := live["segments/child.bin"]; ok {
		t.Fatal("child-owned segment was marked live for parent volume")
	}
}

func newPrefixedRecordingStore(t *testing.T, volumeID string) *recordingStore {
	t.Helper()
	base := objectstore.NewMemoryStore("s0fs-tests")
	return &recordingStore{Store: objectstore.Prefix(base, "sandboxvolumes/team-a/"+volumeID+"/s0fs/")}
}

func hasSegmentGet(calls []getCall) bool {
	for _, call := range calls {
		if strings.HasPrefix(call.key, segmentDir+"/") {
			return true
		}
	}
	return false
}

func fileName(i int) string {
	return strings.Join([]string{"file", string(rune('a' + (i % 26))), string(rune('0' + (i % 10))), ".txt"}, "")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMaterializerBuildSegmentProducesRoundTrippableLayout(t *testing.T) {
	state := &SnapshotState{
		NextSeq:   4,
		NextInode: 4,
		Nodes: map[uint64]*Node{
			RootInode: {Inode: RootInode, Type: TypeDirectory},
			2:         {Inode: 2, Type: TypeFile, Size: 5},
			3:         {Inode: 3, Type: TypeFile, Size: 5},
		},
		Children: map[uint64]map[string]uint64{
			RootInode: {"a.txt": 2, "b.txt": 3},
		},
		Data: map[uint64][]byte{
			2: []byte("hello"),
			3: []byte("world"),
		},
	}
	segment, files, err := buildSegment(7, "vol-1", state)
	if err != nil {
		t.Fatalf("buildSegment() error = %v", err)
	}
	if got := string(segment.Payload); got != "helloworld" {
		t.Fatalf("segment payload = %q, want helloworld", got)
	}
	if !bytes.Equal(state.Data[2], []byte("hello")) || !bytes.Equal(state.Data[3], []byte("world")) {
		t.Fatal("buildSegment() mutated input state")
	}
	if len(files[2]) != 1 || files[2][0].Offset != 0 || files[2][0].Length != 5 {
		t.Fatalf("files[2] = %+v", files[2])
	}
	if len(files[3]) != 1 || files[3][0].Offset != 5 || files[3][0].Length != 5 {
		t.Fatalf("files[3] = %+v", files[3])
	}
}
