package s0fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

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
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
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
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
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

	reader, err := Open(ctx, Config{
		VolumeID:    "vol-refresh",
		WALPath:     filepath.Join(t.TempDir(), "reader.wal"),
		ObjectStore: store,
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
	staleWALPath := filepath.Join(t.TempDir(), "stale.wal")

	stale, err := Open(ctx, Config{
		VolumeID:    "vol-stale-head",
		WALPath:     staleWALPath,
		ObjectStore: store,
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
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-1",
		WALPath:     walPath,
		ObjectStore: store,
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
	if first == nil || first.ManifestSeq != 1 {
		t.Fatalf("first manifest = %+v, want seq 1", first)
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
	if second == nil || second.ManifestSeq != 2 {
		t.Fatalf("second manifest = %+v, want seq 2", second)
	}

	materializer := NewMaterializer(store)
	latest, err := materializer.LoadLatestManifest(ctx)
	if err != nil {
		t.Fatalf("LoadLatestManifest() error = %v", err)
	}
	if latest.ManifestSeq != 2 {
		t.Fatalf("latest manifest seq = %d, want 2", latest.ManifestSeq)
	}
	if _, err := store.Head(manifestKey(1)); err != nil {
		t.Fatalf("Head(manifest 1) error = %v", err)
	}
	if _, err := store.Head(manifestKey(2)); err != nil {
		t.Fatalf("Head(manifest 2) error = %v", err)
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
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-retain",
		WALPath:     walPath,
		ObjectStore: store,
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
	if second == nil || second.ManifestSeq != 2 {
		t.Fatalf("second manifest = %+v, want seq 2", second)
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

func TestEngineColdSmallFileReadsUseSegmentCache(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-cache")
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-cache",
		WALPath:     walPath,
		ObjectStore: store,
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

func TestCreateSnapshotHydratesColdFilesInline(t *testing.T) {
	ctx := context.Background()
	store := newPrefixedRecordingStore(t, "vol-snapshot")
	walPath := filepath.Join(t.TempDir(), "engine.wal")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-snapshot",
		WALPath:     walPath,
		ObjectStore: store,
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
	if len(state.ColdFiles) != 0 {
		t.Fatalf("snapshot cold files = %+v, want inline data", state.ColdFiles)
	}
	snapNode, err := state.Lookup(RootInode, "snap.txt")
	if err != nil {
		t.Fatalf("snapshot Lookup() error = %v", err)
	}
	payload, err := state.Read(snapNode.Inode, 0, snapNode.Size)
	if err != nil {
		t.Fatalf("SnapshotState.Read() error = %v", err)
	}
	if string(payload) != "snapshot-data" {
		t.Fatalf("snapshot data = %q, want snapshot-data", payload)
	}
}

func newPrefixedRecordingStore(t *testing.T, volumeID string) *recordingStore {
	t.Helper()
	base := objectstore.NewMemoryStore("s0fs-tests")
	return &recordingStore{Store: objectstore.Prefix(base, "sandboxvolumes/team-a/"+volumeID+"/s0fs/")}
}

func fileName(i int) string {
	return strings.Join([]string{"file", string(rune('a' + (i % 26))), string(rune('0' + (i % 10))), ".txt"}, "")
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
	segment, files, err := buildSegment(7, state)
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
