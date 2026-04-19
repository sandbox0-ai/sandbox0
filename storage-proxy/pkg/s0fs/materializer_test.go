package s0fs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
)

type getCall struct {
	key   string
	off   int64
	limit int64
}

type recordingStore struct {
	object.ObjectStorage

	mu   sync.Mutex
	gets []getCall
}

func (s *recordingStore) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets = append(s.gets, getCall{key: key, off: off, limit: limit})
	s.mu.Unlock()
	return s.ObjectStorage.Get(key, off, limit, getters...)
}

func (s *recordingStore) calls() []getCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]getCall, len(s.gets))
	copy(out, s.gets)
	return out
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
	if segmentRead.off != 6 || segmentRead.limit != 5 {
		t.Fatalf("segment range read = %+v, want off=6 limit=5", *segmentRead)
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

	objects, _, _, err := store.List("", "", "", "", 1000, false)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(objects) >= 12 {
		t.Fatalf("object count = %d, want < 12 for 64 files", len(objects))
	}
}

func newPrefixedRecordingStore(t *testing.T, volumeID string) *recordingStore {
	t.Helper()
	base, err := object.CreateStorage("mem", "s0fs-tests", "", "", "")
	if err != nil {
		t.Fatalf("CreateStorage(mem) error = %v", err)
	}
	return &recordingStore{ObjectStorage: object.WithPrefix(base, "sandboxvolumes/team-a/"+volumeID+"/s0fs/")}
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
