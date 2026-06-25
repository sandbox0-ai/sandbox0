package storageproxy

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

type integrationHeadStore struct {
	mu    sync.Mutex
	heads map[string]*s0fs.CommittedHead
}

func newIntegrationHeadStore() *integrationHeadStore {
	return &integrationHeadStore{heads: make(map[string]*s0fs.CommittedHead)}
}

func (s *integrationHeadStore) LoadCommittedHead(_ context.Context, volumeID string) (*s0fs.CommittedHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	head := s.heads[volumeID]
	if head == nil {
		return nil, s0fs.ErrCommittedHeadNotFound
	}
	clone := *head
	return &clone, nil
}

func (s *integrationHeadStore) CompareAndSwapCommittedHead(_ context.Context, volumeID string, expectedManifestSeq uint64, head *s0fs.CommittedHead) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.heads[volumeID]
	if current == nil {
		if expectedManifestSeq != 0 {
			return s0fs.ErrCommittedHeadConflict
		}
		clone := *head
		s.heads[volumeID] = &clone
		return nil
	}
	if current.ManifestSeq != expectedManifestSeq || head.ManifestSeq <= current.ManifestSeq {
		return s0fs.ErrCommittedHeadConflict
	}
	clone := *head
	s.heads[volumeID] = &clone
	return nil
}

func TestS0FSIntegrationPartialOverwritesCompactAndGarbageCollect(t *testing.T) {
	ctx := context.Background()
	volumeID := "vol-partial-overwrite"
	store := newIntegrationObjectStore(t, volumeID)
	heads := newIntegrationHeadStore()

	engine := openIntegrationEngine(t, ctx, volumeID, store, heads, 1024)
	node, err := engine.CreateFile(s0fs.RootInode, "pack.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	payload := deterministicPayload(4096)
	if _, err := engine.Write(node.Inode, 0, payload); err != nil {
		t.Fatalf("Write(initial) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(initial) error = %v", err)
	}
	initialSegments := listObjectKeys(t, store, "segments/")
	if got, want := len(initialSegments), 4; got != want {
		t.Fatalf("initial segment count = %d, want %d: %v", got, want, initialSegments)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(initial engine) error = %v", err)
	}

	engine = openIntegrationEngine(t, ctx, volumeID, store, heads, 1024)
	replacement := bytes.Repeat([]byte("Z"), 128)
	copy(payload[1536:], replacement)
	if _, err := engine.Write(node.Inode, 1536, replacement); err != nil {
		t.Fatalf("Write(overwrite) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(overwrite) error = %v", err)
	}
	fragmentedBytes := sumObjectSizes(t, store, "segments/")

	compacted, result, err := engine.Compact(ctx, s0fs.CompactionOptions{
		SegmentTargetSize: 1024,
		MinDeadRatio:      0.01,
		MinReclaimBytes:   1,
	})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if compacted == nil {
		t.Fatal("Compact() returned nil manifest")
	}
	if result == nil || result.ReclaimableBytes < uint64(len(replacement)) || len(result.CompactedSegments) == 0 {
		t.Fatalf("compaction result = %+v, want reclaimed overwrite bytes", result)
	}
	head := loadCommittedHead(t, ctx, heads, volumeID)
	retainedManifests := map[string]struct{}{head.ManifestKey: {}}
	materializer := s0fs.NewMaterializer(volumeID, store, heads)
	plan, err := materializer.PlanGarbageCollection(ctx, []*s0fs.SnapshotState{compacted.State}, retainedManifests)
	if err != nil {
		t.Fatalf("PlanGarbageCollection() error = %v", err)
	}
	if len(plan.Segments) == 0 {
		t.Fatalf("GC plan did not include obsolete segments after compaction")
	}
	gcResult, err := plan.Apply(ctx)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(gcResult.DeletedSegments) == 0 {
		t.Fatalf("GC deleted no segments: %+v", gcResult)
	}
	if compactedBytes := sumObjectSizes(t, store, "segments/"); compactedBytes >= fragmentedBytes {
		t.Fatalf("segment bytes after GC = %d, want less than fragmented bytes %d", compactedBytes, fragmentedBytes)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(compacted engine) error = %v", err)
	}

	engine = openIntegrationEngine(t, ctx, volumeID, store, heads, 1024)
	readBack, err := engine.Read(node.Inode, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read(after GC) error = %v", err)
	}
	if !bytes.Equal(readBack, payload) {
		t.Fatalf("payload mismatch after compaction and GC")
	}
}

func TestS0FSIntegrationSnapshotRetainsCompactedSegmentsUntilReleased(t *testing.T) {
	ctx := context.Background()
	volumeID := "vol-snapshot-retention"
	store := newIntegrationObjectStore(t, volumeID)
	heads := newIntegrationHeadStore()

	engine := openIntegrationEngine(t, ctx, volumeID, store, heads, 1024)
	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
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
	oldSegmentKey := onlySegmentKey(t, first.State)
	snapshotState, err := engine.CreateSnapshot("before-overwrite")
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}

	if _, err := engine.Write(node.Inode, 1, []byte("Z")); err != nil {
		t.Fatalf("Write(overwrite) error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(overwrite) error = %v", err)
	}
	compacted, _, err := engine.Compact(ctx, s0fs.CompactionOptions{
		SegmentTargetSize: 1024,
		Force:             true,
	})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if compacted == nil {
		t.Fatal("Compact() returned nil manifest")
	}

	materializer := s0fs.NewMaterializer(volumeID, store, heads)
	snapshotPayload, err := s0fs.NewSnapshotReader(snapshotState, materializer).Read(node.Inode, 0, 6)
	if err != nil {
		t.Fatalf("SnapshotReader.Read() error = %v", err)
	}
	if string(snapshotPayload) != "abcdef" {
		t.Fatalf("snapshot payload = %q, want abcdef", snapshotPayload)
	}

	head := loadCommittedHead(t, ctx, heads, volumeID)
	retainedManifests := map[string]struct{}{head.ManifestKey: {}}
	withSnapshot, err := materializer.PlanGarbageCollection(ctx, []*s0fs.SnapshotState{compacted.State, snapshotState}, retainedManifests)
	if err != nil {
		t.Fatalf("PlanGarbageCollection(with snapshot) error = %v", err)
	}
	if slices.Contains(withSnapshot.Segments, oldSegmentKey) {
		t.Fatalf("snapshot segment %s was planned for deletion while snapshot is retained", oldSegmentKey)
	}
	withoutSnapshot, err := materializer.PlanGarbageCollection(ctx, []*s0fs.SnapshotState{compacted.State}, retainedManifests)
	if err != nil {
		t.Fatalf("PlanGarbageCollection(without snapshot) error = %v", err)
	}
	if !slices.Contains(withoutSnapshot.Segments, oldSegmentKey) {
		t.Fatalf("snapshot segment %s was not collectible after snapshot release: %v", oldSegmentKey, withoutSnapshot.Segments)
	}
}

func newIntegrationObjectStore(t *testing.T, volumeID string) objectstore.Store {
	t.Helper()
	namespace := fmt.Sprintf("s0fs-integration-%s", strings.ReplaceAll(t.Name(), "/", "-"))
	base := objectstore.NewMemoryStore(namespace)
	return objectstore.Prefix(base, "sandboxvolumes/team-a/"+volumeID+"/s0fs")
}

func openIntegrationEngine(t *testing.T, ctx context.Context, volumeID string, store objectstore.Store, heads s0fs.HeadStore, segmentTargetSize uint64) *s0fs.Engine {
	t.Helper()

	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          volumeID,
		WALPath:           filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore:       store,
		HeadStore:         heads,
		SegmentTargetSize: segmentTargetSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})
	return engine
}

func loadCommittedHead(t *testing.T, ctx context.Context, heads s0fs.HeadStore, volumeID string) *s0fs.CommittedHead {
	t.Helper()

	head, err := heads.LoadCommittedHead(ctx, volumeID)
	if err != nil {
		t.Fatalf("LoadCommittedHead() error = %v", err)
	}
	if head.ManifestKey == "" {
		t.Fatal("committed head has empty manifest key")
	}
	return head
}

func onlySegmentKey(t *testing.T, state *s0fs.SnapshotState) string {
	t.Helper()

	var key string
	for _, segment := range state.Segments {
		if key != "" {
			t.Fatalf("state has more than one segment: %v", state.Segments)
		}
		key = segment.Key
	}
	if key == "" {
		t.Fatal("state has no segments")
	}
	return key
}

func listObjectKeys(t *testing.T, store objectstore.Store, prefix string) []string {
	t.Helper()

	infos := listObjectInfos(t, store, prefix)
	keys := make([]string, 0, len(infos))
	for _, info := range infos {
		keys = append(keys, info.Key)
	}
	return keys
}

func sumObjectSizes(t *testing.T, store objectstore.Store, prefix string) int64 {
	t.Helper()

	var total int64
	for _, info := range listObjectInfos(t, store, prefix) {
		total += info.Size
	}
	return total
}

func listObjectInfos(t *testing.T, store objectstore.Store, prefix string) []objectstore.Info {
	t.Helper()

	var (
		infos      []objectstore.Info
		startAfter string
		token      string
	)
	for {
		page, hasMore, nextToken, err := store.List(prefix, startAfter, token, "", 1000)
		if err != nil {
			t.Fatalf("List(%q) error = %v", prefix, err)
		}
		infos = append(infos, page...)
		if !hasMore {
			break
		}
		if len(page) > 0 {
			startAfter = page[len(page)-1].Key
		}
		token = nextToken
	}
	slices.SortFunc(infos, func(a, b objectstore.Info) int {
		return strings.Compare(a.Key, b.Key)
	})
	return infos
}

func deterministicPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}
