package s0fs

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestEngineSmallFileReadWriteReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()

	replayedNode, err := replayed.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup() after replay error = %v", err)
	}
	data, err := replayed.Read(replayedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read() after replay error = %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("replayed data = %q, want hello", data)
	}
}

func TestEngineRetainsUnlinkedInodeUntilDurableCollection(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{
		VolumeID:          "vol-1",
		WALPath:           walPath,
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "open.txt", 0o600)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Unlink(RootInode, "open.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	recovered, err := Open(context.Background(), Config{
		VolumeID:          "vol-1",
		WALPath:           walPath,
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	if _, err := recovered.GetAttr(node.Inode); err != nil {
		t.Fatalf("GetAttr(retained inode) error = %v", err)
	}
	if err := recovered.CollectUnlinked(); err != nil {
		t.Fatalf("CollectUnlinked() error = %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close(recovered) error = %v", err)
	}

	final, err := Open(context.Background(), Config{
		VolumeID:          "vol-1",
		WALPath:           walPath,
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(final) error = %v", err)
	}
	defer final.Close()
	if _, err := final.GetAttr(node.Inode); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetAttr(collected inode) error = %v, want %v", err, ErrNotFound)
	}
}

func TestEngineCollectUnlinkedAdvancesMaterializedHead(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID:          "vol-1",
		WALPath:           filepath.Join(t.TempDir(), "volume.wal"),
		ObjectStore:       newPrefixedRecordingStore(t, "vol-1"),
		HeadStore:         newMemoryHeadStore(),
		RetainAllUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(RootInode, "open.txt", 0o600)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if err := engine.Unlink(RootInode, "open.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	retained, err := engine.SyncMaterialize(context.Background())
	if err != nil {
		t.Fatalf("SyncMaterialize(retained) error = %v", err)
	}
	if err := engine.CollectUnlinked(); err != nil {
		t.Fatalf("CollectUnlinked() error = %v", err)
	}
	collected, err := engine.SyncMaterialize(context.Background())
	if err != nil {
		t.Fatalf("SyncMaterialize(collected) error = %v", err)
	}
	if retained == nil || collected == nil || collected.ManifestSeq <= retained.ManifestSeq {
		t.Fatalf("manifest seq retained=%v collected=%v, want advancing collection", retained, collected)
	}
	if collected.State.Nodes[node.Inode] != nil {
		t.Fatalf("collected manifest still contains inode %d", node.Inode)
	}
}

func TestEngineConcurrentFsyncCoalescesWALSync(t *testing.T) {
	var syncs atomic.Int64
	engine, err := Open(context.Background(), Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
		WALSyncHook: func() {
			syncs.Add(1)
		},
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

	const callers = 16
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			errs <- engine.Fsync(node.Inode)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Fsync() error = %v", err)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("WAL sync count = %d, want 1", got)
	}
}

func TestEngineSnapshotReferenceStateSharesInlinePayload(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID:    "vol-1",
		WALPath:     filepath.Join(t.TempDir(), "volume.wal"),
		ObjectStore: newPrefixedRecordingStore(t, "vol-1"),
		HeadStore:   newMemoryHeadStore(),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "payload.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var segmentID string
	for id, segment := range engine.segments {
		if len(segment.InlineData) > 0 {
			segmentID = id
			break
		}
	}
	if segmentID == "" {
		t.Fatal("engine did not retain an inline segment")
	}
	engineSegment := engine.segments[segmentID]

	full := engine.SnapshotState()
	fullSegment := full.Segments[segmentID]
	if fullSegment == nil || len(fullSegment.InlineData) == 0 {
		t.Fatalf("SnapshotState() missing inline segment %s", segmentID)
	}
	if &fullSegment.InlineData[0] == &engineSegment.InlineData[0] {
		t.Fatal("SnapshotState() shared inline payload with engine")
	}

	references := engine.SnapshotReferenceState()
	referenceSegment := references.Segments[segmentID]
	if referenceSegment == nil || len(referenceSegment.InlineData) == 0 {
		t.Fatalf("SnapshotReferenceState() missing inline segment %s", segmentID)
	}
	if &referenceSegment.InlineData[0] != &engineSegment.InlineData[0] {
		t.Fatal("SnapshotReferenceState() copied inline payload")
	}
}

func TestEngineWriteCopiesCallerBufferBeforeReturn(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	payload := []byte("stable")
	if _, err := engine.Write(node.Inode, 0, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	copy(payload, "mutate")
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte("stable")) {
		t.Fatalf("read data = %q, want stable", data)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	replayedNode, err := replayed.Lookup(RootInode, "data.txt")
	if err != nil {
		t.Fatalf("Lookup() after replay error = %v", err)
	}
	replayedData, err := replayed.Read(replayedNode.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() after replay error = %v", err)
	}
	if !bytes.Equal(replayedData, []byte("stable")) {
		t.Fatalf("replayed data = %q, want stable", replayedData)
	}
}

func TestEngineCreateFileWithOwnerReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFileWithOwner(RootInode, "owned.txt", 0o640, 1000, 2000)
	if err != nil {
		t.Fatalf("CreateFileWithOwner() error = %v", err)
	}
	if node.UID != 1000 || node.GID != 2000 {
		t.Fatalf("created node owner = %d:%d, want 1000:2000", node.UID, node.GID)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	replayedNode, err := replayed.Lookup(RootInode, "owned.txt")
	if err != nil {
		t.Fatalf("Lookup() after replay error = %v", err)
	}
	if replayedNode.Mode != 0o640 || replayedNode.UID != 1000 || replayedNode.GID != 2000 {
		t.Fatalf("replayed node = %+v, want mode 0640 owner 1000:2000", replayedNode)
	}
}

func TestEngineLocalDiskGuardRejectsProjectedCacheGrowth(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(context.Background(), Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(dir, "volume.wal"),
		LocalDiskGuard: &LocalDiskGuard{
			Path:     dir,
			MaxBytes: 2048,
		},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "limited.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("ok")); err != nil {
		t.Fatalf("small Write() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, bytes.Repeat([]byte("x"), 2048)); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("large Write() error = %v, want ErrNoSpace", err)
	}
}

func TestEngineWriteExtendsWithZeros(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
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
	data, err := engine.Read(node.Inode, 0, 8)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte{0, 0, 0, 0, 'x'}) {
		t.Fatalf("sparse data = %#v", data)
	}
}

func TestEngineReadIntoSmallFile(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	buf := bytes.Repeat([]byte{0xff}, 8)
	n, err := engine.ReadInto(node.Inode, 2, buf[:3])
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadInto() n = %d, want 3", n)
	}
	if !bytes.Equal(buf[:3], []byte("cde")) {
		t.Fatalf("ReadInto() data = %q, want cde", buf[:3])
	}
	if !bytes.Equal(buf[3:], bytes.Repeat([]byte{0xff}, 5)) {
		t.Fatalf("ReadInto() modified bytes past destination: %#v", buf)
	}
}

func TestEngineRenameAndUnlinkReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "before.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Rename(RootInode, "before.txt", RootInode, "after.txt"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	replayedNode, err := replayed.Lookup(RootInode, "after.txt")
	if err != nil {
		t.Fatalf("Lookup(after) error = %v", err)
	}
	if data, err := replayed.Read(replayedNode.Inode, 0, 1024); err != nil || !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(after) = %q, %v", data, err)
	}
	if err := replayed.Unlink(RootInode, "after.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if err := replayed.Fsync(replayedNode.Inode); err != nil {
		t.Fatalf("Fsync() after unlink error = %v", err)
	}
	if err := replayed.Close(); err != nil {
		t.Fatalf("Close(replayed) error = %v", err)
	}

	replayedAgain, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay again) error = %v", err)
	}
	defer replayedAgain.Close()
	if _, err := replayedAgain.Lookup(RootInode, "after.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(after) err = %v, want ErrNotFound", err)
	}
}

func TestEngineRejectsDuplicateDentry(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); !errors.Is(err, ErrExists) {
		t.Fatalf("CreateFile(duplicate) err = %v, want ErrExists", err)
	}
}

func TestEngineLinkReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "source.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	linked, err := engine.Link(node.Inode, RootInode, "linked.txt")
	if err != nil {
		t.Fatalf("Link() error = %v", err)
	}
	if linked.Inode != node.Inode || linked.Nlink != 2 {
		t.Fatalf("Link() node = %#v, want same inode with nlink 2", linked)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	replayedNode, err := replayed.Lookup(RootInode, "linked.txt")
	if err != nil {
		t.Fatalf("Lookup(linked) error = %v", err)
	}
	if replayedNode.Inode != node.Inode || replayedNode.Nlink != 2 {
		t.Fatalf("Lookup(linked) node = %#v, want same inode with nlink 2", replayedNode)
	}
	data, err := replayed.Read(replayedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read(linked) error = %v", err)
	}
	if !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(linked) data = %q, want payload", data)
	}
}

func TestEngineDirectoryOperationsReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	dir, err := engine.Mkdir(RootInode, "dir", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if _, err := engine.CreateFile(dir.Inode, "child.txt", 0o644); err != nil {
		t.Fatalf("CreateFile(child) error = %v", err)
	}
	entries, err := engine.ReadDir(dir.Inode)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "child.txt" || entries[0].Type != TypeFile {
		t.Fatalf("ReadDir() entries = %+v", entries)
	}
	if err := engine.RemoveDir(RootInode, "dir"); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("RemoveDir(non-empty) err = %v, want ErrNotEmpty", err)
	}
	if err := engine.Unlink(dir.Inode, "child.txt"); err != nil {
		t.Fatalf("Unlink(child) error = %v", err)
	}
	if err := engine.RemoveDir(RootInode, "dir"); err != nil {
		t.Fatalf("RemoveDir() error = %v", err)
	}
	if err := engine.Fsync(RootInode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	if _, err := replayed.Lookup(RootInode, "dir"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(dir) err = %v, want ErrNotFound", err)
	}
}

func TestEngineMetadataMutationReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	node, err := engine.CreateFile(RootInode, "meta.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Truncate(node.Inode, 3); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if err := engine.SetMode(node.Inode, 0o600); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}
	if err := engine.SetOwner(node.Inode, 1000, 1001); err != nil {
		t.Fatalf("SetOwner() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	attr, err := replayed.Lookup(RootInode, "meta.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if attr.Mode != 0o600 || attr.UID != 1000 || attr.GID != 1001 || attr.Size != 3 {
		t.Fatalf("attr after replay = %+v", attr)
	}
	data, err := replayed.Read(attr.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte("abc")) {
		t.Fatalf("data after replay = %q, want abc", data)
	}
}

func TestEngineUnlinkThenForget(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "open.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	unlinkedInode, err := engine.UnlinkWithInode(RootInode, "open.txt")
	if err != nil {
		t.Fatalf("UnlinkWithInode() error = %v", err)
	}
	if unlinkedInode != node.Inode {
		t.Fatalf("UnlinkWithInode() inode = %d, want %d", unlinkedInode, node.Inode)
	}
	if _, err := engine.Lookup(RootInode, "open.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup() after unlink err = %v, want ErrNotFound", err)
	}
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read(unlinked inode) error = %v", err)
	}
	if !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(unlinked inode) = %q, want payload", data)
	}
	if err := engine.Forget(node.Inode); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, err := engine.GetAttr(node.Inode); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetAttr() after forget err = %v, want ErrNotFound", err)
	}
}
