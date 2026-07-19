package portal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type rejectingHeadStore struct{}

func (rejectingHeadStore) LoadCommittedHead(context.Context, string) (*s0fs.CommittedHead, error) {
	return nil, s0fs.ErrCommittedHeadNotFound
}

func (rejectingHeadStore) CompareAndSwapCommittedHead(context.Context, string, uint64, *s0fs.CommittedHead) error {
	return s0fs.ErrCommittedHeadConflict
}

func newDirtyConflictS0FSEngine(t *testing.T, volumeID string) (*s0fs.Engine, string) {
	t.Helper()
	cacheDir := filepath.Join(t.TempDir(), volumeID+"-cache")
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:    volumeID,
		WALPath:     filepath.Join(cacheDir, "engine.wal"),
		ObjectStore: objectstore.NewMemoryStore(t.Name() + "-" + volumeID),
		HeadStore:   rejectingHeadStore{},
	})
	if err != nil {
		t.Fatalf("Open(%s) error = %v", volumeID, err)
	}
	node, err := engine.CreateFile(s0fs.RootInode, "dirty.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile(%s) error = %v", volumeID, err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("dirty")); err != nil {
		t.Fatalf("Write(%s) error = %v", volumeID, err)
	}
	return engine, cacheDir
}

func TestLocalSessionReadIntoUsesMountedS0FS(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)
	openResp, err := session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	buf := bytes.Repeat([]byte{0xff}, 8)
	n, eof, err := session.ReadInto(context.Background(), &pb.ReadRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		HandleId: openResp.HandleId,
		Offset:   1,
		Size:     3,
	}, buf)
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadInto() n = %d, want 3", n)
	}
	if eof {
		t.Fatal("ReadInto() eof = true, want false")
	}
	if !bytes.Equal(buf[:3], []byte("bcd")) {
		t.Fatalf("ReadInto() data = %q, want bcd", buf[:3])
	}
	if !bytes.Equal(buf[3:], bytes.Repeat([]byte{0xff}, 5)) {
		t.Fatalf("ReadInto() modified bytes past requested size: %#v", buf)
	}
}

func TestLocalSessionRmdirPreservesENOTEMPTY(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-errno",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	dir, err := engine.Mkdir(s0fs.RootInode, "non-empty", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if _, err := engine.CreateFile(dir.Inode, "child", 0o644); err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-errno",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-errno", mgr, nil)

	_, err = session.Rmdir(context.Background(), &pb.RmdirRequest{
		Parent: s0fs.RootInode,
		Name:   "non-empty",
	})
	if !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir(non-empty) error = %v, want ENOTEMPTY", err)
	}
}

func TestLocalSessionReadIntoRequiresTrackedHandleForCache(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("engine")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)
	session.storeCompleteReadCache(volCtx, node.Inode, []byte("cached"))

	buf := make([]byte, 16)
	n, eof, err := session.ReadInto(context.Background(), &pb.ReadRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Size:     int64(len(buf)),
	}, buf)
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if !eof {
		t.Fatal("ReadInto() eof = false, want true")
	}
	if got := string(buf[:n]); got != "engine" {
		t.Fatalf("ReadInto() without tracked handle = %q, want engine", got)
	}
}

func TestLocalVolumeManagerPrepareSnapshotCheckpointDrainsInflightAndBlocksNewAcquires(t *testing.T) {
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{VolumeID: "vol-1"})

	releaseExisting, err := mgr.AcquireDirectVolumeFileMount(context.Background(), "vol-1", nil)
	if err != nil {
		t.Fatalf("AcquireDirectVolumeFileMount(existing) error = %v", err)
	}

	prepared := make(chan error, 1)
	go func() {
		prepared <- mgr.prepareSnapshotCheckpoint(context.Background(), "vol-1")
	}()

	select {
	case err := <-prepared:
		t.Fatalf("prepareSnapshotCheckpoint() returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	acquired := make(chan error, 1)
	go func() {
		release, err := mgr.AcquireDirectVolumeFileMount(context.Background(), "vol-1", nil)
		if err == nil && release != nil {
			release()
		}
		acquired <- err
	}()

	select {
	case err := <-acquired:
		t.Fatalf("AcquireDirectVolumeFileMount(new) returned during snapshot checkpoint: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseExisting()

	select {
	case err := <-prepared:
		if err != nil {
			t.Fatalf("prepareSnapshotCheckpoint() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepareSnapshotCheckpoint() did not complete after inflight request drained")
	}

	select {
	case err := <-acquired:
		t.Fatalf("AcquireDirectVolumeFileMount(new) returned before snapshot checkpoint completion: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	mgr.completeSnapshotCheckpoint("vol-1")

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("AcquireDirectVolumeFileMount(new) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AcquireDirectVolumeFileMount(new) did not resume after snapshot checkpoint completion")
	}
}

func TestLocalSessionMutationsWaitForSnapshotCheckpoint(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)

	if err := mgr.prepareSnapshotCheckpoint(context.Background(), "vol-1"); err != nil {
		t.Fatalf("prepareSnapshotCheckpoint() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := session.Create(context.Background(), &pb.CreateRequest{
			VolumeId: "ignored-by-local-session",
			Parent:   s0fs.RootInode,
			Name:     "blocked.txt",
			Mode:     0o644,
		})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Create() returned during checkpoint: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	mgr.completeSnapshotCheckpoint("vol-1")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create() did not resume after checkpoint completed")
	}
}

func TestLocalSessionReadCacheTracksSmallWrites(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)

	if _, err := session.Write(context.Background(), &pb.WriteRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Data:     []byte("cached"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := string(session.readCache[localReadCacheKey("vol-1", node.Inode)]); got != "cached" {
		t.Fatalf("read cache = %q, want cached", got)
	}

	openResp, err := session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	buf := make([]byte, 16)
	n, eof, err := session.ReadInto(context.Background(), &pb.ReadRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		HandleId: openResp.HandleId,
		Size:     int64(len(buf)),
	}, buf)
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if !eof {
		t.Fatal("ReadInto() eof = false, want true")
	}
	if got := string(buf[:n]); got != "cached" {
		t.Fatalf("ReadInto() = %q, want cached", got)
	}
}

func TestLocalSessionReleaseSyncsDirtyWrites(t *testing.T) {
	counter := &walSyncCounter{}
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:    "vol-1",
		WALPath:     filepath.Join(t.TempDir(), "volume.wal"),
		WALSyncHook: counter.Hook,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)

	createResp, err := session.Create(context.Background(), &pb.CreateRequest{
		VolumeId: "ignored-by-local-session",
		Parent:   s0fs.RootInode,
		Name:     "data.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := session.Write(context.Background(), &pb.WriteRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
		Data:     []byte("persist"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := counter.Load(); got != 0 {
		t.Fatalf("sync count after Write() = %d, want 0", got)
	}
	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if got := counter.Load(); got != 1 {
		t.Fatalf("sync count after Release() = %d, want 1", got)
	}
}

func TestLocalVolumeManagerUnmountRemovesCacheDir(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "volume-cache")
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(cacheDir, "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	})

	if err := mgr.UnmountVolume(context.Background(), "vol-1", ""); err != nil {
		t.Fatalf("UnmountVolume() error = %v", err)
	}
	if _, err := os.Stat(cacheDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache dir stat error = %v, want not exist", err)
	}
}

func TestLocalVolumeManagerUnmountKeepsVolumeOnMaterializeFailure(t *testing.T) {
	engine, cacheDir := newDirtyConflictS0FSEngine(t, "vol-1")
	defer engine.Close()

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	})

	if err := mgr.UnmountVolume(context.Background(), "vol-1", ""); !errors.Is(err, s0fs.ErrCommittedHeadConflict) {
		t.Fatalf("UnmountVolume() error = %v, want %v", err, s0fs.ErrCommittedHeadConflict)
	}
	if _, err := mgr.GetVolume("vol-1"); err != nil {
		t.Fatalf("GetVolume() after failed unmount error = %v, want mounted volume to remain", err)
	}
	if _, err := os.Stat(cacheDir); err != nil {
		t.Fatalf("cache dir stat after failed unmount error = %v, want cache to remain", err)
	}
}

func TestLocalSessionReadCacheResizesOnTruncate(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)

	if _, err := session.Write(context.Background(), &pb.WriteRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Data:     []byte("abcdef"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := session.SetAttr(context.Background(), &pb.SetAttrRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Valid:    uint32(fsmeta.SetAttrSize),
		Attr:     &pb.GetAttrResponse{Size: 3},
	}); err != nil {
		t.Fatalf("SetAttr() error = %v", err)
	}
	if got := string(session.readCache[localReadCacheKey("vol-1", node.Inode)]); got != "abc" {
		t.Fatalf("read cache after truncate = %q, want abc", got)
	}
}

type walSyncCounter struct {
	count atomic.Int64
}

func (c *walSyncCounter) Hook() {
	c.count.Add(1)
}

func (c *walSyncCounter) Load() int64 {
	return c.count.Load()
}

func TestLocalSessionReadCacheDisabledForRWX(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWX,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)

	if _, err := session.Write(context.Background(), &pb.WriteRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Data:     []byte("uncached"),
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if len(session.readCache) != 0 {
		t.Fatalf("read cache entries = %d, want 0 for RWX volume", len(session.readCache))
	}
}

func TestLocalSessionOpenUsesMountedS0FS(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	volCtx := &volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	}
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)

	resp, err := session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if resp.HandleId == 0 {
		t.Fatal("Open() handle = 0, want non-zero")
	}
	if got, ok := volCtx.HandleInode(resp.HandleId); !ok || got != node.Inode {
		t.Fatalf("HandleInode() = %d, %v; want %d, true", got, ok, node.Inode)
	}
	if len(session.readOnlyHandles) != 1 {
		t.Fatalf("read-only handle count = %d, want 1", len(session.readOnlyHandles))
	}
	if _, err := session.Unlink(context.Background(), &pb.UnlinkRequest{
		VolumeId: "ignored-by-local-session",
		Parent:   s0fs.RootInode,
		Name:     "data.txt",
	}); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		HandleId: resp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, ok := volCtx.HandleInode(resp.HandleId); ok {
		t.Fatal("HandleInode() still tracks read-only handle after Release")
	}
	if len(session.readOnlyHandles) != 0 {
		t.Fatalf("read-only handle count after Release = %d, want 0", len(session.readOnlyHandles))
	}
	if _, err := engine.GetAttr(node.Inode); !errors.Is(err, s0fs.ErrNotFound) {
		t.Fatalf("GetAttr() after unlinked read-only Release err = %v, want ErrNotFound", err)
	}
}

func TestLocalSessionOpenUsesFSServerPermissions(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(s0fs.RootInode, "private.txt", 0o600)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if err := engine.SetOwner(node.Inode, 0, 0); err != nil {
		t.Fatalf("SetOwner() error = %v", err)
	}

	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  "vol-1",
		TeamID:    "team-a",
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: 1,
		RootPath:  "/",
	})
	session := newLocalSession("vol-1", mgr, nil)

	_, err = session.Open(context.Background(), &pb.OpenRequest{
		VolumeId: "ignored-by-local-session",
		Inode:    node.Inode,
		Actor:    &pb.PosixActor{Uid: 1000, Gids: []uint32{1000}},
	})
	if fserror.CodeOf(err) != fserror.PermissionDenied {
		t.Fatalf("Open() code = %v, want %v (err=%v)", fserror.CodeOf(err), fserror.PermissionDenied, err)
	}
	if len(session.readOnlyHandles) != 0 {
		t.Fatalf("read-only handle count = %d, want 0 after denied open", len(session.readOnlyHandles))
	}
}

func TestLocalSessionRestoresOpenUnlinkedS0FSHandle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "engine.wal")
	statePath := filepath.Join(dir, "handles.json")
	firstEngine, err := s0fs.Open(ctx, s0fs.Config{VolumeID: "vol-1", WALPath: walPath, RetainUnlinked: true})
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	firstManager := newLocalVolumeManager()
	firstVolume := &volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: firstEngine, Access: volume.AccessModeRWO, RootInode: 1, RootPath: "/", CacheDir: dir,
	}
	firstManager.add(firstVolume)
	firstSession := newLocalSession("vol-1", firstManager, nil)
	firstSession.statePath = statePath
	created, err := firstSession.Create(ctx, &pb.CreateRequest{Parent: s0fs.RootInode, Name: "transient.txt", Mode: 0o644})
	if err != nil {
		t.Fatalf("Create(transient.txt) error = %v", err)
	}
	if _, err := firstSession.Write(ctx, &pb.WriteRequest{Inode: created.Inode, HandleId: created.HandleId, Data: []byte("survives")}); err != nil {
		t.Fatalf("Write(transient.txt) error = %v", err)
	}
	if _, err := firstSession.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "transient.txt"}); err != nil {
		t.Fatalf("Unlink(transient.txt) error = %v", err)
	}
	if err := firstSession.Handoff(); err != nil {
		t.Fatalf("Handoff() error = %v", err)
	}
	if err := firstEngine.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	handleState, err := loadS0FSHandleState(statePath, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	secondEngine, err := s0fs.Open(ctx, s0fs.Config{VolumeID: "vol-1", WALPath: walPath, RetainUnlinked: true})
	if err != nil {
		t.Fatalf("Open(second) error = %v", err)
	}
	defer secondEngine.Close()
	secondVolume := &volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: secondEngine, Access: volume.AccessModeRWO, RootInode: 1, RootPath: "/", CacheDir: dir,
	}
	secondVolume.RestoreHandleState(handleState)
	secondEngine.PruneUnlinked(retainedUnlinkedInodes(handleState))
	secondManager := newLocalVolumeManager()
	secondManager.add(secondVolume)
	secondSession := newLocalSession("vol-1", secondManager, nil)
	secondSession.statePath = statePath
	read, err := secondSession.Read(ctx, &pb.ReadRequest{Inode: created.Inode, HandleId: created.HandleId, Size: 64})
	if err != nil {
		t.Fatalf("Read(restored handle) error = %v", err)
	}
	if string(read.Data) != "survives" {
		t.Fatalf("Read(restored handle) = %q, want survives", string(read.Data))
	}
	if _, err := secondSession.Release(ctx, &pb.ReleaseRequest{Inode: created.Inode, HandleId: created.HandleId}); err != nil {
		t.Fatalf("Release(restored handle) error = %v", err)
	}
	if _, err := secondEngine.GetAttr(created.Inode); !errors.Is(err, s0fs.ErrNotFound) {
		t.Fatalf("GetAttr(released unlinked inode) error = %v, want ErrNotFound", err)
	}
}

func TestLocalSessionPersistsFileHandlesIncrementallyAndSkipsDirectories(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(s0fs.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: engine, Access: volume.AccessModeRWO, RootInode: fsmeta.Ino(s0fs.RootInode), RootPath: "/",
	})
	session := newLocalSession("vol-1", mgr, nil)
	session.statePath = filepath.Join(t.TempDir(), "handles.json")
	defer session.Close()

	opened, err := session.Open(context.Background(), &pb.OpenRequest{Inode: node.Inode})
	if err != nil {
		t.Fatalf("Open(file) error = %v", err)
	}
	if _, err := os.Stat(session.statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot Stat() error = %v, want not exist before compaction", err)
	}
	journalInfo, err := os.Stat(s0fsHandleJournalPath(session.statePath))
	if err != nil {
		t.Fatalf("journal Stat() error = %v", err)
	}
	state, err := loadS0FSHandleState(session.statePath, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if got := state.FileHandles[opened.HandleId]; got != node.Inode {
		t.Fatalf("replayed handle inode = %d, want %d", got, node.Inode)
	}

	dir, err := session.OpenDir(context.Background(), &pb.OpenDirRequest{Inode: s0fs.RootInode})
	if err != nil {
		t.Fatalf("OpenDir() error = %v", err)
	}
	if _, err := session.ReleaseDir(context.Background(), &pb.ReleaseDirRequest{
		Inode: s0fs.RootInode, HandleId: dir.HandleId,
	}); err != nil {
		t.Fatalf("ReleaseDir() error = %v", err)
	}
	afterDirInfo, err := os.Stat(s0fsHandleJournalPath(session.statePath))
	if err != nil {
		t.Fatalf("journal Stat(after directory operations) error = %v", err)
	}
	if afterDirInfo.Size() != journalInfo.Size() {
		t.Fatalf("journal size after directory operations = %d, want %d", afterDirInfo.Size(), journalInfo.Size())
	}

	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		Inode: node.Inode, HandleId: opened.HandleId,
	}); err != nil {
		t.Fatalf("Release(file) error = %v", err)
	}
	state, err = loadS0FSHandleState(session.statePath, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState(after release) error = %v", err)
	}
	if len(state.FileHandles) != 0 {
		t.Fatalf("replayed file handles after release = %#v, want empty", state.FileHandles)
	}
}

func TestLocalSessionFallsBackToSnapshotForLegacyStandby(t *testing.T) {
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(t.TempDir(), "engine.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(s0fs.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: engine, Access: volume.AccessModeRWO, RootInode: fsmeta.Ino(s0fs.RootInode), RootPath: "/",
	})
	session := newLocalSession("vol-1", mgr, nil)
	session.statePath = filepath.Join(t.TempDir(), "handles.json")
	session.incrementalReady = func() bool { return false }
	defer session.Close()

	opened, err := session.Open(context.Background(), &pb.OpenRequest{Inode: node.Inode})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	state, err := loadS0FSHandleSnapshot(session.statePath, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleSnapshot() error = %v", err)
	}
	if got := state.FileHandles[opened.HandleId]; got != node.Inode {
		t.Fatalf("snapshot handle inode = %d, want %d", got, node.Inode)
	}
	if _, err := os.Stat(s0fsHandleJournalPath(session.statePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy fallback journal Stat() error = %v, want not exist", err)
	}
}

func TestLocalSessionRestoresOpenUnlinkedS0FSHandleAfterProcessKill(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalSessionS0FSProcessKillHelper$")
	cmd.Env = append(os.Environ(), "S0FS_PROCESS_KILL_DIR="+dir)
	output, err := cmd.CombinedOutput()
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("process-kill helper error = %v, want signal exit\n%s", err, output)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("process-kill helper status = %v, want SIGKILL\n%s", exitErr.Sys(), output)
	}

	statePath := filepath.Join(dir, "handles.json")
	handleState, err := loadS0FSHandleState(statePath, "vol-1")
	if err != nil {
		t.Fatalf("loadS0FSHandleState() error = %v", err)
	}
	if len(handleState.FileHandles) != 1 || len(handleState.UnlinkedFiles) != 1 {
		t.Fatalf("recovered handle state = %#v, want one journal-replayed handle for an open-unlinked file", handleState)
	}
	handleIDs := make([]uint64, 0, len(handleState.FileHandles))
	var inode uint64
	for handleID, handleInode := range handleState.FileHandles {
		if inode != 0 && inode != handleInode {
			t.Fatalf("recovered handles reference different inodes: %#v", handleState.FileHandles)
		}
		inode = handleInode
		handleIDs = append(handleIDs, handleID)
	}
	sort.Slice(handleIDs, func(i, j int) bool { return handleIDs[i] < handleIDs[j] })

	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:       "vol-1",
		WALPath:        filepath.Join(dir, "engine.wal"),
		RetainUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(recovered) error = %v", err)
	}
	defer engine.Close()
	volCtx := &volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: engine, Access: volume.AccessModeRWO, RootInode: 1, RootPath: "/", CacheDir: dir,
	}
	volCtx.RestoreHandleState(handleState)
	engine.PruneUnlinked(retainedUnlinkedInodes(handleState))
	mgr := newLocalVolumeManager()
	mgr.add(volCtx)
	session := newLocalSession("vol-1", mgr, nil)
	session.statePath = statePath

	read, err := session.Read(context.Background(), &pb.ReadRequest{
		Inode: inode, HandleId: handleIDs[0], Size: 64,
	})
	if err != nil {
		t.Fatalf("Read(recovered handle) error = %v", err)
	}
	if string(read.Data) != "survives" {
		t.Fatalf("Read(recovered handle) = %q, want survives", read.Data)
	}
	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		Inode: inode, HandleId: handleIDs[0],
	}); err != nil {
		t.Fatalf("Release(final recovered handle) error = %v", err)
	}
	if _, err := engine.GetAttr(inode); !errors.Is(err, s0fs.ErrNotFound) {
		t.Fatalf("GetAttr(released unlinked inode) error = %v, want ErrNotFound", err)
	}
}

func TestLocalSessionS0FSProcessKillHelper(t *testing.T) {
	dir := os.Getenv("S0FS_PROCESS_KILL_DIR")
	if dir == "" {
		t.Skip("process-kill helper")
	}
	engine, err := s0fs.Open(context.Background(), s0fs.Config{
		VolumeID:       "vol-1",
		WALPath:        filepath.Join(dir, "engine.wal"),
		RetainUnlinked: true,
	})
	if err != nil {
		t.Fatalf("Open(primary) error = %v", err)
	}
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID: "vol-1", TeamID: "team-a", Backend: volume.BackendS0FS,
		S0FS: engine, Access: volume.AccessModeRWO, RootInode: 1, RootPath: "/", CacheDir: dir,
	})
	session := newLocalSession("vol-1", mgr, nil)
	session.statePath = filepath.Join(dir, "handles.json")
	created, err := session.Create(context.Background(), &pb.CreateRequest{
		Parent: s0fs.RootInode, Name: "transient.txt", Mode: 0o644,
	})
	if err != nil {
		t.Fatalf("Create(transient.txt) error = %v", err)
	}
	if _, err := session.Write(context.Background(), &pb.WriteRequest{
		Inode: created.Inode, HandleId: created.HandleId, Data: []byte("survives"),
	}); err != nil {
		t.Fatalf("Write(transient.txt) error = %v", err)
	}
	if _, err := session.Open(context.Background(), &pb.OpenRequest{
		Inode: created.Inode, Flags: syscall.O_RDONLY,
	}); err != nil {
		t.Fatalf("Open(second handle) error = %v", err)
	}
	if _, err := session.Unlink(context.Background(), &pb.UnlinkRequest{
		Parent: s0fs.RootInode, Name: "transient.txt",
	}); err != nil {
		t.Fatalf("Unlink(transient.txt) error = %v", err)
	}
	if _, err := session.Release(context.Background(), &pb.ReleaseRequest{
		Inode: created.Inode, HandleId: created.HandleId,
	}); err != nil {
		t.Fatalf("Release(first handle) error = %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		t.Fatalf("Kill(self) error = %v", err)
	}
	t.Fatal("process survived SIGKILL")
}

func TestLocalVolumeManagerHandoffPreservesS0FSCache(t *testing.T) {
	cacheDir := t.TempDir()
	walPath := filepath.Join(cacheDir, "engine.wal")
	engine, err := s0fs.Open(context.Background(), s0fs.Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{VolumeID: "vol-1", S0FS: engine, CacheDir: cacheDir})

	if err := mgr.HandoffVolume("vol-1"); err != nil {
		t.Fatalf("HandoffVolume() error = %v", err)
	}
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("handoff removed recovery WAL: %v", err)
	}
	if _, err := mgr.GetVolume("vol-1"); err == nil {
		t.Fatal("handoff kept volume registered in old manager")
	}
}
