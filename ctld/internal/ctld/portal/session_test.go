package portal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestLocalVolumeManagerPrepareHandoffDrainsInflightAndBlocksNewAcquires(t *testing.T) {
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{VolumeID: "vol-1"})

	releaseExisting, err := mgr.AcquireDirectVolumeFileMount(context.Background(), "vol-1", nil)
	if err != nil {
		t.Fatalf("AcquireDirectVolumeFileMount(existing) error = %v", err)
	}

	prepared := make(chan error, 1)
	go func() {
		prepared <- mgr.prepareHandoff(context.Background(), "vol-1")
	}()

	select {
	case err := <-prepared:
		t.Fatalf("prepareHandoff() returned early: %v", err)
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
		t.Fatalf("AcquireDirectVolumeFileMount(new) returned during handoff: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseExisting()

	select {
	case err := <-prepared:
		if err != nil {
			t.Fatalf("prepareHandoff() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepareHandoff() did not complete after inflight request drained")
	}

	select {
	case err := <-acquired:
		t.Fatalf("AcquireDirectVolumeFileMount(new) returned before handoff abort: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	mgr.abortHandoff("vol-1")

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("AcquireDirectVolumeFileMount(new) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AcquireDirectVolumeFileMount(new) did not resume after handoff abort")
	}
}

func TestLocalVolumeManagerAcquireDirectMountMountsMissingVolume(t *testing.T) {
	mgr := newLocalVolumeManager()
	mountCalls := 0

	release, err := mgr.AcquireDirectVolumeFileMount(context.Background(), "vol-source", func(context.Context) (string, error) {
		mountCalls++
		mgr.add(&volume.VolumeContext{VolumeID: "vol-source"})
		return "local-vol-source", nil
	})
	if err != nil {
		t.Fatalf("AcquireDirectVolumeFileMount() error = %v", err)
	}
	defer release()
	if mountCalls != 1 {
		t.Fatalf("mountFn calls = %d, want 1", mountCalls)
	}
	if _, err := mgr.GetVolume("vol-source"); err != nil {
		t.Fatalf("GetVolume() error = %v", err)
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
