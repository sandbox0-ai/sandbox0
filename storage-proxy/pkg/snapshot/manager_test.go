package snapshot

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func newTestManager(repo *fakeRepo, volMgr volumeProvider) *Manager {
	metaClient := newFakeMeta()
	return &Manager{
		repo:       repo,
		volMgr:     volMgr,
		config:     &config.StorageProxyConfig{DefaultClusterId: "test-cluster"},
		logger:     logrus.New(),
		clusterID:  "test-cluster",
		podID:      "test-pod",
		locks:      make(map[string]time.Time),
		metaClient: metaClient, // Independent meta client for testing
	}
}

type fakeRepo struct {
	volumes   map[string]*db.SandboxVolume
	snapshots map[string]*db.Snapshot
	deleted   []string
	deleteErr error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		volumes:   make(map[string]*db.SandboxVolume),
		snapshots: make(map[string]*db.Snapshot),
	}
}

func (r *fakeRepo) GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error) {
	v, ok := r.volumes[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return v, nil
}

func (r *fakeRepo) ListSnapshotsByVolume(ctx context.Context, volumeID string) ([]*db.Snapshot, error) {
	var snaps []*db.Snapshot
	for _, s := range r.snapshots {
		if s.VolumeID == volumeID {
			snaps = append(snaps, s)
		}
	}
	return snaps, nil
}

func (r *fakeRepo) GetSnapshot(ctx context.Context, id string) (*db.Snapshot, error) {
	s, ok := r.snapshots[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return s, nil
}

func (r *fakeRepo) CreateSnapshot(ctx context.Context, snapshot *db.Snapshot) error {
	if snapshot != nil {
		r.snapshots[snapshot.ID] = snapshot
	}
	return nil
}

func (r *fakeRepo) DeleteSnapshot(ctx context.Context, id string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	if _, ok := r.snapshots[id]; !ok {
		return db.ErrNotFound
	}
	delete(r.snapshots, id)
	r.deleted = append(r.deleted, id)
	return nil
}

// Transaction support methods for fakeRepo
func (r *fakeRepo) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	// For testing, we just execute the function without a real transaction
	return fn(nil)
}

func (r *fakeRepo) GetSandboxVolumeForUpdate(ctx context.Context, tx pgx.Tx, id string) (*db.SandboxVolume, error) {
	return r.GetSandboxVolume(ctx, id)
}

func (r *fakeRepo) CreateSnapshotTx(ctx context.Context, tx pgx.Tx, snapshot *db.Snapshot) error {
	return r.CreateSnapshot(ctx, snapshot)
}

func (r *fakeRepo) GetSnapshotForUpdate(ctx context.Context, tx pgx.Tx, id string) (*db.Snapshot, error) {
	return r.GetSnapshot(ctx, id)
}

func (r *fakeRepo) DeleteSnapshotTx(ctx context.Context, tx pgx.Tx, id string) error {
	return r.DeleteSnapshot(ctx, id)
}

type fakeVolumeProvider struct {
	ctx *volume.VolumeContext
	err error
}

func (f *fakeVolumeProvider) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.ctx, nil
}

type fakeMeta struct {
	mu           sync.Mutex
	pathToIno    map[string]meta.Ino
	inoToPath    map[meta.Ino]string
	nextIno      meta.Ino
	removedPaths []string
}

func newFakeMeta() *fakeMeta {
	f := &fakeMeta{
		pathToIno: make(map[string]meta.Ino),
		inoToPath: make(map[meta.Ino]string),
		nextIno:   meta.RootInode + 1,
	}
	f.pathToIno["/"] = meta.RootInode
	f.inoToPath[meta.RootInode] = "/"
	return f
}

func (f *fakeMeta) Lookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino, attr *meta.Attr, check bool) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	parentPath, ok := f.inoToPath[parent]
	if !ok {
		return syscall.ENOENT
	}
	var path string
	if parentPath == "/" {
		path = "/" + name
	} else {
		path = filepath.Join(parentPath, name)
	}
	ino, ok := f.pathToIno[path]
	if !ok {
		return syscall.ENOENT
	}
	if inode != nil {
		*inode = ino
	}
	return 0
}

func (f *fakeMeta) Mkdir(ctx meta.Context, parent meta.Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	parentPath, ok := f.inoToPath[parent]
	if !ok {
		return syscall.ENOENT
	}
	var path string
	if parentPath == "/" {
		path = "/" + name
	} else {
		path = filepath.Join(parentPath, name)
	}
	if _, exists := f.pathToIno[path]; !exists {
		ino := f.nextIno
		f.nextIno++
		f.pathToIno[path] = ino
		f.inoToPath[ino] = path
		if inode != nil {
			*inode = ino
		}
	}
	return 0
}

func (f *fakeMeta) Clone(ctx meta.Context, srcParentIno, srcIno, parentIno meta.Ino, name string, cmode uint8, cumask uint16, count *uint64, total *uint64) syscall.Errno {
	return 0
}

func (f *fakeMeta) Rename(ctx meta.Context, parentSrc meta.Ino, nameSrc string, parentDst meta.Ino, nameDst string, flags uint32, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	return 0
}

func (f *fakeMeta) Remove(ctx meta.Context, parent meta.Ino, name string, skipTrash bool, numThreads int, count *uint64) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	parentPath, ok := f.inoToPath[parent]
	if !ok {
		return syscall.ENOENT
	}
	var path string
	if parentPath == "/" {
		path = "/" + name
	} else {
		path = filepath.Join(parentPath, name)
	}
	if _, exists := f.pathToIno[path]; !exists {
		return syscall.ENOENT
	}
	delete(f.pathToIno, path)
	f.removedPaths = append(f.removedPaths, path)
	return 0
}

func (f *fakeMeta) ensurePath(path string) meta.Ino {
	f.mu.Lock()
	defer f.mu.Unlock()
	clean := "/" + strings.Trim(path, "/")
	if clean == "/" {
		return meta.RootInode
	}
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	current := "/"
	for _, part := range parts {
		var next string
		if current == "/" {
			next = "/" + part
		} else {
			next = filepath.Join(current, part)
		}
		if _, ok := f.pathToIno[next]; !ok {
			ino := f.nextIno
			f.nextIno++
			f.pathToIno[next] = ino
			f.inoToPath[ino] = next
		}
		current = next
	}
	return f.pathToIno[current]
}

func TestListSnapshots_VolumeNotFound(t *testing.T) {
	repo := newFakeRepo()
	mgr := newTestManager(repo, nil)
	if _, err := mgr.ListSnapshots(context.Background(), "missing", "team"); !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("expected ErrVolumeNotFound, got %v", err)
	}
}

func TestListSnapshots_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1"}
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	mgr := newTestManager(repo, nil)
	snapshots, err := mgr.ListSnapshots(context.Background(), "vol1", "team1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].ID != "snap1" {
		t.Fatalf("unexpected snapshots: %v", snapshots)
	}
}

func TestGetSnapshot_NotFound(t *testing.T) {
	repo := newFakeRepo()
	mgr := newTestManager(repo, nil)
	if _, err := mgr.GetSnapshot(context.Background(), "vol1", "snap1", "team"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

func TestGetSnapshot_Mismatch(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	mgr := newTestManager(repo, nil)
	if _, err := mgr.GetSnapshot(context.Background(), "vol2", "snap1", "team1"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

func TestGetSnapshot_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	mgr := newTestManager(repo, nil)
	snapshot, err := mgr.GetSnapshot(context.Background(), "vol1", "snap1", "team1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snapshot.ID != "snap1" {
		t.Fatalf("unexpected snapshot: %v", snapshot)
	}
}

func TestDeleteSnapshot_VolumeNotMounted(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{err: errors.New("not mounted")}
	mgr := newTestManager(repo, volMgr)
	if err := mgr.DeleteSnapshot(context.Background(), "vol1", "snap1", "team1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(repo.deleted) != 1 || repo.deleted[0] != "snap1" {
		t.Fatalf("snapshot was not deleted: %v", repo.deleted)
	}
}

func TestDeleteSnapshotDir_RemovesDir(t *testing.T) {
	repo := newFakeRepo()
	mgr := newTestManager(repo, nil)
	// Use the mgr's metaClient (which is created in newTestManager)
	metaClient := mgr.metaClient.(*fakeMeta)
	p, err := naming.JuiceFSSnapshotPath("vol1", "snap1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	metaClient.ensurePath(p)
	mgr.deleteSnapshotDir(context.Background(), p)
	if len(metaClient.removedPaths) != 1 || metaClient.removedPaths[0] != p {
		t.Fatalf("snapshot dir not removed: %v", metaClient.removedPaths)
	}
}

func TestEnsurePathExists_CreatesDirectories(t *testing.T) {
	repo := newFakeRepo()
	mgr := newTestManager(repo, nil)
	parent, err := naming.JuiceFSSnapshotParentPath("vol1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := mgr.ensurePathExists(context.Background(), parent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, err := mgr.lookupPath(parent); err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
}

func TestVolumeLock(t *testing.T) {
	repo := newFakeRepo()
	mgr := newTestManager(repo, nil)
	if !mgr.acquireVolumeLock("vol1", time.Second) {
		t.Fatalf("expected lock acquisition")
	}
	if mgr.acquireVolumeLock("vol1", time.Second) {
		t.Fatalf("expected lock to be held")
	}
	mgr.releaseVolumeLock("vol1")
	if !mgr.acquireVolumeLock("vol1", time.Second) {
		t.Fatalf("expected lock after release")
	}
}
