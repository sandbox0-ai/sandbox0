package snapshot

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
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

func (r *fakeRepo) CreateSandboxVolume(ctx context.Context, volume *db.SandboxVolume) error {
	if volume == nil {
		return nil
	}
	r.volumes[volume.ID] = volume
	return nil
}

func (r *fakeRepo) CreateSandboxVolumeTx(ctx context.Context, tx pgx.Tx, volume *db.SandboxVolume) error {
	return r.CreateSandboxVolume(ctx, volume)
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

func (r *fakeRepo) DeleteSandboxVolumeTx(ctx context.Context, tx pgx.Tx, id string) error {
	delete(r.volumes, id)
	return nil
}

type fakeVolumeProvider struct {
	ctx          *volume.VolumeContext
	err          error
	beginPending int
	beginErr     error
	waitErr      error
	beginCalled  bool
	waitCalled   bool
	lastVolumeID string
	lastAckID    string
}

func (f *fakeVolumeProvider) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	f.lastVolumeID = volumeID
	if f.err != nil {
		return nil, f.err
	}
	return f.ctx, nil
}

func (f *fakeVolumeProvider) UpdateVolumeRoot(volumeID string, rootInode meta.Ino) error {
	f.lastVolumeID = volumeID
	if f.err != nil {
		return f.err
	}
	return nil
}

func (f *fakeVolumeProvider) BeginInvalidate(volumeID, invalidateID string) (int, error) {
	f.beginCalled = true
	f.lastVolumeID = volumeID
	f.lastAckID = invalidateID
	if f.beginErr != nil {
		return 0, f.beginErr
	}
	return f.beginPending, nil
}

func (f *fakeVolumeProvider) WaitForInvalidate(ctx context.Context, volumeID, invalidateID string) error {
	f.waitCalled = true
	f.lastVolumeID = volumeID
	f.lastAckID = invalidateID
	if f.waitErr != nil {
		return f.waitErr
	}
	return nil
}

type fakeMeta struct {
	mu           sync.Mutex
	pathToIno    map[string]meta.Ino
	inoToPath    map[meta.Ino]string
	nextIno      meta.Ino
	removedPaths []string
}

type fakeArchiveMeta struct {
	attrs   map[meta.Ino]*meta.Attr
	entries map[meta.Ino][]*meta.Entry
	links   map[meta.Ino]string
	getErr  syscall.Errno
	readErr syscall.Errno
	linkErr syscall.Errno
}

func (f *fakeArchiveMeta) GetAttr(ctx meta.Context, inode meta.Ino, attr *meta.Attr) syscall.Errno {
	if f.getErr != 0 {
		return f.getErr
	}
	stored, ok := f.attrs[inode]
	if !ok {
		return syscall.ENOENT
	}
	if attr != nil {
		*attr = *stored
	}
	return 0
}

func (f *fakeArchiveMeta) Readdir(ctx meta.Context, inode meta.Ino, wantattr uint8, entries *[]*meta.Entry) syscall.Errno {
	if f.readErr != 0 {
		return f.readErr
	}
	list, ok := f.entries[inode]
	if !ok {
		return syscall.ENOENT
	}
	cloned := make([]*meta.Entry, 0, len(list))
	for _, entry := range list {
		copyEntry := *entry
		if entry.Attr != nil {
			copyAttr := *entry.Attr
			copyEntry.Attr = &copyAttr
		}
		cloned = append(cloned, &copyEntry)
	}
	*entries = cloned
	return 0
}

func (f *fakeArchiveMeta) ReadLink(ctx meta.Context, inode meta.Ino, path *[]byte) syscall.Errno {
	if f.linkErr != 0 {
		return f.linkErr
	}
	target, ok := f.links[inode]
	if !ok {
		return syscall.ENOENT
	}
	*path = []byte(target)
	return 0
}

type fakeArchiveReader struct {
	contents map[meta.Ino][]byte
}

func (f *fakeArchiveReader) ReadFile(ctx context.Context, inode meta.Ino, size uint64, w io.Writer) error {
	data := f.contents[inode]
	if len(data) != int(size) {
		return errors.New("unexpected file size")
	}
	_, err := w.Write(data)
	return err
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
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.inoToPath[srcIno]; !ok {
		return syscall.ENOENT
	}
	parentPath, ok := f.inoToPath[parentIno]
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
	}

	if count != nil {
		*count = 1
	}
	if total != nil {
		*total = 1
	}
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

type fakeMeteringRecorder struct {
	events     []*metering.Event
	watermarks []metering.ProducerWatermark
}

func (f *fakeMeteringRecorder) AppendEvent(_ context.Context, event *metering.Event) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeMeteringRecorder) AppendEventTx(_ context.Context, _ pgx.Tx, event *metering.Event) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeMeteringRecorder) UpsertProducerWatermark(_ context.Context, producer string, regionID string, completeBefore time.Time) error {
	f.watermarks = append(f.watermarks, metering.ProducerWatermark{
		Producer:       producer,
		RegionID:       regionID,
		CompleteBefore: completeBefore,
	})
	return nil
}

func (f *fakeMeteringRecorder) UpsertProducerWatermarkTx(_ context.Context, _ pgx.Tx, producer string, regionID string, completeBefore time.Time) error {
	f.watermarks = append(f.watermarks, metering.ProducerWatermark{
		Producer:       producer,
		RegionID:       regionID,
		CompleteBefore: completeBefore,
	})
	return nil
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

func TestCreateSnapshot_CreatesVolumePathWhenMissing(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}

	mgr := newTestManager(repo, &fakeVolumeProvider{err: errors.New("not mounted")})
	mgr.config.RegionID = "aws/us-east-1"
	meteringRecorder := &fakeMeteringRecorder{}
	mgr.SetMeteringRepository(meteringRecorder)
	snapshot, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID:    "vol1",
		Name:        "snap-1",
		Description: "test snapshot",
		TeamID:      "team1",
		UserID:      "user1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snapshot == nil || snapshot.ID == "" {
		t.Fatalf("expected snapshot to be created, got: %+v", snapshot)
	}

	volumePath, err := naming.JuiceFSVolumePath("vol1")
	if err != nil {
		t.Fatalf("volume path generation failed: %v", err)
	}
	if _, _, err := mgr.lookupPath(volumePath); err != nil {
		t.Fatalf("volume path should exist after snapshot create, got: %v", err)
	}

	if _, ok := repo.snapshots[snapshot.ID]; !ok {
		t.Fatalf("snapshot metadata not persisted in repository")
	}
	if len(meteringRecorder.events) != 1 {
		t.Fatalf("expected one metering event, got %d", len(meteringRecorder.events))
	}
	if meteringRecorder.events[0].EventType != metering.EventTypeSnapshotCreated {
		t.Fatalf("event type = %q, want %q", meteringRecorder.events[0].EventType, metering.EventTypeSnapshotCreated)
	}
	if meteringRecorder.events[0].RegionID != "aws/us-east-1" {
		t.Fatalf("region_id = %q, want %q", meteringRecorder.events[0].RegionID, "aws/us-east-1")
	}
	if len(meteringRecorder.watermarks) != 1 {
		t.Fatalf("expected one watermark, got %d", len(meteringRecorder.watermarks))
	}
}

func TestRestoreSnapshot_WaitsForInvalidateAck(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{
		err:          errors.New("not mounted"),
		beginPending: 1,
	}
	mgr := newTestManager(repo, volMgr)
	mgr.config.RestoreRemountTimeout = "100ms"

	metaClient := mgr.metaClient.(*fakeMeta)
	volumePath, err := naming.JuiceFSVolumePath("vol1")
	if err != nil {
		t.Fatalf("volume path generation failed: %v", err)
	}
	snapshotPath, err := naming.JuiceFSSnapshotPath("vol1", "snap1")
	if err != nil {
		t.Fatalf("snapshot path generation failed: %v", err)
	}
	metaClient.ensurePath(volumePath)
	metaClient.ensurePath(snapshotPath)

	err = mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !volMgr.beginCalled {
		t.Fatalf("expected BeginInvalidate to be called")
	}
	if !volMgr.waitCalled {
		t.Fatalf("expected WaitForInvalidate to be called")
	}
	if volMgr.lastAckID == "" {
		t.Fatalf("expected invalidate id to be set")
	}
}

func TestRestoreSnapshot_RemountTimeout(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{
		err:          errors.New("not mounted"),
		beginPending: 1,
		waitErr:      context.DeadlineExceeded,
	}
	mgr := newTestManager(repo, volMgr)
	mgr.config.RestoreRemountTimeout = "1ms"

	metaClient := mgr.metaClient.(*fakeMeta)
	volumePath, err := naming.JuiceFSVolumePath("vol1")
	if err != nil {
		t.Fatalf("volume path generation failed: %v", err)
	}
	snapshotPath, err := naming.JuiceFSSnapshotPath("vol1", "snap1")
	if err != nil {
		t.Fatalf("snapshot path generation failed: %v", err)
	}
	metaClient.ensurePath(volumePath)
	metaClient.ensurePath(snapshotPath)

	meteringRecorder := &fakeMeteringRecorder{}
	mgr.SetMeteringRepository(meteringRecorder)

	err = mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if !errors.Is(err, ErrRemountTimeout) {
		t.Fatalf("expected ErrRemountTimeout, got %v", err)
	}
	if !volMgr.beginCalled {
		t.Fatalf("expected BeginInvalidate to be called")
	}
	if !volMgr.waitCalled {
		t.Fatalf("expected WaitForInvalidate to be called")
	}
	if len(meteringRecorder.events) != 1 {
		t.Fatalf("expected one metering event, got %d", len(meteringRecorder.events))
	}
	if meteringRecorder.events[0].EventType != metering.EventTypeSnapshotRestored {
		t.Fatalf("event type = %q, want %q", meteringRecorder.events[0].EventType, metering.EventTypeSnapshotRestored)
	}
	if len(meteringRecorder.watermarks) != 1 {
		t.Fatalf("expected one watermark, got %d", len(meteringRecorder.watermarks))
	}
}

func TestDeleteSnapshotRecordsMetering(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		UserID:    "user1",
		CreatedAt: time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC),
	}
	mgr := newTestManager(repo, &fakeVolumeProvider{err: errors.New("not mounted")})
	mgr.config.RegionID = "aws/us-east-1"
	meteringRecorder := &fakeMeteringRecorder{}
	mgr.SetMeteringRepository(meteringRecorder)

	err := mgr.DeleteSnapshot(context.Background(), "vol1", "snap1", "team1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meteringRecorder.events) != 1 {
		t.Fatalf("expected one metering event, got %d", len(meteringRecorder.events))
	}
	if meteringRecorder.events[0].EventType != metering.EventTypeSnapshotDeleted {
		t.Fatalf("event type = %q, want %q", meteringRecorder.events[0].EventType, metering.EventTypeSnapshotDeleted)
	}
	if len(meteringRecorder.watermarks) != 1 {
		t.Fatalf("expected one watermark, got %d", len(meteringRecorder.watermarks))
	}
}

func TestRestoreSnapshot_BeginInvalidateError(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	beginErr := errors.New("begin failed")
	volMgr := &fakeVolumeProvider{
		err:      errors.New("not mounted"),
		beginErr: beginErr,
	}
	mgr := newTestManager(repo, volMgr)

	metaClient := mgr.metaClient.(*fakeMeta)
	volumePath, err := naming.JuiceFSVolumePath("vol1")
	if err != nil {
		t.Fatalf("volume path generation failed: %v", err)
	}
	snapshotPath, err := naming.JuiceFSSnapshotPath("vol1", "snap1")
	if err != nil {
		t.Fatalf("snapshot path generation failed: %v", err)
	}
	metaClient.ensurePath(volumePath)
	metaClient.ensurePath(snapshotPath)

	err = mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if err == nil || !strings.Contains(err.Error(), "begin invalidate") {
		t.Fatalf("expected begin invalidate error, got %v", err)
	}
	if !volMgr.beginCalled {
		t.Fatalf("expected BeginInvalidate to be called")
	}
	if volMgr.waitCalled {
		t.Fatalf("expected WaitForInvalidate not to be called")
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

func TestExportSnapshotArchive(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{
		ID:         "vol1",
		TeamID:     "team1",
		CacheSize:  "1G",
		Prefetch:   1,
		BufferSize: "32M",
	}
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		RootInode: 100,
	}

	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755, Mtime: 1710000000}
	appDirAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755, Mtime: 1710000001}
	fileAttr := &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Length: 13, Mtime: 1710000002}
	linkAttr := &meta.Attr{Typ: meta.TypeSymlink, Mode: 0o777, Mtime: 1710000003}

	archiveMeta := &fakeArchiveMeta{
		attrs: map[meta.Ino]*meta.Attr{
			100: rootAttr,
			101: appDirAttr,
			102: fileAttr,
			103: linkAttr,
		},
		entries: map[meta.Ino][]*meta.Entry{
			100: {
				{Inode: 103, Name: []byte("latest"), Attr: linkAttr},
				{Inode: 101, Name: []byte("app"), Attr: appDirAttr},
			},
			101: {
				{Inode: 102, Name: []byte("main.go"), Attr: fileAttr},
			},
		},
		links: map[meta.Ino]string{
			103: "app/main.go",
		},
	}
	archiveReader := &fakeArchiveReader{
		contents: map[meta.Ino][]byte{
			102: []byte("package main\n"),
		},
	}

	mgr := newTestManager(repo, nil)
	mgr.newArchiveSession = func(ctx context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
		return &snapshotArchiveSession{
			meta:   archiveMeta,
			reader: archiveReader,
			close:  func() error { return nil },
		}, nil
	}

	var buf bytes.Buffer
	err := mgr.ExportSnapshotArchive(context.Background(), &ExportSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	}, &buf)
	if err != nil {
		t.Fatalf("ExportSnapshotArchive() error = %v", err)
	}

	gzr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var names []string
	fileContents := map[string]string{}
	linkTargets := map[string]string{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		names = append(names, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeReg:
			data, err := io.ReadAll(tr)
			if err != nil {
				t.Fatalf("ReadAll(%q) error = %v", hdr.Name, err)
			}
			fileContents[hdr.Name] = string(data)
		case tar.TypeSymlink:
			linkTargets[hdr.Name] = hdr.Linkname
		}
	}

	wantNames := []string{"app/", "app/main.go", "latest"}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("archive entries = %v, want %v", names, wantNames)
	}
	if fileContents["app/main.go"] != "package main\n" {
		t.Fatalf("file content = %q, want %q", fileContents["app/main.go"], "package main\n")
	}
	if linkTargets["latest"] != "app/main.go" {
		t.Fatalf("symlink target = %q, want %q", linkTargets["latest"], "app/main.go")
	}
}

func TestListSnapshotCasefoldCollisions(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{
		ID:     "vol1",
		TeamID: "team1",
	}
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		RootInode: 100,
	}

	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755}
	appDirAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755}
	fileAttr := &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Length: 1}

	archiveMeta := &fakeArchiveMeta{
		attrs: map[meta.Ino]*meta.Attr{
			100: rootAttr,
			101: appDirAttr,
			102: fileAttr,
			103: fileAttr,
		},
		entries: map[meta.Ino][]*meta.Entry{
			100: {
				{Inode: 101, Name: []byte("app"), Attr: appDirAttr},
			},
			101: {
				{Inode: 102, Name: []byte("Main.go"), Attr: fileAttr},
				{Inode: 103, Name: []byte("main.go"), Attr: fileAttr},
			},
		},
	}

	mgr := newTestManager(repo, nil)
	mgr.newArchiveSession = func(ctx context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
		return &snapshotArchiveSession{
			meta:   archiveMeta,
			reader: &fakeArchiveReader{},
			close:  func() error { return nil },
		}, nil
	}

	collisions, err := mgr.ListSnapshotCasefoldCollisions(context.Background(), &ListSnapshotCasefoldCollisionsRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if err != nil {
		t.Fatalf("ListSnapshotCasefoldCollisions() error = %v", err)
	}
	if len(collisions) != 1 {
		t.Fatalf("collisions = %d, want 1", len(collisions))
	}
	if collisions[0].NormalizedPath != "/app/main.go" {
		t.Fatalf("normalized path = %q, want %q", collisions[0].NormalizedPath, "/app/main.go")
	}
	if strings.Join(collisions[0].Paths, ",") != "/app/Main.go,/app/main.go" {
		t.Fatalf("paths = %v, want [/app/Main.go /app/main.go]", collisions[0].Paths)
	}
}

func TestListSnapshotCasefoldCollisionsReturnsEmptyWhenNamespaceIsSafe(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{
		ID:     "vol1",
		TeamID: "team1",
	}
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		RootInode: 100,
	}

	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755}
	fileAttr := &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Length: 1}

	archiveMeta := &fakeArchiveMeta{
		attrs: map[meta.Ino]*meta.Attr{
			100: rootAttr,
			101: fileAttr,
		},
		entries: map[meta.Ino][]*meta.Entry{
			100: {
				{Inode: 101, Name: []byte("main.go"), Attr: fileAttr},
			},
		},
	}

	mgr := newTestManager(repo, nil)
	mgr.newArchiveSession = func(ctx context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
		return &snapshotArchiveSession{
			meta:   archiveMeta,
			reader: &fakeArchiveReader{},
			close:  func() error { return nil },
		}, nil
	}

	collisions, err := mgr.ListSnapshotCasefoldCollisions(context.Background(), &ListSnapshotCasefoldCollisionsRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if err != nil {
		t.Fatalf("ListSnapshotCasefoldCollisions() error = %v", err)
	}
	if len(collisions) != 0 {
		t.Fatalf("collisions = %v, want none", collisions)
	}
}

func TestListSnapshotCasefoldCollisionsDetectsUnicodeNormalizationCollisions(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{
		ID:     "vol1",
		TeamID: "team1",
	}
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		RootInode: 100,
	}

	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755}
	fileAttr := &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Length: 1}

	archiveMeta := &fakeArchiveMeta{
		attrs: map[meta.Ino]*meta.Attr{
			100: rootAttr,
			101: fileAttr,
			102: fileAttr,
		},
		entries: map[meta.Ino][]*meta.Entry{
			100: {
				{Inode: 101, Name: []byte("Caf\u00e9.txt"), Attr: fileAttr},
				{Inode: 102, Name: []byte("Cafe\u0301.txt"), Attr: fileAttr},
			},
		},
	}

	mgr := newTestManager(repo, nil)
	mgr.newArchiveSession = func(ctx context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
		return &snapshotArchiveSession{
			meta:   archiveMeta,
			reader: &fakeArchiveReader{},
			close:  func() error { return nil },
		}, nil
	}

	collisions, err := mgr.ListSnapshotCasefoldCollisions(context.Background(), &ListSnapshotCasefoldCollisionsRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if err != nil {
		t.Fatalf("ListSnapshotCasefoldCollisions() error = %v", err)
	}
	if len(collisions) != 1 {
		t.Fatalf("collisions = %d, want 1", len(collisions))
	}
	if collisions[0].NormalizedPath != "/cafe\u0301.txt" {
		t.Fatalf("normalized path = %q, want %q", collisions[0].NormalizedPath, "/cafe\u0301.txt")
	}
	if strings.Join(collisions[0].Paths, ",") != "/Cafe\u0301.txt,/Caf\u00e9.txt" {
		t.Fatalf("paths = %v, want unicode-normalization collision pair", collisions[0].Paths)
	}
}

func TestListSnapshotCompatibilityIssuesDetectsWindowsReservedNames(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{
		ID:     "vol1",
		TeamID: "team1",
	}
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		RootInode: 100,
	}

	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o755}
	fileAttr := &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Length: 1}

	archiveMeta := &fakeArchiveMeta{
		attrs: map[meta.Ino]*meta.Attr{
			100: rootAttr,
			101: fileAttr,
		},
		entries: map[meta.Ino][]*meta.Entry{
			100: {
				{Inode: 101, Name: []byte("CON.txt"), Attr: fileAttr},
			},
		},
	}

	mgr := newTestManager(repo, nil)
	mgr.newArchiveSession = func(ctx context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
		return &snapshotArchiveSession{
			meta:   archiveMeta,
			reader: &fakeArchiveReader{},
			close:  func() error { return nil },
		}, nil
	}

	issues, err := mgr.ListSnapshotCompatibilityIssues(context.Background(), &ListSnapshotCompatibilityIssuesRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
		Capabilities: pathnorm.FilesystemCapabilities{
			WindowsCompatiblePaths: true,
		},
	})
	if err != nil {
		t.Fatalf("ListSnapshotCompatibilityIssues() error = %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(issues))
	}
	if issues[0].Code != pathnorm.IssueCodeWindowsReservedName {
		t.Fatalf("issue code = %q, want %q", issues[0].Code, pathnorm.IssueCodeWindowsReservedName)
	}
	if issues[0].Path != "/CON.txt" {
		t.Fatalf("issue path = %q, want /CON.txt", issues[0].Path)
	}
}
