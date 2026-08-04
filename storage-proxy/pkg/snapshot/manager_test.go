package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func newTestManager(repo *fakeRepo, volMgr volumeProvider) *Manager {
	cacheDir, err := os.MkdirTemp("", "storage-proxy-snapshot-test-*")
	if err != nil {
		panic(err)
	}
	return &Manager{
		repo:      repo,
		volMgr:    volMgr,
		config:    &config.StorageProxyConfig{DefaultClusterId: "test-cluster", CacheDir: cacheDir},
		logger:    logrus.New(),
		clusterID: "test-cluster",
		podID:     "test-pod",
		locks:     make(map[string]time.Time),
	}
}

func seedS0FSSnapshot(t *testing.T, mgr *Manager, teamID, volumeID, snapshotID string) {
	t.Helper()
	engine, closeFn, err := mgr.openS0FSEngine(context.Background(), teamID, volumeID)
	if err != nil {
		t.Fatalf("open s0fs engine: %v", err)
	}
	defer closeFn()
	if _, err := engine.CreateSnapshot(snapshotID); err != nil {
		t.Fatalf("create snapshot state: %v", err)
	}
}

type fakeRepo struct {
	volumes      map[string]*db.SandboxVolume
	owners       map[string]*db.SandboxVolumeOwner
	snapshots    map[string]*db.Snapshot
	heads        map[string]*db.S0FSCommittedHead
	activeMounts map[string][]*db.VolumeMount
	deleted      []string
	deleteErr    error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		volumes:      make(map[string]*db.SandboxVolume),
		owners:       make(map[string]*db.SandboxVolumeOwner),
		snapshots:    make(map[string]*db.Snapshot),
		heads:        make(map[string]*db.S0FSCommittedHead),
		activeMounts: make(map[string][]*db.VolumeMount),
	}
}

func (r *fakeRepo) GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error) {
	v, ok := r.volumes[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return v, nil
}

func (r *fakeRepo) GetSandboxVolumeOwner(ctx context.Context, id string) (*db.SandboxVolumeOwner, error) {
	owner, ok := r.owners[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return owner, nil
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

func (r *fakeRepo) ListSnapshotsByVolumeForUpdate(ctx context.Context, tx pgx.Tx, volumeID string) ([]*db.Snapshot, error) {
	return r.ListSnapshotsByVolume(ctx, volumeID)
}

func (r *fakeRepo) ListSandboxVolumesBySource(ctx context.Context, sourceVolumeID string) ([]*db.SandboxVolume, error) {
	var volumes []*db.SandboxVolume
	for _, volume := range r.volumes {
		if volume.SourceVolumeID != nil && *volume.SourceVolumeID == sourceVolumeID {
			volumes = append(volumes, volume)
		}
	}
	return volumes, nil
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

func (r *fakeRepo) GetS0FSCommittedHead(_ context.Context, volumeID string) (*db.S0FSCommittedHead, error) {
	head, ok := r.heads[volumeID]
	if !ok {
		return nil, db.ErrNotFound
	}
	copy := *head
	return &copy, nil
}

func (r *fakeRepo) CompareAndSwapS0FSCommittedHead(_ context.Context, volumeID string, expectedManifestSeq uint64, head *db.S0FSCommittedHead) error {
	if _, ok := r.volumes[volumeID]; !ok {
		return db.ErrNotFound
	}
	current, ok := r.heads[volumeID]
	if !ok {
		if expectedManifestSeq != 0 {
			return db.ErrConflict
		}
	} else if current.ManifestSeq != expectedManifestSeq {
		return db.ErrConflict
	}
	copy := *head
	r.heads[volumeID] = &copy
	return nil
}

func (r *fakeRepo) GetActiveMounts(_ context.Context, volumeID string, _ int) ([]*db.VolumeMount, error) {
	return r.activeMounts[volumeID], nil
}

func rawMountOptions(t *testing.T, opts volume.MountOptions) *json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	msg := json.RawMessage(payload)
	return &msg
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
	if f.ctx != nil && f.ctx.VolumeID != "" && f.ctx.VolumeID != volumeID {
		return nil, errors.New("not mounted")
	}
	return f.ctx, nil
}

func (f *fakeVolumeProvider) UpdateVolumeRoot(volumeID string, rootInode fsmeta.Ino) error {
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

type fakeMeteringRecorder struct {
	events              []*metering.Event
	storageObservations []*metering.StorageObservation
	closedStorage       []*metering.StorageObservation
	watermarks          []metering.ProducerWatermark
}

func (f *fakeMeteringRecorder) AppendEvent(_ context.Context, event *metering.Event) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeMeteringRecorder) AppendEventTx(_ context.Context, _ pgx.Tx, event *metering.Event) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeMeteringRecorder) RecordStorageObservation(_ context.Context, observation *metering.StorageObservation) error {
	f.storageObservations = append(f.storageObservations, observation)
	return nil
}

func (f *fakeMeteringRecorder) RecordStorageObservationTx(_ context.Context, _ pgx.Tx, observation *metering.StorageObservation) error {
	f.storageObservations = append(f.storageObservations, observation)
	return nil
}

func (f *fakeMeteringRecorder) CloseStorageObservation(_ context.Context, observation *metering.StorageObservation) error {
	f.closedStorage = append(f.closedStorage, observation)
	return nil
}

func (f *fakeMeteringRecorder) CloseStorageObservationTx(_ context.Context, _ pgx.Tx, observation *metering.StorageObservation) error {
	f.closedStorage = append(f.closedStorage, observation)
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

func TestCreateSnapshot_CreatesVolumePathWhenMissing(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}

	mgr := newTestManager(repo, &fakeVolumeProvider{err: errors.New("not mounted")})
	mgr.config.RegionID = "aws-us-east-1"
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

	cfg, err := mgr.s0fsConfig("team1", "vol1")
	if err != nil {
		t.Fatalf("s0fsConfig() error = %v", err)
	}
	if _, err := s0fs.LoadSnapshot(context.Background(), cfg, snapshot.ID); err != nil {
		t.Fatalf("snapshot state should exist after create, got: %v", err)
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
	if meteringRecorder.events[0].RegionID != "aws-us-east-1" {
		t.Fatalf("region_id = %q, want %q", meteringRecorder.events[0].RegionID, "aws-us-east-1")
	}
	if len(meteringRecorder.storageObservations) != 2 {
		t.Fatalf("expected two storage observations, got %d", len(meteringRecorder.storageObservations))
	}
	if len(meteringRecorder.watermarks) != 3 {
		t.Fatalf("expected three watermarks, got %d", len(meteringRecorder.watermarks))
	}
}

func TestRestoreSnapshot_WaitsForInvalidateAck(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{
		err:          errors.New("not mounted"),
		beginPending: 1,
	}
	mgr := newTestManager(repo, volMgr)
	mgr.config.RestoreRemountTimeout = "100ms"
	seedS0FSSnapshot(t, mgr, "team1", "vol1", "snap1")

	err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
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

func TestRestoreSnapshot_SkipsInvalidateWaitWhenNoParticipantsRemain(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{
		err:          errors.New("not mounted"),
		beginPending: 0,
	}
	mgr := newTestManager(repo, volMgr)
	seedS0FSSnapshot(t, mgr, "team1", "vol1", "snap1")

	err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
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
	if volMgr.waitCalled {
		t.Fatalf("expected WaitForInvalidate not to be called when no remount participants remain")
	}
}

func TestRestoreSnapshot_RemountTimeout(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	volMgr := &fakeVolumeProvider{
		err:          errors.New("not mounted"),
		beginPending: 1,
		waitErr:      context.DeadlineExceeded,
	}
	mgr := newTestManager(repo, volMgr)
	mgr.config.RestoreRemountTimeout = "1ms"
	seedS0FSSnapshot(t, mgr, "team1", "vol1", "snap1")

	meteringRecorder := &fakeMeteringRecorder{}
	mgr.SetMeteringRepository(meteringRecorder)

	err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
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
	if len(meteringRecorder.events) != 0 {
		t.Fatalf("expected no metering event on remount timeout, got %d", len(meteringRecorder.events))
	}
	if len(meteringRecorder.watermarks) != 0 {
		t.Fatalf("expected no watermark on remount timeout, got %d", len(meteringRecorder.watermarks))
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
	mgr.config.RegionID = "aws-us-east-1"
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
	if len(meteringRecorder.closedStorage) != 1 {
		t.Fatalf("expected one closed storage observation, got %d", len(meteringRecorder.closedStorage))
	}
	if len(meteringRecorder.watermarks) != 2 {
		t.Fatalf("expected two watermarks, got %d", len(meteringRecorder.watermarks))
	}
}

func TestDeleteSnapshotsForVolumeTxRecordsMetering(t *testing.T) {
	repo := newFakeRepo()
	createdAt := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	repo.snapshots["snap1"] = &db.Snapshot{
		ID:        "snap1",
		VolumeID:  "vol1",
		TeamID:    "team1",
		UserID:    "user1",
		SizeBytes: 128,
		CreatedAt: createdAt,
	}
	repo.snapshots["snap2"] = &db.Snapshot{
		ID:        "snap2",
		VolumeID:  "vol1",
		TeamID:    "team1",
		UserID:    "user1",
		SizeBytes: 256,
		CreatedAt: createdAt.Add(time.Hour),
	}
	mgr := newTestManager(repo, nil)
	mgr.config.RegionID = "aws-us-east-1"
	meteringRecorder := &fakeMeteringRecorder{}
	mgr.SetMeteringRepository(meteringRecorder)
	deletedAt := time.Date(2026, 3, 14, 15, 30, 0, 0, time.UTC)

	if err := mgr.DeleteSnapshotsForVolumeTx(context.Background(), nil, "vol1", "team1", deletedAt); err != nil {
		t.Fatalf("DeleteSnapshotsForVolumeTx() error = %v", err)
	}
	if len(repo.snapshots) != 0 {
		t.Fatalf("remaining snapshots = %v, want none", repo.snapshots)
	}
	if len(meteringRecorder.events) != 2 {
		t.Fatalf("event count = %d, want 2", len(meteringRecorder.events))
	}
	if len(meteringRecorder.closedStorage) != 2 {
		t.Fatalf("closed storage count = %d, want 2", len(meteringRecorder.closedStorage))
	}
	if len(meteringRecorder.watermarks) != 4 {
		t.Fatalf("watermark count = %d, want 4", len(meteringRecorder.watermarks))
	}

	eventSubjects := make(map[string]bool, 2)
	for _, event := range meteringRecorder.events {
		if event.EventType != metering.EventTypeSnapshotDeleted {
			t.Fatalf("event type = %q, want %q", event.EventType, metering.EventTypeSnapshotDeleted)
		}
		if !event.OccurredAt.Equal(deletedAt) {
			t.Fatalf("event occurred_at = %v, want %v", event.OccurredAt, deletedAt)
		}
		eventSubjects[event.SubjectID] = true
	}
	if !eventSubjects["snap1"] || !eventSubjects["snap2"] {
		t.Fatalf("event subjects = %v, want snap1 and snap2", eventSubjects)
	}
	for _, observation := range meteringRecorder.closedStorage {
		if observation.SubjectType != metering.SubjectTypeSnapshot {
			t.Fatalf("observation subject type = %q, want %q", observation.SubjectType, metering.SubjectTypeSnapshot)
		}
		if !observation.ObservedAt.Equal(deletedAt) {
			t.Fatalf("observation observed_at = %v, want %v", observation.ObservedAt, deletedAt)
		}
	}
}

func TestRestoreSnapshot_BeginInvalidateError(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", UserID: "user1"}
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	beginErr := errors.New("begin failed")
	volMgr := &fakeVolumeProvider{
		err:      errors.New("not mounted"),
		beginErr: beginErr,
	}
	mgr := newTestManager(repo, volMgr)
	seedS0FSSnapshot(t, mgr, "team1", "vol1", "snap1")

	err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
	})
	if !errors.Is(err, beginErr) {
		t.Fatalf("expected begin invalidate error, got %v", err)
	}
	if !volMgr.beginCalled {
		t.Fatalf("expected BeginInvalidate to be called")
	}
	if volMgr.waitCalled {
		t.Fatalf("expected WaitForInvalidate not to be called")
	}
}

func TestCreateSnapshot_RejectsMountedCtldOwnerWithoutCheckpoint(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1"}
	repo.activeMounts["vol1"] = []*db.VolumeMount{{
		VolumeID:     "vol1",
		MountOptions: rawMountOptions(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
	}}
	mgr := newTestManager(repo, nil)

	_, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol1",
		Name:     "snap1",
		TeamID:   "team1",
		UserID:   "user1",
	})
	if !errors.Is(err, ErrMountedCtldOwner) {
		t.Fatalf("CreateSnapshot() error = %v, want %v", err, ErrMountedCtldOwner)
	}
}

func TestCreateSnapshot_AllowsMountedCtldOwnerWithCheckpoint(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1"}
	repo.activeMounts["vol1"] = []*db.VolumeMount{{
		VolumeID:     "vol1",
		MountOptions: rawMountOptions(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
	}}
	mgr := newTestManager(repo, nil)
	coordinator := &failingFlushCoordinator{}
	mgr.SetFlushCoordinator(coordinator)

	snapshot, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID:                 "vol1",
		Name:                     "snap1",
		TeamID:                   "team1",
		UserID:                   "user1",
		ActiveCheckpointPrepared: true,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if snapshot == nil || snapshot.VolumeID != "vol1" {
		t.Fatalf("CreateSnapshot() snapshot = %#v", snapshot)
	}
	if coordinator.called {
		t.Fatal("CreateSnapshot called distributed flush coordinator for ctld-owned mount")
	}
}

func TestCreateSnapshot_RejectsActiveRWXWritableMount(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1", AccessMode: string(volume.AccessModeRWX)}
	repo.activeMounts["vol1"] = []*db.VolumeMount{{
		VolumeID:     "vol1",
		MountOptions: rawMountOptions(t, volume.MountOptions{AccessMode: volume.AccessModeRWX, OwnerKind: volume.OwnerKindStorageProxy}),
	}}
	mgr := newTestManager(repo, nil)

	_, err := mgr.CreateSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "vol1",
		Name:     "snap1",
		TeamID:   "team1",
		UserID:   "user1",
	})
	if !errors.Is(err, ErrActiveRWXSnapshotUnsupported) {
		t.Fatalf("CreateSnapshot() error = %v, want %v", err, ErrActiveRWXSnapshotUnsupported)
	}
}

func TestRestoreSnapshot_RejectsMountedCtldOwner(t *testing.T) {
	repo := newFakeRepo()
	repo.snapshots["snap1"] = &db.Snapshot{ID: "snap1", VolumeID: "vol1", TeamID: "team1"}
	repo.activeMounts["vol1"] = []*db.VolumeMount{{
		VolumeID:     "vol1",
		MountOptions: rawMountOptions(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
	}}
	mgr := newTestManager(repo, nil)

	err := mgr.RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeID:   "vol1",
		SnapshotID: "snap1",
		TeamID:     "team1",
		UserID:     "user1",
	})
	if !errors.Is(err, ErrMountedCtldOwner) {
		t.Fatalf("RestoreSnapshot() error = %v, want %v", err, ErrMountedCtldOwner)
	}
}

func TestForkVolume_RejectsMountedCtldOwner(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol1"] = &db.SandboxVolume{ID: "vol1", TeamID: "team1"}
	repo.activeMounts["vol1"] = []*db.VolumeMount{{
		VolumeID:     "vol1",
		MountOptions: rawMountOptions(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
	}}
	mgr := newTestManager(repo, nil)

	_, err := mgr.ForkVolume(context.Background(), &ForkVolumeRequest{
		SourceVolumeID: "vol1",
		TeamID:         "team1",
		UserID:         "user1",
	})
	if !errors.Is(err, ErrMountedCtldOwner) {
		t.Fatalf("ForkVolume() error = %v, want %v", err, ErrMountedCtldOwner)
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

func TestConfiguredMeteringRecorderRejectsTypedNil(t *testing.T) {
	var recorder *fakeMeteringRecorder
	if _, ok := configuredMeteringRecorder(recorder); ok {
		t.Fatal("typed-nil metering recorder should be treated as disabled")
	}
}

func TestAppendStorageObservationIgnoresTypedNilMeteringRecorder(t *testing.T) {
	var recorder *fakeMeteringRecorder
	mgr := &Manager{}
	mgr.SetMeteringRepository(recorder)

	err := mgr.appendStorageObservation(context.Background(), &metering.StorageObservation{})
	if err != nil {
		t.Fatalf("appendStorageObservation() error = %v", err)
	}
}
