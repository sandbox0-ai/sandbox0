package http

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sirupsen/logrus"
)

type fakeHTTPRepo struct {
	volumes        map[string]*db.SandboxVolume
	activeMounts   map[string][]*db.VolumeMount
	getActiveFunc  func(context.Context, string, int) ([]*db.VolumeMount, error)
	deletedMounts  []db.VolumeMount
	createdVolumes []*db.SandboxVolume
	deletedVolume  []string
}

func newFakeHTTPRepo() *fakeHTTPRepo {
	return &fakeHTTPRepo{
		volumes:      make(map[string]*db.SandboxVolume),
		activeMounts: make(map[string][]*db.VolumeMount),
	}
}

func (r *fakeHTTPRepo) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

func (r *fakeHTTPRepo) CreateSandboxVolumeTx(ctx context.Context, tx pgx.Tx, volume *db.SandboxVolume) error {
	r.volumes[volume.ID] = volume
	r.createdVolumes = append(r.createdVolumes, volume)
	return nil
}

func (r *fakeHTTPRepo) ListSandboxVolumesByTeam(ctx context.Context, teamID string) ([]*db.SandboxVolume, error) {
	var volumes []*db.SandboxVolume
	for _, volume := range r.volumes {
		if volume.TeamID == teamID {
			volumes = append(volumes, volume)
		}
	}
	return volumes, nil
}

func (r *fakeHTTPRepo) GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error) {
	volume, ok := r.volumes[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return volume, nil
}

func (r *fakeHTTPRepo) GetActiveMounts(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*db.VolumeMount, error) {
	if r.getActiveFunc != nil {
		return r.getActiveFunc(ctx, volumeID, heartbeatTimeout)
	}
	return r.activeMounts[volumeID], nil
}

func (r *fakeHTTPRepo) DeleteMount(ctx context.Context, volumeID, clusterID, podID string) error {
	r.deletedMounts = append(r.deletedMounts, db.VolumeMount{
		VolumeID:  volumeID,
		ClusterID: clusterID,
		PodID:     podID,
	})
	return nil
}

func (r *fakeHTTPRepo) DeleteSandboxVolumeTx(ctx context.Context, tx pgx.Tx, id string) error {
	delete(r.volumes, id)
	r.deletedVolume = append(r.deletedVolume, id)
	return nil
}

type fakeHTTPMeteringWriter struct {
	events     []*metering.Event
	watermarks []metering.ProducerWatermark
}

func (f *fakeHTTPMeteringWriter) AppendEventTx(ctx context.Context, tx pgx.Tx, event *metering.Event) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeHTTPMeteringWriter) UpsertProducerWatermarkTx(ctx context.Context, tx pgx.Tx, producer string, regionID string, completeBefore time.Time) error {
	f.watermarks = append(f.watermarks, metering.ProducerWatermark{
		Producer:       producer,
		RegionID:       regionID,
		CompleteBefore: completeBefore,
	})
	return nil
}

type fakeHTTPSnapshotManager struct {
	exportBody          []byte
	lastCreate          *snapshot.CreateSnapshotRequest
	lastExport          *snapshot.ExportSnapshotRequest
	lastCompatibility   *snapshot.ListSnapshotCompatibilityIssuesRequest
	casefoldEntries     []snapshot.SnapshotCasefoldCollision
	compatibilityIssues []pathnorm.CompatibilityIssue
	deletedSnapshot     []string
}

func (f *fakeHTTPSnapshotManager) CreateSnapshotSimple(ctx context.Context, req *snapshot.CreateSnapshotRequest) (*db.Snapshot, error) {
	f.lastCreate = req
	return &db.Snapshot{
		ID:          "snap-1",
		VolumeID:    req.VolumeID,
		TeamID:      req.TeamID,
		UserID:      req.UserID,
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   time.Date(2026, 3, 25, 3, 30, 0, 0, time.UTC),
	}, nil
}

func (f *fakeHTTPSnapshotManager) ListSnapshots(ctx context.Context, volumeID, teamID string) ([]*db.Snapshot, error) {
	return nil, nil
}

func (f *fakeHTTPSnapshotManager) GetSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) (*db.Snapshot, error) {
	return &db.Snapshot{
		ID:        snapshotID,
		VolumeID:  volumeID,
		TeamID:    teamID,
		Name:      "bootstrap-a",
		CreatedAt: time.Date(2026, 3, 25, 3, 30, 0, 0, time.UTC),
	}, nil
}

func (f *fakeHTTPSnapshotManager) ListSnapshotCasefoldCollisions(ctx context.Context, req *snapshot.ListSnapshotCasefoldCollisionsRequest) ([]snapshot.SnapshotCasefoldCollision, error) {
	return f.casefoldEntries, nil
}

func (f *fakeHTTPSnapshotManager) ListSnapshotCompatibilityIssues(ctx context.Context, req *snapshot.ListSnapshotCompatibilityIssuesRequest) ([]pathnorm.CompatibilityIssue, error) {
	f.lastCompatibility = req
	return f.compatibilityIssues, nil
}

func (f *fakeHTTPSnapshotManager) ExportSnapshotArchive(ctx context.Context, req *snapshot.ExportSnapshotRequest, w io.Writer) error {
	f.lastExport = req
	if len(f.exportBody) == 0 {
		f.exportBody = []byte("fake-archive")
	}
	_, err := w.Write(f.exportBody)
	return err
}

func (f *fakeHTTPSnapshotManager) RestoreSnapshot(ctx context.Context, req *snapshot.RestoreSnapshotRequest) error {
	return nil
}

func (f *fakeHTTPSnapshotManager) DeleteSnapshot(ctx context.Context, volumeID, snapshotID, teamID string) error {
	f.deletedSnapshot = append(f.deletedSnapshot, snapshotID)
	return nil
}

func (f *fakeHTTPSnapshotManager) ForkVolume(ctx context.Context, req *snapshot.ForkVolumeRequest) (*db.SandboxVolume, error) {
	return nil, nil
}

func TestCreateSandboxVolumeRecordsMetering(t *testing.T) {
	repo := newFakeHTTPRepo()
	meteringWriter := &fakeHTTPMeteringWriter{}
	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		meteringRepo: meteringWriter,
		regionID:     "aws-us-east-1",
		snapshotMgr:  &fakeHTTPSnapshotManager{},
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", bytes.NewBufferString(`{}`))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.createSandboxVolume(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if len(repo.createdVolumes) != 1 {
		t.Fatalf("created volume count = %d, want 1", len(repo.createdVolumes))
	}
	if len(meteringWriter.events) != 1 {
		t.Fatalf("event count = %d, want 1", len(meteringWriter.events))
	}
	event := meteringWriter.events[0]
	if event.EventType != metering.EventTypeVolumeCreated {
		t.Fatalf("event type = %q, want %q", event.EventType, metering.EventTypeVolumeCreated)
	}
	if event.RegionID != "aws-us-east-1" {
		t.Fatalf("region_id = %q, want %q", event.RegionID, "aws-us-east-1")
	}
	if len(meteringWriter.watermarks) != 1 {
		t.Fatalf("watermark count = %d, want 1", len(meteringWriter.watermarks))
	}
	if !meteringWriter.watermarks[0].CompleteBefore.Equal(event.OccurredAt) {
		t.Fatalf("watermark complete_before = %v, want %v", meteringWriter.watermarks[0].CompleteBefore, event.OccurredAt)
	}

	resp, apiErr, err := spec.DecodeResponse[db.SandboxVolume](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.TeamID != "team-1" || resp.UserID != "user-1" {
		t.Fatalf("unexpected response body: %+v", resp)
	}
}

func TestDeleteSandboxVolumeForceRecordsMetering(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{
		ID:        "vol-1",
		TeamID:    "team-1",
		UserID:    "user-1",
		CreatedAt: time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC),
	}
	repo.activeMounts["vol-1"] = []*db.VolumeMount{{
		VolumeID:  "vol-1",
		ClusterID: "cluster-a",
		PodID:     "pod-a",
	}}
	meteringWriter := &fakeHTTPMeteringWriter{}
	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		meteringRepo: meteringWriter,
		regionID:     "aws-us-east-1",
		snapshotMgr:  &fakeHTTPSnapshotManager{},
	}

	req := httptest.NewRequest(http.MethodDelete, "/sandboxvolumes/vol-1?force=true", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.deleteSandboxVolume(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(repo.deletedMounts) != 1 {
		t.Fatalf("deleted mount count = %d, want 1", len(repo.deletedMounts))
	}
	if len(repo.deletedVolume) != 1 || repo.deletedVolume[0] != "vol-1" {
		t.Fatalf("deleted volume = %v, want [vol-1]", repo.deletedVolume)
	}
	if len(meteringWriter.events) != 1 {
		t.Fatalf("event count = %d, want 1", len(meteringWriter.events))
	}
	event := meteringWriter.events[0]
	if event.EventType != metering.EventTypeVolumeDeleted {
		t.Fatalf("event type = %q, want %q", event.EventType, metering.EventTypeVolumeDeleted)
	}
	if event.RegionID != "aws-us-east-1" {
		t.Fatalf("region_id = %q, want %q", event.RegionID, "aws-us-east-1")
	}
	if len(meteringWriter.watermarks) != 1 {
		t.Fatalf("watermark count = %d, want 1", len(meteringWriter.watermarks))
	}
}

func TestDeleteSandboxVolumeCleansIdleDirectMountBeforeDelete(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{
		ID:        "vol-1",
		TeamID:    "team-1",
		UserID:    "user-1",
		CreatedAt: time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC),
	}
	repo.getActiveFunc = func(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*db.VolumeMount, error) {
		if len(repo.deletedMounts) == 0 {
			return []*db.VolumeMount{{
				VolumeID:  "vol-1",
				ClusterID: "cluster-a",
				PodID:     "pod-a",
			}}, nil
		}
		return nil, nil
	}
	volMgr := &fakeHTTPVolumeMountManager{
		cleanupDirectFunc: func(ctx context.Context, volumeID string) (bool, error) {
			repo.deletedMounts = append(repo.deletedMounts, db.VolumeMount{
				VolumeID:  volumeID,
				ClusterID: "cluster-a",
				PodID:     "pod-a",
			})
			return true, nil
		},
	}
	meteringWriter := &fakeHTTPMeteringWriter{}
	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		meteringRepo: meteringWriter,
		regionID:     "aws-us-east-1",
		snapshotMgr:  &fakeHTTPSnapshotManager{},
		volMgr:       volMgr,
	}

	req := httptest.NewRequest(http.MethodDelete, "/sandboxvolumes/vol-1", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.deleteSandboxVolume(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if volMgr.cleanupCalls != 1 || volMgr.lastCleanupVolume != "vol-1" {
		t.Fatalf("cleanup calls = %d volume = %q, want 1 and vol-1", volMgr.cleanupCalls, volMgr.lastCleanupVolume)
	}
	if len(repo.deletedVolume) != 1 || repo.deletedVolume[0] != "vol-1" {
		t.Fatalf("deleted volume = %v, want [vol-1]", repo.deletedVolume)
	}
}

func TestDeleteSandboxVolumeReturnsConflictWhenDirectMountStillInflight(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{
		ID:        "vol-1",
		TeamID:    "team-1",
		UserID:    "user-1",
		CreatedAt: time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC),
	}
	repo.activeMounts["vol-1"] = []*db.VolumeMount{{
		VolumeID:  "vol-1",
		ClusterID: "cluster-a",
		PodID:     "pod-a",
	}}
	volMgr := &fakeHTTPVolumeMountManager{
		cleanupDirectFunc: func(ctx context.Context, volumeID string) (bool, error) {
			return false, nil
		},
	}
	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		regionID:    "aws-us-east-1",
		snapshotMgr: &fakeHTTPSnapshotManager{},
		volMgr:      volMgr,
	}

	req := httptest.NewRequest(http.MethodDelete, "/sandboxvolumes/vol-1", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.deleteSandboxVolume(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if len(repo.deletedVolume) != 0 {
		t.Fatalf("delete should not proceed, got deleted volume %v", repo.deletedVolume)
	}
}
