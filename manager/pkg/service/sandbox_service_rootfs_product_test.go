package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestSandboxRootFSProductRequiresPausedSandbox(t *testing.T) {
	now := time.Now().UTC()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusRunning, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
		rootFSSnapshots: map[string]*RootFSSnapshot{
			"snapshot-1": {
				ID:              "snapshot-1",
				FilesystemID:    "sandbox-1",
				TeamID:          "team-1",
				SourceSandboxID: "sandbox-1",
				HeadLayerID:     "layer-1",
				CreatedAt:       now,
			},
		},
	}
	svc := rootFSProductTestService(store)

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", nil)
	require.ErrorIs(t, err, ErrSandboxRootFSRequiresPausedSandbox)

	_, err = svc.RestoreSandboxRootFS(context.Background(), "sandbox-1", "team-1", &RestoreSandboxRootFSRequest{SnapshotID: "snapshot-1"})
	require.ErrorIs(t, err, ErrSandboxRootFSRequiresPausedSandbox)

	_, err = svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-1", nil)
	require.ErrorIs(t, err, ErrSandboxRootFSRequiresPausedSandbox)
}

func TestSandboxRootFSProductSnapshotsRestoresAndForksPausedSandbox(t *testing.T) {
	now := time.Now().UTC()
	autoResume := true
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	store.records["sandbox-1"].Config = SandboxConfig{
		AutoResume: &autoResume,
		Services: []SandboxAppService{{
			ID:      "web",
			Port:    8080,
			Ingress: SandboxAppServiceIngress{Public: true},
		}},
	}
	store.records["sandbox-1"].Mounts = []ClaimMount{{
		SandboxVolumeID: "volume-1",
		MountPoint:      "/workspace/data",
	}}
	svc := rootFSProductTestService(store)

	snapshot, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", &CreateSandboxRootFSSnapshotRequest{
		Name:        "before-edit",
		Description: "state before edit",
	})
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, "sandbox-1", snapshot.SandboxID)
	assert.Equal(t, "before-edit", snapshot.Name)

	list, err := svc.ListSandboxRootFSSnapshots(context.Background(), "sandbox-1", "team-1")
	require.NoError(t, err)
	require.NotNil(t, list)
	require.Len(t, list.Snapshots, 1)
	assert.Equal(t, snapshot.ID, list.Snapshots[0].ID)

	loaded, err := svc.GetSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1")
	require.NoError(t, err)
	assert.Equal(t, snapshot.ID, loaded.ID)

	store.rootFSStates["sandbox-1"] = rootFSProductTestState("sandbox-1", "team-1", "layer-v2")
	restoreResp, err := svc.RestoreSandboxRootFS(context.Background(), "sandbox-1", "team-1", &RestoreSandboxRootFSRequest{SnapshotID: snapshot.ID})
	require.NoError(t, err)
	assert.Equal(t, SandboxStatusPaused, restoreResp.Status)
	assert.Equal(t, "layer-v1", store.rootFSStates["sandbox-1"].LayerID)

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)
	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	assert.Equal(t, "sandbox-1", forkResp.SourceSandboxID)
	assert.NotEqual(t, "sandbox-1", forkResp.Sandbox.ID)
	assert.Equal(t, "team-1", forkResp.Sandbox.TeamID)
	assert.Equal(t, "user-2", forkResp.Sandbox.UserID)
	assert.Equal(t, SandboxStatusPaused, forkResp.Sandbox.Status)
	assert.Empty(t, forkResp.Sandbox.Mounts)
	assert.Len(t, forkResp.Sandbox.Services, 1)
	assert.Equal(t, "layer-v1", store.rootFSStates[forkResp.Sandbox.ID].LayerID)

	require.NoError(t, svc.DeleteSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1"))
	_, err = svc.GetSandboxRootFSSnapshot(context.Background(), snapshot.ID, "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)
}

func TestSandboxRootFSProductForkSetsLifecycleExpirations(t *testing.T) {
	claimedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	forkedAt := claimedAt.Add(5 * time.Minute)
	source := rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, claimedAt)
	source.Config.TTL = int32Ptr(900)
	source.Config.HardTTL = int32Ptr(1800)
	source.ExpiresAt = claimedAt.Add(15 * time.Minute)
	source.HardExpiresAt = claimedAt.Add(30 * time.Minute)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": source,
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-v1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: forkedAt}

	forkResp, err := svc.ForkSandbox(context.Background(), "sandbox-1", "team-1", "user-2", nil)

	require.NoError(t, err)
	require.NotNil(t, forkResp)
	require.NotNil(t, forkResp.Sandbox)
	wantExpiresAt := forkedAt.Add(15 * time.Minute)
	wantHardExpiresAt := forkedAt.Add(30 * time.Minute)
	assert.Equal(t, wantExpiresAt, forkResp.Sandbox.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, forkResp.Sandbox.HardExpiresAt)
	stored := store.records[forkResp.Sandbox.ID]
	require.NotNil(t, stored)
	assert.Equal(t, wantExpiresAt, stored.ExpiresAt)
	assert.Equal(t, wantHardExpiresAt, stored.HardExpiresAt)
}

func TestSandboxRootFSProductRejectsExpiredSnapshotExpiration(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
	}
	svc := rootFSProductTestService(store)
	svc.clock = fixedClock{now: now}

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-1", &CreateSandboxRootFSSnapshotRequest{
		Name:      "expired",
		ExpiresAt: now.Add(-time.Second),
	})

	require.ErrorIs(t, err, ErrRootFSSnapshotExpired)
	assert.Empty(t, store.rootFSSnapshots)
}

func TestSandboxRootFSProductEnforcesTeamOwnership(t *testing.T) {
	now := time.Now().UTC()
	store := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": rootFSProductTestRecord("sandbox-1", "team-1", SandboxStatusPaused, now),
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": rootFSProductTestState("sandbox-1", "team-1", "layer-1"),
		},
	}
	svc := rootFSProductTestService(store)

	_, err := svc.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-1", "team-2", nil)
	require.True(t, apierrors.IsForbidden(err), "error = %v", err)
}

func (s *memorySandboxStore) CreateRootFSSnapshot(_ context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	if s.rootFSSnapshots == nil {
		s.rootFSSnapshots = make(map[string]*RootFSSnapshot)
	}
	state := s.rootFSStates[req.SandboxID]
	if state == nil || state.LayerID == "" {
		return nil, ErrRootFSFilesystemNotFound
	}
	record := s.records[req.SandboxID]
	if record == nil {
		return nil, ErrSandboxRecordNotFound
	}
	snapshot := &RootFSSnapshot{
		ID:              req.SnapshotID,
		FilesystemID:    req.SandboxID,
		TeamID:          record.TeamID,
		SourceSandboxID: req.SandboxID,
		HeadLayerID:     state.LayerID,
		Name:            req.Name,
		Description:     req.Description,
		CreatedAt:       time.Now().UTC(),
		ExpiresAt:       req.ExpiresAt,
	}
	s.rootFSSnapshots[snapshot.ID] = cloneRootFSSnapshotForTest(snapshot)
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) ListRootFSSnapshots(_ context.Context, req *ListRootFSSnapshotsRequest) ([]*RootFSSnapshot, error) {
	var snapshots []*RootFSSnapshot
	for _, snapshot := range s.rootFSSnapshots {
		if snapshot == nil || snapshot.SourceSandboxID != req.SandboxID {
			continue
		}
		if req.TeamID != "" && snapshot.TeamID != req.TeamID {
			continue
		}
		snapshots = append(snapshots, cloneRootFSSnapshotForTest(snapshot))
	}
	return snapshots, nil
}

func (s *memorySandboxStore) GetRootFSSnapshot(_ context.Context, snapshotID, teamID string) (*RootFSSnapshot, error) {
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return nil, ErrRootFSSnapshotNotFound
	}
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) DeleteRootFSSnapshot(_ context.Context, snapshotID, teamID string) error {
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return ErrRootFSSnapshotNotFound
	}
	delete(s.rootFSSnapshots, snapshotID)
	return nil
}

func (s *memorySandboxStore) ForkRootFSFilesystem(_ context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	sourceState := s.rootFSStates[req.SourceSandboxID]
	if sourceState == nil || sourceState.LayerID == "" {
		return nil, ErrRootFSFilesystemNotFound
	}
	target := s.records[req.TargetSandboxID]
	if target == nil {
		return nil, ErrSandboxRecordNotFound
	}
	targetTeamID := req.TargetTeamID
	if targetTeamID == "" {
		targetTeamID = target.TeamID
	}
	state := cloneSandboxRootFSState(sourceState)
	state.SandboxID = req.TargetSandboxID
	state.TeamID = targetTeamID
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[req.TargetSandboxID] = state
	if s.rootFSFilesystems == nil {
		s.rootFSFilesystems = make(map[string]*RootFSFilesystem)
	}
	filesystem := &RootFSFilesystem{
		ID:                 req.TargetSandboxID,
		TeamID:             targetTeamID,
		SourceFilesystemID: req.SourceSandboxID,
		HeadLayerID:        sourceState.LayerID,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	s.rootFSFilesystems[filesystem.ID] = cloneRootFSFilesystemForTest(filesystem)
	return cloneRootFSFilesystemForTest(filesystem), nil
}

func (s *memorySandboxStore) RestoreRootFSFromSnapshot(_ context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	snapshot, err := s.GetRootFSSnapshot(context.Background(), req.SnapshotID, req.TeamID)
	if err != nil {
		return nil, err
	}
	target := s.records[req.SandboxID]
	if target == nil {
		return nil, ErrSandboxRecordNotFound
	}
	if s.rootFSStates == nil {
		s.rootFSStates = make(map[string]*SandboxRootFSState)
	}
	s.rootFSStates[req.SandboxID] = rootFSProductTestState(req.SandboxID, target.TeamID, snapshot.HeadLayerID)
	filesystem := &RootFSFilesystem{
		ID:                 req.SandboxID,
		TeamID:             target.TeamID,
		SourceFilesystemID: snapshot.FilesystemID,
		HeadLayerID:        snapshot.HeadLayerID,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	return filesystem, nil
}

func rootFSProductTestService(store *memorySandboxStore) *SandboxService {
	return &SandboxService{
		sandboxStore: store,
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}
}

func rootFSProductTestRecord(id, teamID, status string, now time.Time) *SandboxRecord {
	return &SandboxRecord{
		ID:                id,
		TeamID:            teamID,
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		ClusterID:         "default",
		Status:            status,
		TemplateSpec:      v1alpha1.SandboxTemplateSpec{},
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

func rootFSProductTestState(sandboxID, teamID, layerID string) *SandboxRootFSState {
	manifestKey := "manifests/" + layerID + ".json"
	return &SandboxRootFSState{
		LayerID:           layerID,
		SandboxID:         sandboxID,
		TeamID:            teamID,
		RuntimeGeneration: 1,
		Runtime:           "runc",
		BaseImageRef:      "docker.io/library/busybox:1.36",
		BaseImageDigest:   "sha256:base",
		Snapshotter:       "overlayfs",
		StorageEngine:     ctldapi.RootFSStorageEngineS0FS,
		DiffDigest:        "s0fs:" + manifestKey,
		DiffMediaType:     "application/vnd.sandbox0.rootfs.s0fs.v1+json",
		DiffObjectKey:     manifestKey,
		S0FSVolumeID:      sandboxID,
		S0FSManifestKey:   manifestKey,
		S0FSManifestSeq:   1,
		S0FSCheckpointSeq: 1,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
}

func cloneRootFSSnapshotForTest(snapshot *RootFSSnapshot) *RootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	return &clone
}

func cloneRootFSFilesystemForTest(filesystem *RootFSFilesystem) *RootFSFilesystem {
	if filesystem == nil {
		return nil
	}
	clone := *filesystem
	return &clone
}

var _ SandboxRootFSProductStore = (*memorySandboxStore)(nil)
