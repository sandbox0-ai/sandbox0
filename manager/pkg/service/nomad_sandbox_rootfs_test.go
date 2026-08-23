package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestNomadSandboxRootFSServiceManagesPausedBlockSnapshots(t *testing.T) {
	now := time.Date(2026, time.August, 21, 5, 0, 0, 0, time.UTC)
	store := newNomadRootFSTestStore(now)
	service, err := NewNomadSandboxRootFSService(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	created, err := service.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-a", "team-a", &CreateSandboxRootFSSnapshotRequest{
		Name: "checkpoint", Description: "before edit", ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateSandboxRootFSSnapshot() error = %v", err)
	}
	if created.ID == "" || created.SandboxID != "sandbox-a" || created.Name != "checkpoint" {
		t.Fatalf("created snapshot = %+v", created)
	}
	listed, err := service.ListSandboxRootFSSnapshots(context.Background(), "sandbox-a", "team-a")
	if err != nil {
		t.Fatalf("ListSandboxRootFSSnapshots() error = %v", err)
	}
	if listed.Count != 1 || len(listed.Snapshots) != 1 || listed.Snapshots[0].ID != created.ID {
		t.Fatalf("snapshot list = %+v", listed)
	}
	got, err := service.GetSandboxRootFSSnapshot(context.Background(), created.ID, "team-a")
	if err != nil || got.ID != created.ID {
		t.Fatalf("GetSandboxRootFSSnapshot() = %+v, %v", got, err)
	}
	restored, err := service.RestoreSandboxRootFS(context.Background(), "sandbox-b", "team-a", &RestoreSandboxRootFSRequest{SnapshotID: created.ID})
	if err != nil {
		t.Fatalf("RestoreSandboxRootFS() error = %v", err)
	}
	if restored.SandboxID != "sandbox-b" || restored.SnapshotID != created.ID {
		t.Fatalf("restore response = %+v", restored)
	}
	filesystem, err := store.GetRootFSFilesystem(context.Background(), "sandbox-b")
	if err != nil || filesystem.HeadGenerationID != "generation-a" {
		t.Fatalf("restored filesystem = %+v, %v", filesystem, err)
	}
	if err := service.DeleteSandboxRootFSSnapshot(context.Background(), created.ID, "team-a"); err != nil {
		t.Fatalf("DeleteSandboxRootFSSnapshot() error = %v", err)
	}
	if _, err := service.GetSandboxRootFSSnapshot(context.Background(), created.ID, "team-a"); !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		t.Fatalf("deleted snapshot error = %v", err)
	}
}

func TestNomadSandboxRootFSServiceFailsClosed(t *testing.T) {
	now := time.Date(2026, time.August, 21, 5, 0, 0, 0, time.UTC)
	if _, err := NewNomadSandboxRootFSService(nil, nil); err == nil {
		t.Fatal("nil rootfs store was accepted")
	}
	store := newNomadRootFSTestStore(now)
	service, err := NewNomadSandboxRootFSService(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-a", "team-a", &CreateSandboxRootFSSnapshotRequest{ExpiresAt: now}); !errors.Is(err, ErrRootFSSnapshotExpired) {
		t.Fatalf("expired snapshot error = %v", err)
	}
	store.records["sandbox-a"].DesiredState = sandboxstore.SandboxDesiredStateActive
	if _, err := service.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-a", "team-a", nil); !errors.Is(err, ErrSandboxRootFSRequiresPausedSandbox) {
		t.Fatalf("active snapshot error = %v", err)
	}
	store.records["sandbox-a"].DesiredState = sandboxstore.SandboxDesiredStatePaused
	store.lifecycleTxns["resume-a"] = &sandboxstore.SandboxLifecycleTxn{
		ID: "resume-a", SandboxID: "sandbox-a", Kind: sandboxstore.SandboxLifecycleKindResume,
		Phase: sandboxstore.SandboxLifecyclePhasePreparing,
	}
	if _, err := service.CreateSandboxRootFSSnapshot(context.Background(), "sandbox-a", "team-a", nil); !apierrors.IsConflict(err) {
		t.Fatalf("lifecycle snapshot error = %v", err)
	}
	delete(store.lifecycleTxns, "resume-a")
	store.records["sandbox-a"].RuntimeBackend = "kubernetes"
	if _, err := service.ListSandboxRootFSSnapshots(context.Background(), "sandbox-a", "team-a"); !apierrors.IsConflict(err) {
		t.Fatalf("foreign runtime list error = %v", err)
	}
	store.records["sandbox-a"].RuntimeBackend = sandboxstore.SandboxRuntimeBackendNomad
	if _, err := service.ListSandboxRootFSSnapshots(context.Background(), "sandbox-a", "team-b"); !apierrors.IsForbidden(err) {
		t.Fatalf("cross-team list error = %v", err)
	}

	store.rootFSSnapshots["legacy-a"] = &sandboxstore.RootFSSnapshot{
		ID: "legacy-a", TeamID: "team-a", SourceSandboxID: "sandbox-a",
		StorageFormat: sandboxstore.RootFSStorageFormatLegacyLayer, HeadLayerID: "layer-a",
	}
	if _, err := service.GetSandboxRootFSSnapshot(context.Background(), "legacy-a", "team-a"); !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		t.Fatalf("legacy snapshot get error = %v", err)
	}
	if err := service.DeleteSandboxRootFSSnapshot(context.Background(), "legacy-a", "team-a"); !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		t.Fatalf("legacy snapshot delete error = %v", err)
	}
	if _, err := service.GetSandboxRootFSSnapshot(context.Background(), template.BuildSnapshotID("build-a"), "team-a"); !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		t.Fatalf("internal snapshot get error = %v", err)
	}

	store.records["sandbox-b"].DesiredState = sandboxstore.SandboxDesiredStateActive
	store.rootFSSnapshots["snapshot-a"] = &sandboxstore.RootFSSnapshot{
		ID: "snapshot-a", FilesystemID: "filesystem-a", TeamID: "team-a", SourceSandboxID: "sandbox-a",
		StorageFormat: sandboxstore.RootFSStorageFormatBlockCOWV1, HeadGenerationID: "generation-a",
	}
	if _, err := service.RestoreSandboxRootFS(context.Background(), "sandbox-b", "team-a", &RestoreSandboxRootFSRequest{SnapshotID: "snapshot-a"}); !errors.Is(err, ErrSandboxRootFSRequiresPausedSandbox) {
		t.Fatalf("active restore error = %v", err)
	}
}

type nomadRootFSTestStore struct {
	*memorySandboxStore
}

func newNomadRootFSTestStore(now time.Time) *nomadRootFSTestStore {
	return &nomadRootFSTestStore{memorySandboxStore: &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID: "sandbox-a", TeamID: "team-a", RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
				DesiredState: sandboxstore.SandboxDesiredStatePaused, CreatedAt: now, UpdatedAt: now,
			},
			"sandbox-b": {
				ID: "sandbox-b", TeamID: "team-a", RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
				DesiredState: sandboxstore.SandboxDesiredStatePaused, CreatedAt: now, UpdatedAt: now,
			},
		},
		lifecycleTxns:   map[string]*sandboxstore.SandboxLifecycleTxn{},
		rootFSSnapshots: map[string]*sandboxstore.RootFSSnapshot{},
		rootFSFilesystems: map[string]*sandboxstore.RootFSFilesystem{
			"sandbox-a": {
				ID: "filesystem-a", TeamID: "team-a", StorageFormat: sandboxstore.RootFSStorageFormatBlockCOWV1,
				HeadGenerationID: "generation-a",
			},
			"sandbox-b": {
				ID: "filesystem-b", TeamID: "team-a", StorageFormat: sandboxstore.RootFSStorageFormatBlockCOWV1,
				HeadGenerationID: "generation-b",
			},
		},
	}}
}

func (s *nomadRootFSTestStore) GetRootFSFilesystem(_ context.Context, sandboxID string) (*sandboxstore.RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	filesystem := s.rootFSFilesystems[sandboxID]
	if filesystem == nil {
		return nil, sandboxstore.ErrRootFSFilesystemNotFound
	}
	return cloneRootFSFilesystemForTest(filesystem), nil
}

func (s *nomadRootFSTestStore) CreateRootFSSnapshot(_ context.Context, request *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[request.SandboxID]
	filesystem := s.rootFSFilesystems[request.SandboxID]
	if record == nil || filesystem == nil || filesystem.StorageFormat != sandboxstore.RootFSStorageFormatBlockCOWV1 || filesystem.HeadGenerationID == "" {
		return nil, sandboxstore.ErrRootFSFilesystemNotFound
	}
	snapshot := &sandboxstore.RootFSSnapshot{
		ID: request.SnapshotID, FilesystemID: filesystem.ID, TeamID: record.TeamID,
		SourceSandboxID: request.SandboxID, HeadGenerationID: filesystem.HeadGenerationID,
		StorageFormat: filesystem.StorageFormat, BaseArtifactDigest: filesystem.BaseArtifactDigest,
		FormatGeneration: filesystem.FormatGeneration, Name: request.Name,
		Description: request.Description, CreatedAt: time.Now().UTC(), ExpiresAt: request.ExpiresAt,
	}
	s.rootFSSnapshots[snapshot.ID] = cloneRootFSSnapshotForTest(snapshot)
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *nomadRootFSTestStore) RestoreRootFSFromSnapshot(_ context.Context, request *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[request.SnapshotID]
	record := s.records[request.SandboxID]
	if snapshot == nil || snapshot.TeamID != request.TeamID {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	if record == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	filesystem := &sandboxstore.RootFSFilesystem{
		ID: request.SandboxID, TeamID: record.TeamID, SourceFilesystemID: snapshot.FilesystemID,
		HeadGenerationID: snapshot.HeadGenerationID, StorageFormat: snapshot.StorageFormat,
		BaseArtifactDigest: snapshot.BaseArtifactDigest, FormatGeneration: snapshot.FormatGeneration,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	s.rootFSFilesystems[request.SandboxID] = cloneRootFSFilesystemForTest(filesystem)
	return cloneRootFSFilesystemForTest(filesystem), nil
}
