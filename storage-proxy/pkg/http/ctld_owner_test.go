package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func TestReleaseReleasableCtldVolumeOwnersCallsOwningCtld(t *testing.T) {
	releaseCalls := 0
	ctldServer, ownerPort := newReleaseOwnerTestServer(t, http.StatusOK, ctldapi.ReleaseVolumeOwnerResponse{Released: true}, &releaseCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.activeMounts["vol-1"] = []*db.VolumeMount{ctldOwnerMount(t, "vol-1", "cluster-a", "sandbox0-system/ctld-a", ownerPort)}
	server := &Server{
		repo:          repo,
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	if err := server.releaseReleasableCtldVolumeOwners(context.Background(), "vol-1"); err != nil {
		t.Fatalf("releaseReleasableCtldVolumeOwners() error = %v", err)
	}
	if releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", releaseCalls)
	}
}

func TestRestoreSnapshotReleasesCtldOwnerBeforeRestore(t *testing.T) {
	releaseCalls := 0
	ctldServer, ownerPort := newReleaseOwnerTestServer(t, http.StatusOK, ctldapi.ReleaseVolumeOwnerResponse{Released: true}, &releaseCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.getActiveFunc = func(_ context.Context, volumeID string, _ int) ([]*db.VolumeMount, error) {
		if releaseCalls > 0 {
			return nil, nil
		}
		return []*db.VolumeMount{ctldOwnerMount(t, volumeID, "cluster-a", "sandbox0-system/ctld-a", ownerPort)}, nil
	}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   snapshotMgr,
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots/snap-1/restore", nil)
	req.SetPathValue("volume_id", "vol-1")
	req.SetPathValue("snapshot_id", "snap-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.restoreSnapshot(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", releaseCalls)
	}
	if snapshotMgr.lastRestore == nil || snapshotMgr.lastRestore.VolumeID != "vol-1" {
		t.Fatalf("RestoreSnapshot request = %+v, want volume vol-1", snapshotMgr.lastRestore)
	}
}

func TestForkVolumeReleasesCtldOwnerBeforeFork(t *testing.T) {
	releaseCalls := 0
	ctldServer, ownerPort := newReleaseOwnerTestServer(t, http.StatusOK, ctldapi.ReleaseVolumeOwnerResponse{Released: true}, &releaseCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.getActiveFunc = func(_ context.Context, volumeID string, _ int) ([]*db.VolumeMount, error) {
		if releaseCalls > 0 {
			return nil, nil
		}
		return []*db.VolumeMount{ctldOwnerMount(t, volumeID, "cluster-a", "sandbox0-system/ctld-a", ownerPort)}, nil
	}
	snapshotMgr := &captureForkSnapshotManager{fakeHTTPSnapshotManager: &fakeHTTPSnapshotManager{}}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   snapshotMgr,
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/fork", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.forkVolume(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", releaseCalls)
	}
	if snapshotMgr.lastFork == nil || snapshotMgr.lastFork.SourceVolumeID != "vol-1" {
		t.Fatalf("ForkVolume request = %+v, want source vol-1", snapshotMgr.lastFork)
	}
}

func TestCreateSnapshotPreparesCtldCheckpoint(t *testing.T) {
	checkpointCalls := map[string]int{}
	ctldServer, ownerPort := newSnapshotCheckpointTestServer(t, checkpointCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1", UserID: "user-1"}
	repo.activeMounts["vol-1"] = []*db.VolumeMount{ctldOwnerMount(t, "vol-1", "cluster-a", "sandbox0-system/ctld-a", ownerPort)}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   snapshotMgr,
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots", bytes.NewReader([]byte(`{"name":" checkpoint "}`)))
	req.SetPathValue("volume_id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSnapshot(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if checkpointCalls["prepare"] != 1 || checkpointCalls["complete"] != 1 || checkpointCalls["abort"] != 0 {
		t.Fatalf("checkpoint calls = %v, want prepare=1 complete=1 abort=0", checkpointCalls)
	}
	if snapshotMgr.lastCreate == nil || !snapshotMgr.lastCreate.ActiveCheckpointPrepared {
		t.Fatalf("CreateSnapshot request = %+v, want active checkpoint prepared", snapshotMgr.lastCreate)
	}
	if snapshotMgr.lastCreate.Name != "checkpoint" {
		t.Fatalf("CreateSnapshot name = %q, want normalized checkpoint", snapshotMgr.lastCreate.Name)
	}
}

func TestCreateSnapshotRejectsWhitespaceName(t *testing.T) {
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots", bytes.NewReader([]byte(`{"name":"   "}`)))
	req.SetPathValue("volume_id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSnapshot(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if snapshotMgr.lastCreate != nil {
		t.Fatalf("CreateSnapshot request = %+v, want nil for invalid name", snapshotMgr.lastCreate)
	}
}

func TestCreateSnapshotAbortsCtldCheckpointOnSnapshotError(t *testing.T) {
	checkpointCalls := map[string]int{}
	ctldServer, ownerPort := newSnapshotCheckpointTestServer(t, checkpointCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1", UserID: "user-1"}
	repo.activeMounts["vol-1"] = []*db.VolumeMount{ctldOwnerMount(t, "vol-1", "cluster-a", "sandbox0-system/ctld-a", ownerPort)}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   &fakeHTTPSnapshotManager{createErr: snapshot.ErrCloneFailed},
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots", bytes.NewReader([]byte(`{"name":"checkpoint"}`)))
	req.SetPathValue("volume_id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSnapshot(recorder, req)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if checkpointCalls["prepare"] != 1 || checkpointCalls["complete"] != 0 || checkpointCalls["abort"] != 1 {
		t.Fatalf("checkpoint calls = %v, want prepare=1 complete=0 abort=1", checkpointCalls)
	}
}

func TestCreateSnapshotRejectsActiveRWXWritableMount(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1", UserID: "user-1", AccessMode: string(volume.AccessModeRWX)}
	repo.activeMounts["vol-1"] = []*db.VolumeMount{{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "storage-proxy-a",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWX, OwnerKind: volume.OwnerKindStorageProxy}),
	}}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots", bytes.NewReader([]byte(`{"name":"rwx"}`)))
	req.SetPathValue("volume_id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSnapshot(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if snapshotMgr.lastCreate != nil {
		t.Fatalf("CreateSnapshot request = %+v, want nil for active RWX", snapshotMgr.lastCreate)
	}
	_, apiErr, err := spec.DecodeResponse[map[string]any](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "active RWX volume snapshots are not supported" {
		t.Fatalf("api error = %+v, want active RWX conflict", apiErr)
	}
}

func TestDeleteSandboxVolumeReleasesCtldOwnerBeforeActiveMountCheck(t *testing.T) {
	releaseCalls := 0
	ctldServer, ownerPort := newReleaseOwnerTestServer(t, http.StatusOK, ctldapi.ReleaseVolumeOwnerResponse{Released: true}, &releaseCalls)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1", UserID: "user-1"}
	repo.getActiveFunc = func(_ context.Context, volumeID string, _ int) ([]*db.VolumeMount, error) {
		if releaseCalls > 0 {
			return nil, nil
		}
		return []*db.VolumeMount{ctldOwnerMount(t, volumeID, "cluster-a", "sandbox0-system/ctld-a", ownerPort)}, nil
	}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   &fakeHTTPSnapshotManager{},
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodDelete, "/sandboxvolumes/vol-1", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.deleteSandboxVolume(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", releaseCalls)
	}
	if len(repo.deletedVolume) != 1 || repo.deletedVolume[0] != "vol-1" {
		t.Fatalf("deleted volume = %v, want [vol-1]", repo.deletedVolume)
	}
}

func TestRestoreSnapshotReturnsConflictWhenCtldOwnerIsBusy(t *testing.T) {
	ctldServer, ownerPort := newReleaseOwnerTestServer(t, http.StatusConflict, ctldapi.ReleaseVolumeOwnerResponse{Busy: true, Error: "volume vol-1 is actively bound to a portal"}, nil)
	defer ctldServer.Close()

	repo := newFakeHTTPRepo()
	repo.activeMounts["vol-1"] = []*db.VolumeMount{ctldOwnerMount(t, "vol-1", "cluster-a", "sandbox0-system/ctld-a", ownerPort)}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:        logrus.New(),
		repo:          repo,
		snapshotMgr:   snapshotMgr,
		podResolver:   &fakeVolumeFilePodResolver{urls: map[string]string{"sandbox0-system/ctld-a": ctldServer.URL}},
		selfClusterID: "cluster-a",
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/snapshots/snap-1/restore", nil)
	req.SetPathValue("volume_id", "vol-1")
	req.SetPathValue("snapshot_id", "snap-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.restoreSnapshot(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if snapshotMgr.lastRestore != nil {
		t.Fatalf("RestoreSnapshot request = %+v, want nil when owner is busy", snapshotMgr.lastRestore)
	}
	_, apiErr, err := spec.DecodeResponse[map[string]any](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "ctld-mounted volumes must be unmounted before snapshot or restore" {
		t.Fatalf("api error = %+v, want ctld-mounted conflict", apiErr)
	}
}

func ctldOwnerMount(t *testing.T, volumeID, clusterID, podID string, ownerPort int) *db.VolumeMount {
	t.Helper()
	return &db.VolumeMount{
		VolumeID:  volumeID,
		ClusterID: clusterID,
		PodID:     podID,
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{
			AccessMode: volume.AccessModeRWO,
			OwnerKind:  volume.OwnerKindCtld,
			OwnerPort:  ownerPort,
		}),
	}
}

func newReleaseOwnerTestServer(t *testing.T, status int, resp ctldapi.ReleaseVolumeOwnerResponse, calls *int) (*httptest.Server, int) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/volume-portals/owners/release" {
			t.Fatalf("ctld release path = %q, want /api/v1/volume-portals/owners/release", r.URL.Path)
		}
		if calls != nil {
			*calls = *calls + 1
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	return server, mustHTTPServerPort(t, server)
}

func newSnapshotCheckpointTestServer(t *testing.T, calls map[string]int) (*httptest.Server, int) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/volume-portals/snapshot-checkpoints/prepare":
			calls["prepare"]++
			_ = json.NewEncoder(w).Encode(ctldapi.PrepareVolumeSnapshotCheckpointResponse{Prepared: true})
		case "/api/v1/volume-portals/snapshot-checkpoints/complete":
			calls["complete"]++
			_ = json.NewEncoder(w).Encode(ctldapi.CompleteVolumeSnapshotCheckpointResponse{Completed: true})
		case "/api/v1/volume-portals/snapshot-checkpoints/abort":
			calls["abort"]++
			_ = json.NewEncoder(w).Encode(ctldapi.AbortVolumeSnapshotCheckpointResponse{Aborted: true})
		default:
			t.Fatalf("ctld checkpoint path = %q", r.URL.Path)
		}
	}))
	return server, mustHTTPServerPort(t, server)
}

func mustHTTPServerPort(t *testing.T, server *httptest.Server) int {
	t.Helper()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split test server host: %v", err)
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return parsedPort
}
