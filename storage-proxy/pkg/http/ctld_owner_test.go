package http

import (
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
