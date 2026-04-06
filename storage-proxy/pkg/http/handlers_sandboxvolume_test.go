package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sirupsen/logrus"
)

type captureForkSnapshotManager struct {
	*fakeHTTPSnapshotManager
	lastFork *snapshot.ForkVolumeRequest
	forkResp *db.SandboxVolume
}

func (m *captureForkSnapshotManager) ForkVolume(_ context.Context, req *snapshot.ForkVolumeRequest) (*db.SandboxVolume, error) {
	m.lastFork = req
	if m.forkResp != nil {
		return m.forkResp, nil
	}
	return &db.SandboxVolume{ID: "fork-1", TeamID: req.TeamID, UserID: req.UserID}, nil
}

func TestCreateSandboxVolumeStoresDefaultPosixIdentity(t *testing.T) {
	repo := newFakeHTTPRepo()
	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		meteringRepo: &fakeHTTPMeteringWriter{},
		snapshotMgr:  &fakeHTTPSnapshotManager{},
	}

	uid := int64(1001)
	gid := int64(2002)
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", bytes.NewReader([]byte(`{"cache_size":"2G","default_posix_uid":1001,"default_posix_gid":2002}`)))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSandboxVolume(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if len(repo.createdVolumes) != 1 {
		t.Fatalf("created volumes = %d, want 1", len(repo.createdVolumes))
	}
	created := repo.createdVolumes[0]
	if created.DefaultPosixUID == nil || *created.DefaultPosixUID != uid {
		t.Fatalf("DefaultPosixUID = %v, want %d", created.DefaultPosixUID, uid)
	}
	if created.DefaultPosixGID == nil || *created.DefaultPosixGID != gid {
		t.Fatalf("DefaultPosixGID = %v, want %d", created.DefaultPosixGID, gid)
	}

	resp, apiErr, err := spec.DecodeResponse[db.SandboxVolume](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.DefaultPosixUID == nil || *resp.DefaultPosixUID != uid {
		t.Fatalf("response DefaultPosixUID = %v, want %d", resp.DefaultPosixUID, uid)
	}
	if resp.DefaultPosixGID == nil || *resp.DefaultPosixGID != gid {
		t.Fatalf("response DefaultPosixGID = %v, want %d", resp.DefaultPosixGID, gid)
	}
}

func TestCreateSandboxVolumeDefaultsPosixIdentityToRoot(t *testing.T) {
	repo := newFakeHTTPRepo()
	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		meteringRepo: &fakeHTTPMeteringWriter{},
		snapshotMgr:  &fakeHTTPSnapshotManager{},
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", bytes.NewReader([]byte(`{"cache_size":"2G"}`)))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSandboxVolume(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if len(repo.createdVolumes) != 1 {
		t.Fatalf("created volumes = %d, want 1", len(repo.createdVolumes))
	}
	created := repo.createdVolumes[0]
	if created.DefaultPosixUID == nil || *created.DefaultPosixUID != 0 {
		t.Fatalf("DefaultPosixUID = %v, want 0", created.DefaultPosixUID)
	}
	if created.DefaultPosixGID == nil || *created.DefaultPosixGID != 0 {
		t.Fatalf("DefaultPosixGID = %v, want 0", created.DefaultPosixGID)
	}
}

func TestCreateSandboxVolumeRejectsPartialDefaultPosixIdentity(t *testing.T) {
	server := &Server{
		logger:       logrus.New(),
		repo:         newFakeHTTPRepo(),
		meteringRepo: &fakeHTTPMeteringWriter{},
		snapshotMgr:  &fakeHTTPSnapshotManager{},
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", bytes.NewReader([]byte(`{"default_posix_uid":1001}`)))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.createSandboxVolume(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestForkVolumePassesDefaultPosixIdentity(t *testing.T) {
	snapshotMgr := &captureForkSnapshotManager{fakeHTTPSnapshotManager: &fakeHTTPSnapshotManager{}}
	server := &Server{
		logger:      logrus.New(),
		repo:        newFakeHTTPRepo(),
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/fork", bytes.NewReader([]byte(`{"default_posix_uid":1001,"default_posix_gid":2002}`)))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	recorder := httptest.NewRecorder()

	server.forkVolume(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if snapshotMgr.lastFork == nil {
		t.Fatalf("ForkVolume request was not captured")
	}
	if snapshotMgr.lastFork.DefaultPosixUID == nil || *snapshotMgr.lastFork.DefaultPosixUID != 1001 {
		t.Fatalf("fork DefaultPosixUID = %v, want 1001", snapshotMgr.lastFork.DefaultPosixUID)
	}
	if snapshotMgr.lastFork.DefaultPosixGID == nil || *snapshotMgr.lastFork.DefaultPosixGID != 2002 {
		t.Fatalf("fork DefaultPosixGID = %v, want 2002", snapshotMgr.lastFork.DefaultPosixGID)
	}
}
