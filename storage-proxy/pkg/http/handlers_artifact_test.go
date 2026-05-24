package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

func TestCreateArtifactSnapshotsSourceVolume(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["source-vol"] = &db.SandboxVolume{
		ID:         "source-vol",
		TeamID:     "team-a",
		UserID:     "user-a",
		AccessMode: string(volume.AccessModeRWX),
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		snapshotMgr: snapshotMgr,
	}

	body := bytes.NewBufferString(`{
		"name":"next-bundle",
		"kind":"nextjs",
		"media_type":"application/vnd.sandbox0.volume-snapshot",
		"digest":"sha256:abc",
		"source":{"type":"sandbox_volume","sandboxvolume_id":"source-vol"},
		"metadata":{"entrypoint":"server.js"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/artifacts", body)
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a", UserID: "user-a"}))
	recorder := httptest.NewRecorder()

	server.createArtifact(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	artifact, apiErr, err := spec.DecodeResponse[db.Artifact](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("api error: %+v", apiErr)
	}
	if artifact.Name != "next-bundle" || artifact.Kind != "nextjs" || artifact.SourceVolumeID != "source-vol" || artifact.SnapshotID != "snap-1" {
		t.Fatalf("artifact = %+v, want stored artifact snapshot", artifact)
	}
	if snapshotMgr.lastCreate == nil || snapshotMgr.lastCreate.VolumeID != "source-vol" || snapshotMgr.lastCreate.TeamID != "team-a" {
		t.Fatalf("snapshot request = %+v, want source volume snapshot", snapshotMgr.lastCreate)
	}
	stored := repo.artifacts[artifact.ID]
	if stored == nil || string(stored.Metadata) != `{"entrypoint":"server.js"}` {
		t.Fatalf("stored artifact = %+v, want metadata", stored)
	}
}

func TestCreateVolumeFromArtifactUsesReadOnlyDefaults(t *testing.T) {
	zero := int64(0)
	repo := newFakeHTTPRepo()
	repo.artifacts["artifact-1"] = &db.Artifact{
		ID:             "artifact-1",
		TeamID:         "team-a",
		UserID:         "user-a",
		Name:           "bundle",
		Kind:           "generic",
		MediaType:      "application/octet-stream",
		SourceVolumeID: "source-vol",
		SnapshotID:     "snap-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		repo:        repo,
		snapshotMgr: snapshotMgr,
	}
	req := httptest.NewRequest(http.MethodPost, "/artifacts/artifact-1/volume", bytes.NewBufferString(`{}`))
	req.SetPathValue("id", "artifact-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a", UserID: "user-a"}))
	recorder := httptest.NewRecorder()

	server.createVolumeFromArtifact(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if snapshotMgr.lastCreateVolume == nil {
		t.Fatal("CreateVolumeFromSnapshot was not called")
	}
	if snapshotMgr.lastCreateVolume.SnapshotID != "snap-1" || snapshotMgr.lastCreateVolume.AccessMode != string(volume.AccessModeROX) {
		t.Fatalf("create volume request = %+v, want ROX snap-1", snapshotMgr.lastCreateVolume)
	}
	if snapshotMgr.lastCreateVolume.DefaultPosixUID == nil || *snapshotMgr.lastCreateVolume.DefaultPosixUID != zero {
		t.Fatalf("default uid = %v, want 0", snapshotMgr.lastCreateVolume.DefaultPosixUID)
	}
	if snapshotMgr.lastCreateVolume.DefaultPosixGID == nil || *snapshotMgr.lastCreateVolume.DefaultPosixGID != zero {
		t.Fatalf("default gid = %v, want 0", snapshotMgr.lastCreateVolume.DefaultPosixGID)
	}
}

func TestGetArtifactHidesOtherTeams(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.artifacts["artifact-1"] = &db.Artifact{
		ID:             "artifact-1",
		TeamID:         "team-a",
		UserID:         "user-a",
		Name:           "bundle",
		Kind:           "generic",
		MediaType:      "application/octet-stream",
		SourceVolumeID: "source-vol",
		SnapshotID:     "snap-1",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	server := &Server{logger: logrus.New(), repo: repo}
	req := httptest.NewRequest(http.MethodGet, "/artifacts/artifact-1", nil)
	req.SetPathValue("id", "artifact-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-b", UserID: "user-b"}))
	recorder := httptest.NewRecorder()

	server.getArtifact(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}
