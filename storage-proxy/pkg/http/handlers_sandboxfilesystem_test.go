package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sirupsen/logrus"
)

type fakeFilesystemRepository struct {
	volumeRepository

	filesystems map[string]*db.SandboxFilesystem
	snapshots   map[string]*db.SandboxFilesystemSnapshot
}

func newFakeFilesystemRepository() *fakeFilesystemRepository {
	return &fakeFilesystemRepository{
		filesystems: make(map[string]*db.SandboxFilesystem),
		snapshots:   make(map[string]*db.SandboxFilesystemSnapshot),
	}
}

func (f *fakeFilesystemRepository) CreateSandboxFilesystem(_ context.Context, fs *db.SandboxFilesystem) error {
	f.filesystems[fs.ID] = cloneTestFilesystem(fs)
	return nil
}

func (f *fakeFilesystemRepository) ListSandboxFilesystemsByTeam(_ context.Context, teamID string) ([]*db.SandboxFilesystem, error) {
	var out []*db.SandboxFilesystem
	for _, fs := range f.filesystems {
		if fs.TeamID == teamID && fs.DeletedAt == nil {
			out = append(out, cloneTestFilesystem(fs))
		}
	}
	return out, nil
}

func (f *fakeFilesystemRepository) GetSandboxFilesystem(_ context.Context, id string) (*db.SandboxFilesystem, error) {
	fs := f.filesystems[id]
	if fs == nil || fs.DeletedAt != nil {
		return nil, db.ErrNotFound
	}
	return cloneTestFilesystem(fs), nil
}

func (f *fakeFilesystemRepository) DeleteSandboxFilesystem(_ context.Context, id string) error {
	fs := f.filesystems[id]
	if fs == nil || fs.DeletedAt != nil {
		return db.ErrNotFound
	}
	now := time.Now().UTC()
	fs.DeletedAt = &now
	fs.State = db.SandboxFilesystemStateDeleted
	return nil
}

func (f *fakeFilesystemRepository) ForkSandboxFilesystem(_ context.Context, sourceID string, fs *db.SandboxFilesystem) error {
	source := f.filesystems[sourceID]
	if source == nil || source.DeletedAt != nil {
		return db.ErrNotFound
	}
	fs.SourceFilesystemID = &source.ID
	fs.BaseImageDigest = source.BaseImageDigest
	fs.S0FSHead = source.S0FSHead
	f.filesystems[fs.ID] = cloneTestFilesystem(fs)
	return nil
}

func (f *fakeFilesystemRepository) CreateSandboxFilesystemSnapshot(_ context.Context, snapshot *db.SandboxFilesystemSnapshot) error {
	f.snapshots[snapshot.ID] = cloneTestFilesystemSnapshot(snapshot)
	return nil
}

func (f *fakeFilesystemRepository) ListSandboxFilesystemSnapshots(_ context.Context, filesystemID, teamID string) ([]*db.SandboxFilesystemSnapshot, error) {
	var out []*db.SandboxFilesystemSnapshot
	for _, snapshot := range f.snapshots {
		if snapshot.FilesystemID == filesystemID && snapshot.TeamID == teamID {
			out = append(out, cloneTestFilesystemSnapshot(snapshot))
		}
	}
	return out, nil
}

func (f *fakeFilesystemRepository) GetSandboxFilesystemSnapshot(_ context.Context, filesystemID, snapshotID, teamID string) (*db.SandboxFilesystemSnapshot, error) {
	snapshot := f.snapshots[snapshotID]
	if snapshot == nil || snapshot.FilesystemID != filesystemID || snapshot.TeamID != teamID {
		return nil, db.ErrNotFound
	}
	return cloneTestFilesystemSnapshot(snapshot), nil
}

func (f *fakeFilesystemRepository) FindSandboxFilesystemSnapshot(_ context.Context, snapshotID, teamID string) (*db.SandboxFilesystemSnapshot, error) {
	snapshot := f.snapshots[snapshotID]
	if snapshot == nil || snapshot.TeamID != teamID {
		return nil, db.ErrNotFound
	}
	return cloneTestFilesystemSnapshot(snapshot), nil
}

func (f *fakeFilesystemRepository) DeleteSandboxFilesystemSnapshot(_ context.Context, filesystemID, snapshotID, teamID string) error {
	snapshot := f.snapshots[snapshotID]
	if snapshot == nil || snapshot.FilesystemID != filesystemID || snapshot.TeamID != teamID {
		return db.ErrNotFound
	}
	delete(f.snapshots, snapshotID)
	return nil
}

func (f *fakeFilesystemRepository) RestoreSandboxFilesystemSnapshot(_ context.Context, filesystemID, snapshotID, teamID string) (*db.SandboxFilesystem, error) {
	fs := f.filesystems[filesystemID]
	snapshot := f.snapshots[snapshotID]
	if fs == nil || snapshot == nil || snapshot.FilesystemID != filesystemID || snapshot.TeamID != teamID {
		return nil, db.ErrNotFound
	}
	if fs.BaseImageDigest != snapshot.BaseImageDigest {
		return nil, db.ErrConflict
	}
	fs.S0FSHead = snapshot.S0FSHead
	fs.State = db.SandboxFilesystemStateAvailable
	return cloneTestFilesystem(fs), nil
}

func TestSandboxFilesystemHandlersCreateSnapshotAndRestore(t *testing.T) {
	repo := newFakeFilesystemRepository()
	server := &Server{repo: repo, logger: logrus.New()}

	createReq := filesystemRequest(http.MethodPost, "/sandboxfilesystems", `{"template":"ubuntu-24.04","base_image_digest":"sha256:base","s0fs_head":"head-1"}`)
	createRec := httptest.NewRecorder()
	server.createSandboxFilesystem(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	if len(repo.filesystems) != 1 {
		t.Fatalf("filesystem count = %d, want 1", len(repo.filesystems))
	}
	var fs *db.SandboxFilesystem
	for _, item := range repo.filesystems {
		fs = item
	}
	if fs.TeamID != "team-1" || fs.UserID != "user-1" || fs.BaseImageDigest != "sha256:base" || fs.S0FSHead != "head-1" {
		t.Fatalf("created filesystem = %+v", fs)
	}

	fs.S0FSHead = "head-2"
	snapshotReq := filesystemRequest(http.MethodPost, "/sandboxfilesystems/"+fs.ID+"/snapshots", `{"name":"before-clean"}`)
	snapshotReq.SetPathValue("id", fs.ID)
	snapshotRec := httptest.NewRecorder()
	server.createSandboxFilesystemSnapshot(snapshotRec, snapshotReq)
	if snapshotRec.Code != http.StatusCreated {
		t.Fatalf("snapshot status = %d, body = %s", snapshotRec.Code, snapshotRec.Body.String())
	}
	if len(repo.snapshots) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(repo.snapshots))
	}
	var snapshot *db.SandboxFilesystemSnapshot
	for _, item := range repo.snapshots {
		snapshot = item
	}

	fs.S0FSHead = "head-3"
	restoreReq := filesystemRequest(http.MethodPost, "/sandboxfilesystems/"+fs.ID+"/snapshots/"+snapshot.ID+"/restore", `{}`)
	restoreReq.SetPathValue("id", fs.ID)
	restoreReq.SetPathValue("snapshot_id", snapshot.ID)
	restoreRec := httptest.NewRecorder()
	server.restoreSandboxFilesystemSnapshot(restoreRec, restoreReq)
	if restoreRec.Code != http.StatusOK {
		t.Fatalf("restore status = %d, body = %s", restoreRec.Code, restoreRec.Body.String())
	}
	if got := repo.filesystems[fs.ID].S0FSHead; got != "head-2" {
		t.Fatalf("restored head = %q, want head-2", got)
	}
}

func filesystemRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader([]byte(body)))
	return req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
}

func cloneTestFilesystem(fs *db.SandboxFilesystem) *db.SandboxFilesystem {
	if fs == nil {
		return nil
	}
	clone := *fs
	return &clone
}

func cloneTestFilesystemSnapshot(snapshot *db.SandboxFilesystemSnapshot) *db.SandboxFilesystemSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	return &clone
}
