package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
)

type createSandboxFilesystemRequest struct {
	Template        string `json:"template,omitempty"`
	SnapshotID      string `json:"snapshot_id,omitempty"`
	BaseImageDigest string `json:"base_image_digest,omitempty"`
	S0FSHead        string `json:"s0fs_head,omitempty"`
}

type forkSandboxFilesystemRequest struct {
	Template string `json:"template,omitempty"`
}

type createSandboxFilesystemSnapshotRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

func (s *Server) filesystemRepo(w http.ResponseWriter) (filesystemRepository, bool) {
	repo, ok := s.repo.(filesystemRepository)
	if !ok || repo == nil {
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "sandbox filesystem repository is not configured")
		return nil, false
	}
	return repo, true
}

func (s *Server) filesystemSnapshotManager() (filesystemSnapshotManager, bool) {
	mgr, ok := s.snapshotMgr.(filesystemSnapshotManager)
	return mgr, ok && mgr != nil
}

func filesystemClaims(w http.ResponseWriter, r *http.Request) (*internalauth.Claims, bool) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return nil, false
	}
	if claims.TeamID == "" || claims.UserID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "team_id and user_id are required")
		return nil, false
	}
	return claims, true
}

func stringPtrOrNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func (s *Server) createSandboxFilesystem(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}

	var req createSandboxFilesystemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	if snapshotID := strings.TrimSpace(req.SnapshotID); snapshotID != "" {
		if mgr, ok := s.filesystemSnapshotManager(); ok {
			fs, err := mgr.CreateSandboxFilesystemFromSnapshot(r.Context(), &snapshot.CreateSandboxFilesystemFromSnapshotRequest{
				SnapshotID: snapshotID,
				TeamID:     claims.TeamID,
				UserID:     claims.UserID,
				TemplateID: stringPtrOrNil(req.Template),
			})
			if err != nil {
				s.writeSandboxFilesystemError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusCreated, fs)
			return
		}
	}

	now := time.Now().UTC()
	fs := &db.SandboxFilesystem{
		ID:              uuid.New().String(),
		TeamID:          claims.TeamID,
		UserID:          claims.UserID,
		TemplateID:      stringPtrOrNil(req.Template),
		BaseImageDigest: strings.TrimSpace(req.BaseImageDigest),
		S0FSHead:        strings.TrimSpace(req.S0FSHead),
		State:           db.SandboxFilesystemStateAvailable,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if snapshotID := strings.TrimSpace(req.SnapshotID); snapshotID != "" {
		snapshot, err := repo.FindSandboxFilesystemSnapshot(r.Context(), snapshotID, claims.TeamID)
		if err != nil {
			s.writeSandboxFilesystemError(w, err)
			return
		}
		fs.SourceFilesystemID = &snapshot.FilesystemID
		fs.BaseImageDigest = snapshot.BaseImageDigest
		fs.S0FSHead = snapshot.S0FSHead
	}
	if fs.BaseImageDigest == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "base_image_digest is required when snapshot_id is omitted")
		return
	}

	if err := repo.CreateSandboxFilesystem(r.Context(), fs); err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusCreated, fs)
}

func (s *Server) listSandboxFilesystems(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	filesystems, err := repo.ListSandboxFilesystemsByTeam(r.Context(), claims.TeamID)
	if err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, filesystems)
}

func (s *Server) getSandboxFilesystem(w http.ResponseWriter, r *http.Request) {
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, fs)
}

func (s *Server) deleteSandboxFilesystem(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	if err := repo.DeleteSandboxFilesystem(r.Context(), fs.ID); err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) forkSandboxFilesystem(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	source, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	var req forkSandboxFilesystemRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	now := time.Now().UTC()
	fs := &db.SandboxFilesystem{
		ID:         uuid.New().String(),
		TeamID:     claims.TeamID,
		UserID:     claims.UserID,
		TemplateID: stringPtrOrNil(req.Template),
		State:      db.SandboxFilesystemStateAvailable,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if fs.TemplateID == nil {
		fs.TemplateID = source.TemplateID
	}
	if mgr, ok := s.filesystemSnapshotManager(); ok {
		fs, err := mgr.ForkSandboxFilesystem(r.Context(), &snapshot.ForkSandboxFilesystemRequest{
			SourceFilesystemID: source.ID,
			TeamID:             claims.TeamID,
			UserID:             claims.UserID,
			TemplateID:         fs.TemplateID,
		})
		if err != nil {
			s.writeSandboxFilesystemError(w, err)
			return
		}
		_ = spec.WriteSuccess(w, http.StatusCreated, fs)
		return
	}
	if err := repo.ForkSandboxFilesystem(r.Context(), source.ID, fs); err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusCreated, fs)
}

func (s *Server) createSandboxFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	var req createSandboxFilesystemSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "name is required")
		return
	}

	if mgr, ok := s.filesystemSnapshotManager(); ok {
		snapshotRecord, err := mgr.CreateSandboxFilesystemSnapshot(r.Context(), &snapshot.CreateSandboxFilesystemSnapshotRequest{
			FilesystemID: fs.ID,
			TeamID:       claims.TeamID,
			UserID:       claims.UserID,
			Name:         req.Name,
			Description:  req.Description,
		})
		if err != nil {
			s.writeSandboxFilesystemError(w, err)
			return
		}
		_ = spec.WriteSuccess(w, http.StatusCreated, snapshotRecord)
		return
	}

	now := time.Now().UTC()
	snapshot := &db.SandboxFilesystemSnapshot{
		ID:              uuid.New().String(),
		FilesystemID:    fs.ID,
		TeamID:          claims.TeamID,
		UserID:          claims.UserID,
		BaseImageDigest: fs.BaseImageDigest,
		S0FSHead:        fs.S0FSHead,
		Name:            req.Name,
		Description:     req.Description,
		SizeBytes:       0,
		CreatedAt:       now,
	}
	if err := repo.CreateSandboxFilesystemSnapshot(r.Context(), snapshot); err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusCreated, snapshot)
}

func (s *Server) listSandboxFilesystemSnapshots(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	snapshots, err := repo.ListSandboxFilesystemSnapshots(r.Context(), fs.ID, claims.TeamID)
	if err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, snapshots)
}

func (s *Server) getSandboxFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, ok := s.loadTeamSandboxFilesystemSnapshot(w, r)
	if !ok {
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, snapshot)
}

func (s *Server) deleteSandboxFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	snapshotID := strings.TrimSpace(r.PathValue("snapshot_id"))
	if snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "snapshot_id is required")
		return
	}
	if err := repo.DeleteSandboxFilesystemSnapshot(r.Context(), fs.ID, snapshotID, claims.TeamID); err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) restoreSandboxFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return
	}
	snapshotID := strings.TrimSpace(r.PathValue("snapshot_id"))
	if snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "snapshot_id is required")
		return
	}
	if mgr, ok := s.filesystemSnapshotManager(); ok {
		restored, err := mgr.RestoreSandboxFilesystemSnapshot(r.Context(), &snapshot.RestoreSandboxFilesystemSnapshotRequest{
			FilesystemID: fs.ID,
			SnapshotID:   snapshotID,
			TeamID:       claims.TeamID,
			UserID:       claims.UserID,
		})
		if err != nil {
			s.writeSandboxFilesystemError(w, err)
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, restored)
		return
	}
	restored, err := repo.RestoreSandboxFilesystemSnapshot(r.Context(), fs.ID, snapshotID, claims.TeamID)
	if err != nil {
		s.writeSandboxFilesystemError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, restored)
}

func (s *Server) loadTeamSandboxFilesystem(w http.ResponseWriter, r *http.Request) (*db.SandboxFilesystem, bool) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return nil, false
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return nil, false
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "filesystem id is required")
		return nil, false
	}
	fs, err := repo.GetSandboxFilesystem(r.Context(), id)
	if err != nil {
		s.writeSandboxFilesystemError(w, err)
		return nil, false
	}
	if fs.TeamID != claims.TeamID {
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox filesystem not found")
		return nil, false
	}
	return fs, true
}

func (s *Server) loadTeamSandboxFilesystemSnapshot(w http.ResponseWriter, r *http.Request) (*db.SandboxFilesystemSnapshot, bool) {
	repo, ok := s.filesystemRepo(w)
	if !ok {
		return nil, false
	}
	claims, ok := filesystemClaims(w, r)
	if !ok {
		return nil, false
	}
	fs, ok := s.loadTeamSandboxFilesystem(w, r)
	if !ok {
		return nil, false
	}
	snapshotID := strings.TrimSpace(r.PathValue("snapshot_id"))
	if snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "snapshot_id is required")
		return nil, false
	}
	snapshot, err := repo.GetSandboxFilesystemSnapshot(r.Context(), fs.ID, snapshotID, claims.TeamID)
	if err != nil {
		s.writeSandboxFilesystemError(w, err)
		return nil, false
	}
	return snapshot, true
}

func (s *Server) writeSandboxFilesystemError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, db.ErrNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox filesystem not found")
	case errors.Is(err, snapshot.ErrFilesystemNotFound), errors.Is(err, snapshot.ErrFilesystemSnapshotNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, err.Error())
	case errors.Is(err, db.ErrConflict):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, snapshot.ErrFilesystemBusy):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
	default:
		if s != nil && s.logger != nil {
			s.logger.WithError(err).Error("Sandbox filesystem request failed")
		}
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
	}
}
