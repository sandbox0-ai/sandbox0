package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const (
	defaultArtifactKind      = "generic"
	defaultArtifactMediaType = "application/octet-stream"
)

type createArtifactRequest struct {
	Name      string               `json:"name,omitempty"`
	Kind      string               `json:"kind,omitempty"`
	MediaType string               `json:"media_type,omitempty"`
	Digest    string               `json:"digest,omitempty"`
	Source    createArtifactSource `json:"source"`
	Metadata  json.RawMessage      `json:"metadata,omitempty"`
}

type createArtifactSource struct {
	Type            string `json:"type"`
	SandboxVolumeID string `json:"sandboxvolume_id,omitempty"`
}

type createArtifactVolumeRequest struct {
	AccessMode      string `json:"access_mode,omitempty"`
	DefaultPosixUID *int64 `json:"default_posix_uid,omitempty"`
	DefaultPosixGID *int64 `json:"default_posix_gid,omitempty"`
}

func (s *Server) createArtifact(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.TeamID == "" || claims.UserID == "" {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.repo == nil || s.snapshotMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "artifact storage is not configured")
		return
	}

	var req createArtifactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.Source.Type = strings.TrimSpace(req.Source.Type)
	req.Source.SandboxVolumeID = strings.TrimSpace(req.Source.SandboxVolumeID)
	if req.Source.Type == "" {
		req.Source.Type = "sandbox_volume"
	}
	if req.Source.Type != "sandbox_volume" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "source.type must be sandbox_volume")
		return
	}
	if req.Source.SandboxVolumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "source.sandboxvolume_id is required")
		return
	}

	sourceVolume, err := s.loadTeamVolume(r.Context(), claims.TeamID, req.Source.SandboxVolumeID)
	if err != nil {
		s.writeArtifactError(w, err)
		return
	}
	artifactID := uuid.New().String()
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = artifactID
	}
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = defaultArtifactKind
	}
	mediaType := strings.TrimSpace(req.MediaType)
	if mediaType == "" {
		mediaType = defaultArtifactMediaType
	}
	metadata := json.RawMessage(`{}`)
	if len(req.Metadata) > 0 {
		if !json.Valid(req.Metadata) {
			_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "metadata must be valid JSON")
			return
		}
		metadata = append(json.RawMessage(nil), req.Metadata...)
	}

	snap, err := s.snapshotMgr.CreateSnapshotSimple(r.Context(), &snapshot.CreateSnapshotRequest{
		VolumeID:        sourceVolume.ID,
		Name:            "artifact-" + artifactID,
		Description:     "Artifact source snapshot",
		TeamID:          claims.TeamID,
		UserID:          claims.UserID,
		StorageMetadata: storageObservationMetadataFromHeaders(r.Header),
	})
	if err != nil {
		s.writeArtifactError(w, err)
		return
	}
	now := time.Now().UTC()
	artifact := &db.Artifact{
		ID:             artifactID,
		TeamID:         claims.TeamID,
		UserID:         claims.UserID,
		Name:           name,
		Kind:           kind,
		MediaType:      mediaType,
		Digest:         strings.TrimSpace(req.Digest),
		SourceVolumeID: sourceVolume.ID,
		SnapshotID:     snap.ID,
		SizeBytes:      snap.SizeBytes,
		Metadata:       metadata,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.repo.WithTx(r.Context(), func(tx pgx.Tx) error {
		return s.repo.CreateArtifactTx(r.Context(), tx, artifact)
	}); err != nil {
		s.logger.WithError(err).Error("Failed to create artifact")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, artifact)
}

func (s *Server) listArtifacts(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.TeamID == "" {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	if s.repo == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "artifact storage is not configured")
		return
	}
	artifacts, err := s.repo.ListArtifactsByTeam(r.Context(), claims.TeamID)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list artifacts")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) getArtifact(w http.ResponseWriter, r *http.Request) {
	artifact, ok := s.loadAuthorizedArtifactResponse(w, r)
	if !ok {
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, artifact)
}

func (s *Server) deleteArtifact(w http.ResponseWriter, r *http.Request) {
	artifact, ok := s.loadAuthorizedArtifactResponse(w, r)
	if !ok {
		return
	}
	if err := s.repo.WithTx(r.Context(), func(tx pgx.Tx) error {
		return s.repo.DeleteArtifactTx(r.Context(), tx, artifact.ID)
	}); err != nil {
		s.writeArtifactError(w, err)
		return
	}
	if s.snapshotMgr != nil {
		if err := s.snapshotMgr.DeleteSnapshot(r.Context(), artifact.SourceVolumeID, artifact.SnapshotID, artifact.TeamID); err != nil {
			s.logger.WithError(err).WithField("artifact_id", artifact.ID).Warn("Failed to delete artifact snapshot")
		}
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) createVolumeFromArtifact(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.TeamID == "" || claims.UserID == "" {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}
	artifact, ok := s.loadAuthorizedArtifactResponse(w, r)
	if !ok {
		return
	}
	if s.snapshotMgr == nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "snapshot manager is not configured")
		return
	}
	var req createArtifactVolumeRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if !errors.Is(err, io.EOF) {
				_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
				return
			}
		}
	}
	defaultZeroPosixIdentity(&req.DefaultPosixUID, &req.DefaultPosixGID)
	accessMode, okMode := volume.ParseAccessMode(req.AccessMode)
	if strings.TrimSpace(req.AccessMode) == "" {
		accessMode = volume.AccessModeROX
		okMode = true
	}
	if !okMode {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
		return
	}
	if err := validateDefaultPosixIdentity(req.DefaultPosixUID, req.DefaultPosixGID); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	vol, err := s.snapshotMgr.CreateVolumeFromSnapshot(r.Context(), &snapshot.CreateVolumeFromSnapshotRequest{
		SnapshotID:      artifact.SnapshotID,
		TeamID:          claims.TeamID,
		UserID:          claims.UserID,
		AccessMode:      string(accessMode),
		DefaultPosixUID: req.DefaultPosixUID,
		DefaultPosixGID: req.DefaultPosixGID,
		StorageMetadata: storageObservationMetadataFromHeaders(r.Header),
	})
	if err != nil {
		s.writeArtifactError(w, err)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusCreated, vol)
}

func (s *Server) loadAuthorizedArtifactResponse(w http.ResponseWriter, r *http.Request) (*db.Artifact, bool) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.TeamID == "" {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return nil, false
	}
	artifactID := strings.TrimSpace(r.PathValue("id"))
	if artifactID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "artifact id is required")
		return nil, false
	}
	artifact, err := s.loadTeamArtifact(r.Context(), claims.TeamID, artifactID)
	if err != nil {
		s.writeArtifactError(w, err)
		return nil, false
	}
	return artifact, true
}

func (s *Server) loadTeamArtifact(ctx context.Context, teamID, artifactID string) (*db.Artifact, error) {
	if s.repo == nil {
		return nil, errVolumeFileUnavailable
	}
	artifact, err := s.repo.GetArtifact(ctx, artifactID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}
	if artifact.TeamID != teamID {
		return nil, errVolumeNotFound
	}
	return artifact, nil
}

func (s *Server) loadTeamVolume(ctx context.Context, teamID, volumeID string) (*db.SandboxVolume, error) {
	volumeRecord, err := s.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}
	if volumeRecord.TeamID != teamID || s.isOwnedSandboxVolume(ctx, volumeID) {
		return nil, errVolumeNotFound
	}
	return volumeRecord, nil
}

func (s *Server) writeArtifactError(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, errVolumeNotFound), errors.Is(err, snapshot.ErrSnapshotNotFound), errors.Is(err, snapshot.ErrVolumeNotFound), errors.Is(err, db.ErrNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
	case errors.Is(err, snapshot.ErrInvalidAccessMode):
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
	case errors.Is(err, errVolumeFileUnavailable):
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
	default:
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, fmt.Sprintf("internal server error: %v", err))
	}
}
