package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type createSandboxVolumeRequest struct {
	CacheSize  string `json:"cache_size"`
	Prefetch   int    `json:"prefetch"`
	BufferSize string `json:"buffer_size"`
	Writeback  bool   `json:"writeback"`
	AccessMode string `json:"access_mode"`
}

type forkSandboxVolumeRequest struct {
	CacheSize  *string `json:"cache_size"`
	Prefetch   *int    `json:"prefetch"`
	BufferSize *string `json:"buffer_size"`
	Writeback  *bool   `json:"writeback"`
	AccessMode *string `json:"access_mode"`
}

func (s *Server) createSandboxVolume(w http.ResponseWriter, r *http.Request) {
	// Get claims from context (populated by middleware)
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	var req createSandboxVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	teamId, userId := claims.TeamID, claims.UserID
	if teamId == "" || userId == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "team_id and user_id are required")
		return
	}

	// Set defaults if not provided
	if req.CacheSize == "" {
		req.CacheSize = "1G"
	}
	if req.BufferSize == "" {
		req.BufferSize = "32M"
	}

	accessMode, ok := volume.ParseAccessMode(req.AccessMode)
	if req.AccessMode == "" {
		accessMode = volume.AccessModeRWO
		ok = true
	}
	if !ok {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
		return
	}

	vol := &db.SandboxVolume{
		ID:         uuid.New().String(),
		TeamID:     teamId,
		UserID:     userId,
		CacheSize:  req.CacheSize,
		Prefetch:   req.Prefetch,
		BufferSize: req.BufferSize,
		Writeback:  req.Writeback,
		AccessMode: string(accessMode),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := s.repo.CreateSandboxVolume(r.Context(), vol); err != nil {
		s.logger.WithError(err).Error("Failed to create sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, vol)
}

func (s *Server) listSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	// Get claims from context
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	// Use team_id from trusted token
	teamID := claims.TeamID
	if teamID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required in token")
		return
	}

	volumes, err := s.repo.ListSandboxVolumesByTeam(r.Context(), teamID)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list sandbox volumes")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	_ = spec.WriteSuccess(w, http.StatusOK, volumes)
}

func (s *Server) getSandboxVolume(w http.ResponseWriter, r *http.Request) {
	// Get claims from context
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	id := r.PathValue("id")
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	vol, err := s.repo.GetSandboxVolume(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		s.logger.WithError(err).Error("Failed to get sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	// Check if the volume belongs to the requesting team
	if vol.TeamID != claims.TeamID {
		s.logger.WithField("vol_team", vol.TeamID).WithField("req_team", claims.TeamID).Warn("Unauthorized access attempt to sandbox volume")
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found") // Don't reveal existence
		return
	}

	_ = spec.WriteSuccess(w, http.StatusOK, vol)
}

func (s *Server) deleteSandboxVolume(w http.ResponseWriter, r *http.Request) {
	// Get claims from context
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	id := r.PathValue("id")
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	// Get the volume to verify ownership
	vol, err := s.repo.GetSandboxVolume(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
			return
		}
		s.logger.WithError(err).Error("Failed to get sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	// Check if the volume belongs to the requesting team
	if vol.TeamID != claims.TeamID {
		s.logger.WithField("vol_team", vol.TeamID).WithField("req_team", claims.TeamID).Warn("Unauthorized delete attempt to sandbox volume")
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found") // Don't reveal existence
		return
	}

	// Check for active mounts via repository
	// Using 15 seconds as heartbeat timeout (same as coordinator.HeartbeatTimeout)
	const heartbeatTimeout = 15
	force := r.URL.Query().Get("force") == "true"
	mounts, err := s.repo.GetActiveMounts(r.Context(), id, heartbeatTimeout)
	if err != nil {
		s.logger.WithError(err).Error("Failed to check active mounts")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to check active mounts")
		return
	}

	if len(mounts) > 0 && !force {
		// Volume has active mounts, cannot delete
		s.logger.WithField("volume_id", id).WithField("active_mounts", len(mounts)).Warn("Attempted to delete volume with active mounts")
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "volume has active mounts", map[string]any{
			"active_mounts": len(mounts),
			"mounts":        mounts,
			"hint":          "retry with force=true to remove orphan mount records",
		})
		return
	}

	if len(mounts) > 0 && force {
		for _, mount := range mounts {
			if err := s.repo.DeleteMount(r.Context(), id, mount.ClusterID, mount.PodID); err != nil {
				s.logger.WithError(err).WithFields(map[string]any{
					"volume_id":  id,
					"cluster_id": mount.ClusterID,
					"pod_id":     mount.PodID,
				}).Error("Failed to delete mount during force delete")
				_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to cleanup mount records")
				return
			}
		}
	}

	// No active mounts, proceed with deletion
	if err := s.repo.DeleteSandboxVolume(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
			return
		}
		s.logger.WithError(err).Error("Failed to delete sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	s.logger.WithField("volume_id", id).WithField("team_id", vol.TeamID).Info("Sandbox volume deleted")
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) forkVolume(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	id := r.PathValue("id")
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	var req forkSandboxVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	if s.snapshotMgr == nil {
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "snapshot manager is not configured")
		return
	}

	vol, err := s.snapshotMgr.ForkVolume(r.Context(), &snapshot.ForkVolumeRequest{
		SourceVolumeID: id,
		TeamID:         claims.TeamID,
		UserID:         claims.UserID,
		CacheSize:      req.CacheSize,
		Prefetch:       req.Prefetch,
		BufferSize:     req.BufferSize,
		Writeback:      req.Writeback,
		AccessMode:     req.AccessMode,
	})
	if err != nil {
		switch {
		case errors.Is(err, snapshot.ErrVolumeNotFound):
			_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "volume not found")
		case errors.Is(err, snapshot.ErrInvalidAccessMode):
			_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
		case errors.Is(err, snapshot.ErrCloneFailed):
			_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "clone operation failed")
		default:
			s.logger.WithError(err).Error("Failed to fork sandbox volume")
			_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		}
		return
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, vol)
}
