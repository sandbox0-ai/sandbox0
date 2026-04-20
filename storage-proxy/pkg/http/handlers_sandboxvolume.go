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
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type createSandboxVolumeRequest struct {
	AccessMode      string `json:"access_mode"`
	DefaultPosixUID *int64 `json:"default_posix_uid,omitempty"`
	DefaultPosixGID *int64 `json:"default_posix_gid,omitempty"`
}

type createOwnedSandboxVolumeRequest struct {
	SandboxID       string `json:"sandbox_id"`
	ClusterID       string `json:"cluster_id"`
	Purpose         string `json:"purpose"`
	UserID          string `json:"user_id,omitempty"`
	AccessMode      string `json:"access_mode"`
	DefaultPosixUID *int64 `json:"default_posix_uid,omitempty"`
	DefaultPosixGID *int64 `json:"default_posix_gid,omitempty"`
}

type markOwnedSandboxVolumesForCleanupRequest struct {
	SandboxID string `json:"sandbox_id"`
	ClusterID string `json:"cluster_id"`
	Reason    string `json:"reason,omitempty"`
}

type markOwnedSandboxVolumeCleanupAttemptRequest struct {
	Error string `json:"error,omitempty"`
}

var errVolumeHasActiveMounts = errors.New("volume has active mounts")

type forkSandboxVolumeRequest struct {
	AccessMode      *string `json:"access_mode"`
	DefaultPosixUID *int64  `json:"default_posix_uid,omitempty"`
	DefaultPosixGID *int64  `json:"default_posix_gid,omitempty"`
}

func defaultZeroPosixIdentity(uid, gid **int64) {
	if uid == nil || gid == nil {
		return
	}
	if *uid != nil || *gid != nil {
		return
	}
	defaultPosixID := int64(0)
	*uid = &defaultPosixID
	*gid = &defaultPosixID
}

func validateDefaultPosixIdentity(uid, gid *int64) error {
	if (uid == nil) != (gid == nil) {
		return errors.New("default_posix_uid and default_posix_gid must be set together")
	}
	if uid == nil {
		return nil
	}
	if *uid < 0 || *gid < 0 {
		return errors.New("default_posix_uid and default_posix_gid must be non-negative")
	}
	return nil
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

	defaultZeroPosixIdentity(&req.DefaultPosixUID, &req.DefaultPosixGID)

	accessMode, ok := volume.ParseAccessMode(req.AccessMode)
	if req.AccessMode == "" {
		accessMode = volume.AccessModeRWO
		ok = true
	}
	if !ok {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
		return
	}
	if err := validateDefaultPosixIdentity(req.DefaultPosixUID, req.DefaultPosixGID); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	vol := &db.SandboxVolume{
		ID:              uuid.New().String(),
		TeamID:          teamId,
		UserID:          userId,
		DefaultPosixUID: req.DefaultPosixUID,
		DefaultPosixGID: req.DefaultPosixGID,
		AccessMode:      string(accessMode),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	if err := s.repo.WithTx(r.Context(), func(tx pgx.Tx) error {
		if err := s.repo.CreateSandboxVolumeTx(r.Context(), tx, vol); err != nil {
			return err
		}
		if err := s.appendMeteringEventTx(r.Context(), tx, volumeCreatedEvent(s.regionID, vol)); err != nil {
			return err
		}
		return nil
	}); err != nil {
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

	if s.isOwnedSandboxVolume(r.Context(), id) {
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
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

	if s.isOwnedSandboxVolume(r.Context(), id) {
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}

	// Check for active mounts via repository
	// Using 15 seconds as heartbeat timeout (same as coordinator.HeartbeatTimeout)
	const heartbeatTimeout = 15
	force := r.URL.Query().Get("force") == "true"
	if !force && s.volMgr != nil {
		if _, err := s.volMgr.CleanupIdleDirectVolumeFileMount(r.Context(), id); err != nil {
			s.logger.WithError(err).WithField("volume_id", id).Warn("Failed to cleanup idle direct volume mount before delete")
		}
	}
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
	if err := s.repo.WithTx(r.Context(), func(tx pgx.Tx) error {
		if err := s.repo.DeleteSandboxVolumeTx(r.Context(), tx, id); err != nil {
			return err
		}
		if err := s.appendMeteringEventTx(r.Context(), tx, volumeDeletedEvent(s.regionID, vol)); err != nil {
			return err
		}
		return nil
	}); err != nil {
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

func (s *Server) deleteSandboxVolumeRecord(ctx context.Context, id string, force bool) (*db.SandboxVolume, error) {
	vol, err := s.repo.GetSandboxVolume(ctx, id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, db.ErrNotFound
		}
		return nil, fmt.Errorf("get sandbox volume: %w", err)
	}

	const heartbeatTimeout = 15
	if !force && s.volMgr != nil {
		if _, err := s.volMgr.CleanupIdleDirectVolumeFileMount(ctx, id); err != nil {
			s.logger.WithError(err).WithField("volume_id", id).Warn("Failed to cleanup idle direct volume mount before delete")
		}
	}
	mounts, err := s.repo.GetActiveMounts(ctx, id, heartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("check active mounts: %w", err)
	}
	if len(mounts) > 0 && !force {
		return nil, errVolumeHasActiveMounts
	}
	if len(mounts) > 0 && force {
		for _, mount := range mounts {
			if err := s.repo.DeleteMount(ctx, id, mount.ClusterID, mount.PodID); err != nil {
				return nil, fmt.Errorf("cleanup mount records: %w", err)
			}
		}
	}
	if err := s.repo.WithTx(ctx, func(tx pgx.Tx) error {
		if err := s.repo.DeleteSandboxVolumeTx(ctx, tx, id); err != nil {
			return err
		}
		if err := s.appendMeteringEventTx(ctx, tx, volumeDeletedEvent(s.regionID, vol)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return vol, nil
}

func (s *Server) isOwnedSandboxVolume(ctx context.Context, id string) bool {
	if s == nil || s.repo == nil {
		return false
	}
	_, err := s.repo.GetSandboxVolumeOwner(ctx, id)
	return err == nil
}

func (s *Server) requireManagerInternal(w http.ResponseWriter, r *http.Request) (*internalauth.Claims, bool) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return nil, false
	}
	if claims.Caller != internalauth.ServiceManager {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "manager token is required")
		return nil, false
	}
	return claims, true
}

func (s *Server) createOwnedSandboxVolume(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}
	if claims.TeamID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}

	var req createOwnedSandboxVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.ClusterID = strings.TrimSpace(req.ClusterID)
	req.Purpose = strings.TrimSpace(req.Purpose)
	if req.SandboxID == "" || req.ClusterID == "" || req.Purpose == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id, cluster_id and purpose are required")
		return
	}
	if claims.SandboxID != "" && claims.SandboxID != req.SandboxID {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "sandbox_id does not match token")
		return
	}

	if existing, err := s.repo.GetOwnedSandboxVolumeByOwner(r.Context(), req.ClusterID, req.SandboxID, req.Purpose); err == nil {
		_ = spec.WriteSuccess(w, http.StatusOK, existing)
		return
	} else if !errors.Is(err, db.ErrNotFound) {
		s.logger.WithError(err).Error("Failed to check existing owned sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	defaultZeroPosixIdentity(&req.DefaultPosixUID, &req.DefaultPosixGID)
	accessMode, ok := volume.ParseAccessMode(req.AccessMode)
	if req.AccessMode == "" {
		accessMode = volume.AccessModeRWO
		ok = true
	}
	if !ok {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid access_mode")
		return
	}
	if err := validateDefaultPosixIdentity(req.DefaultPosixUID, req.DefaultPosixGID); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	userID := strings.TrimSpace(claims.UserID)
	if userID == "" {
		userID = strings.TrimSpace(req.UserID)
	}
	if userID == "" {
		userID = "system"
	}

	now := time.Now().UTC()
	vol := &db.SandboxVolume{
		ID:              uuid.New().String(),
		TeamID:          claims.TeamID,
		UserID:          userID,
		DefaultPosixUID: req.DefaultPosixUID,
		DefaultPosixGID: req.DefaultPosixGID,
		AccessMode:      string(accessMode),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	owner := &db.SandboxVolumeOwner{
		VolumeID:       vol.ID,
		OwnerKind:      db.SandboxVolumeOwnerKindSandbox,
		OwnerSandboxID: req.SandboxID,
		OwnerClusterID: req.ClusterID,
		Purpose:        req.Purpose,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.repo.WithTx(r.Context(), func(tx pgx.Tx) error {
		if err := s.repo.CreateSandboxVolumeTx(r.Context(), tx, vol); err != nil {
			return err
		}
		if err := s.repo.CreateSandboxVolumeOwnerTx(r.Context(), tx, owner); err != nil {
			return err
		}
		if err := s.appendMeteringEventTx(r.Context(), tx, volumeCreatedEvent(s.regionID, vol)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		if existing, getErr := s.repo.GetOwnedSandboxVolumeByOwner(r.Context(), req.ClusterID, req.SandboxID, req.Purpose); getErr == nil {
			_ = spec.WriteSuccess(w, http.StatusOK, existing)
			return
		}
		s.logger.WithError(err).Error("Failed to create owned sandbox volume")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, &db.OwnedSandboxVolume{
		Volume: *vol,
		Owner:  *owner,
	})
}

func (s *Server) listOwnedSandboxVolumes(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}
	if !claims.IsSystemToken() {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return
	}
	clusterID := strings.TrimSpace(r.URL.Query().Get("cluster_id"))
	if clusterID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "cluster_id is required")
		return
	}
	var cleanupRequested *bool
	switch strings.TrimSpace(r.URL.Query().Get("cleanup_requested")) {
	case "true":
		value := true
		cleanupRequested = &value
	case "false":
		value := false
		cleanupRequested = &value
	}
	owned, err := s.repo.ListOwnedSandboxVolumes(r.Context(), clusterID, cleanupRequested)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list owned sandbox volumes")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, owned)
}

func (s *Server) markOwnedSandboxVolumesForCleanup(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}
	var req markOwnedSandboxVolumesForCleanupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.ClusterID = strings.TrimSpace(req.ClusterID)
	if req.SandboxID == "" || req.ClusterID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and cluster_id are required")
		return
	}
	if !claims.IsSystemToken() && claims.SandboxID != "" && claims.SandboxID != req.SandboxID {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "sandbox_id does not match token")
		return
	}
	marked, err := s.repo.MarkOwnedSandboxVolumesForCleanup(r.Context(), req.ClusterID, req.SandboxID, strings.TrimSpace(req.Reason))
	if err != nil {
		s.logger.WithError(err).Error("Failed to mark owned sandbox volumes for cleanup")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"marked": marked})
}

func (s *Server) markOwnedSandboxVolumeCleanupAttempt(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}
	if !claims.IsSystemToken() {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}
	var req markOwnedSandboxVolumeCleanupAttemptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	var cleanupErr error
	if msg := strings.TrimSpace(req.Error); msg != "" {
		cleanupErr = errors.New(msg)
	}
	if err := s.repo.MarkOwnedSandboxVolumeCleanupAttempt(r.Context(), id, cleanupErr); err != nil && !errors.Is(err, db.ErrNotFound) {
		s.logger.WithError(err).Error("Failed to mark owned sandbox volume cleanup attempt")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"updated": true})
}

func (s *Server) deleteOwnedSandboxVolume(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}
	owner, err := s.repo.GetSandboxVolumeOwner(r.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
			return
		}
		s.logger.WithError(err).Error("Failed to get sandbox volume owner")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	if !claims.IsSystemToken() && claims.SandboxID != "" && claims.SandboxID != owner.OwnerSandboxID {
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, "sandbox_id does not match token")
		return
	}
	if _, err := s.deleteSandboxVolumeRecord(r.Context(), id, false); err != nil {
		_ = s.repo.MarkOwnedSandboxVolumeCleanupAttempt(r.Context(), id, err)
		status, code := http.StatusInternalServerError, spec.CodeInternal
		if errors.Is(err, errVolumeHasActiveMounts) {
			status, code = http.StatusConflict, spec.CodeConflict
		}
		_ = spec.WriteError(w, status, code, err.Error())
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) prepareSandboxVolumeForPortalBind(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireManagerInternal(w, r)
	if !ok {
		return
	}

	id := strings.TrimSpace(r.PathValue("id"))
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
		s.logger.WithError(err).WithField("volume_id", id).Error("Failed to get sandbox volume before portal bind preparation")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	if !claims.IsSystemToken() && claims.TeamID != "" && vol.TeamID != claims.TeamID {
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}

	cleaned := false
	if s.volMgr != nil {
		cleaned, err = s.volMgr.CleanupIdleDirectVolumeFileMount(r.Context(), id)
		if err != nil {
			s.logger.WithError(err).WithField("volume_id", id).Warn("Failed to cleanup direct volume mount before portal bind")
			_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to cleanup direct volume mount")
			return
		}
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{
		"prepared": true,
		"cleaned":  cleaned,
	})
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
	if err := validateDefaultPosixIdentity(req.DefaultPosixUID, req.DefaultPosixGID); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	vol, err := s.snapshotMgr.ForkVolume(r.Context(), &snapshot.ForkVolumeRequest{
		SourceVolumeID:  id,
		TeamID:          claims.TeamID,
		UserID:          claims.UserID,
		AccessMode:      req.AccessMode,
		DefaultPosixUID: req.DefaultPosixUID,
		DefaultPosixGID: req.DefaultPosixGID,
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

func volumeCreatedEvent(regionID string, vol *db.SandboxVolume) *meteringpkg.Event {
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("volume/%s/created/%d", vol.ID, vol.CreatedAt.UTC().UnixNano()),
		Producer:    "storage-proxy.volume",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeVolumeCreated,
		SubjectType: meteringpkg.SubjectTypeVolume,
		SubjectID:   vol.ID,
		TeamID:      vol.TeamID,
		UserID:      vol.UserID,
		VolumeID:    vol.ID,
		OccurredAt:  vol.CreatedAt,
	}
}

func volumeDeletedEvent(regionID string, vol *db.SandboxVolume) *meteringpkg.Event {
	now := time.Now().UTC()
	return &meteringpkg.Event{
		EventID:     fmt.Sprintf("volume/%s/deleted/%d", vol.ID, now.UnixNano()),
		Producer:    "storage-proxy.volume",
		RegionID:    regionID,
		EventType:   meteringpkg.EventTypeVolumeDeleted,
		SubjectType: meteringpkg.SubjectTypeVolume,
		SubjectID:   vol.ID,
		TeamID:      vol.TeamID,
		UserID:      vol.UserID,
		VolumeID:    vol.ID,
		OccurredAt:  now,
	}
}
