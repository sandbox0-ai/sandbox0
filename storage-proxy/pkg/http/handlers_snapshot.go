package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
)

// CreateSnapshotRequest is the request body for creating a snapshot
type createSnapshotRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// SnapshotResponse is the response for snapshot operations
type snapshotResponse struct {
	ID          string  `json:"id"`
	VolumeID    string  `json:"volume_id"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	SizeBytes   int64   `json:"size_bytes"`
	CreatedAt   string  `json:"created_at"`
	ExpiresAt   *string `json:"expires_at,omitempty"`
}

// createSnapshot handles POST /sandboxvolumes/{volume_id}/snapshots
func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	volumeID := r.PathValue("volume_id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume_id is required")
		return
	}

	var req createSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	if req.Name == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "name is required")
		return
	}

	snap, err := s.snapshotMgr.CreateSnapshotSimple(r.Context(), &snapshot.CreateSnapshotRequest{
		VolumeID:    volumeID,
		Name:        req.Name,
		Description: req.Description,
		TeamID:      claims.TeamID,
		UserID:      claims.UserID,
	})

	if err != nil {
		s.handleSnapshotError(w, err)
		return
	}

	resp := snapshotResponse{
		ID:          snap.ID,
		VolumeID:    snap.VolumeID,
		Name:        snap.Name,
		Description: snap.Description,
		SizeBytes:   snap.SizeBytes,
		CreatedAt:   snap.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
	if snap.ExpiresAt != nil {
		expires := snap.ExpiresAt.Format("2006-01-02T15:04:05Z")
		resp.ExpiresAt = &expires
	}

	_ = spec.WriteSuccess(w, http.StatusCreated, resp)
}

// listSnapshots handles GET /sandboxvolumes/{volume_id}/snapshots
func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	volumeID := r.PathValue("volume_id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume_id is required")
		return
	}

	snapshots, err := s.snapshotMgr.ListSnapshots(r.Context(), volumeID, claims.TeamID)
	if err != nil {
		s.handleSnapshotError(w, err)
		return
	}

	var responses []snapshotResponse
	for _, snap := range snapshots {
		resp := snapshotResponse{
			ID:          snap.ID,
			VolumeID:    snap.VolumeID,
			Name:        snap.Name,
			Description: snap.Description,
			SizeBytes:   snap.SizeBytes,
			CreatedAt:   snap.CreatedAt.Format("2006-01-02T15:04:05Z"),
		}
		if snap.ExpiresAt != nil {
			expires := snap.ExpiresAt.Format("2006-01-02T15:04:05Z")
			resp.ExpiresAt = &expires
		}
		responses = append(responses, resp)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = spec.WriteSuccess(w, http.StatusOK, responses)
}

// getSnapshot handles GET /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}
func (s *Server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	volumeID := r.PathValue("volume_id")
	snapshotID := r.PathValue("snapshot_id")
	if volumeID == "" || snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume_id and snapshot_id are required")
		return
	}

	snap, err := s.snapshotMgr.GetSnapshot(r.Context(), volumeID, snapshotID, claims.TeamID)
	if err != nil {
		s.handleSnapshotError(w, err)
		return
	}

	resp := snapshotResponse{
		ID:          snap.ID,
		VolumeID:    snap.VolumeID,
		Name:        snap.Name,
		Description: snap.Description,
		SizeBytes:   snap.SizeBytes,
		CreatedAt:   snap.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
	if snap.ExpiresAt != nil {
		expires := snap.ExpiresAt.Format("2006-01-02T15:04:05Z")
		resp.ExpiresAt = &expires
	}

	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

// restoreSnapshot handles POST /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}/restore
func (s *Server) restoreSnapshot(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	volumeID := r.PathValue("volume_id")
	snapshotID := r.PathValue("snapshot_id")
	if volumeID == "" || snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume_id and snapshot_id are required")
		return
	}

	err := s.snapshotMgr.RestoreSnapshot(r.Context(), &snapshot.RestoreSnapshotRequest{
		VolumeID:   volumeID,
		SnapshotID: snapshotID,
		TeamID:     claims.TeamID,
		UserID:     claims.UserID,
	})

	if err != nil {
		s.handleSnapshotError(w, err)
		return
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]string{"status": "restored"})
}

// deleteSnapshot handles DELETE /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}
func (s *Server) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil {
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
		return
	}

	volumeID := r.PathValue("volume_id")
	snapshotID := r.PathValue("snapshot_id")
	if volumeID == "" || snapshotID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume_id and snapshot_id are required")
		return
	}

	err := s.snapshotMgr.DeleteSnapshot(r.Context(), volumeID, snapshotID, claims.TeamID)
	if err != nil {
		if errors.Is(err, snapshot.ErrSnapshotNotFound) ||
			errors.Is(err, snapshot.ErrSnapshotNotBelongToVolume) ||
			errors.Is(err, snapshot.ErrVolumeNotFound) {
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
			return
		}
		s.handleSnapshotError(w, err)
		return
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
}

// handleSnapshotError maps snapshot errors to HTTP responses
func (s *Server) handleSnapshotError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, snapshot.ErrVolumeNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "volume not found")
	case errors.Is(err, snapshot.ErrSnapshotNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "snapshot not found")
	case errors.Is(err, snapshot.ErrSnapshotNotBelongToVolume):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "snapshot not found") // Don't reveal existence
	case errors.Is(err, snapshot.ErrVolumeLocked):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "volume is locked for another operation")
	case errors.Is(err, snapshot.ErrFlushFailed):
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to flush data")
	case errors.Is(err, snapshot.ErrCloneFailed):
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "clone operation failed")
	case errors.Is(err, snapshot.ErrRemountTimeout):
		_ = spec.WriteError(w, http.StatusGatewayTimeout, spec.CodeInternal, "remount timeout")
	default:
		s.logger.WithError(err).Error("Snapshot operation failed")
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
	}
}
