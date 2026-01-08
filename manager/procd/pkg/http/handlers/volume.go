package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/volume"
	"go.uber.org/zap"
)

// VolumeHandler handles SandboxVolume-related HTTP requests.
type VolumeHandler struct {
	manager *volume.Manager
	logger  *zap.Logger
}

// NewVolumeHandler creates a new volume handler.
func NewVolumeHandler(manager *volume.Manager, logger *zap.Logger) *VolumeHandler {
	return &VolumeHandler{
		manager: manager,
		logger:  logger,
	}
}

// Mount mounts a SandboxVolume.
func (h *VolumeHandler) Mount(w http.ResponseWriter, r *http.Request) {
	var req volume.MountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.SandboxVolumeID == "" {
		writeError(w, http.StatusBadRequest, "invalid_sandboxvolume_id", "sandboxvolume_id is required")
		return
	}

	if req.MountPoint == "" {
		writeError(w, http.StatusBadRequest, "invalid_mount_point", "mount_point is required")
		return
	}

	resp, err := h.manager.Mount(r.Context(), &req)
	if err != nil {
		if err == volume.ErrVolumeAlreadyMounted {
			writeError(w, http.StatusConflict, "already_mounted", err.Error())
			return
		}
		if err == volume.ErrInvalidMountPoint {
			writeError(w, http.StatusBadRequest, "invalid_mount_point", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "mount_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// Unmount unmounts a SandboxVolume.
func (h *VolumeHandler) Unmount(w http.ResponseWriter, r *http.Request) {
	var req volume.UnmountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.SandboxVolumeID == "" {
		writeError(w, http.StatusBadRequest, "invalid_sandboxvolume_id", "sandboxvolume_id is required")
		return
	}

	err := h.manager.Unmount(r.Context(), req.SandboxVolumeID)
	if err != nil {
		if err == volume.ErrVolumeNotMounted {
			writeError(w, http.StatusNotFound, "not_mounted", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "unmount_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"unmounted": true})
}

// Status returns the status of all mounts.
func (h *VolumeHandler) Status(w http.ResponseWriter, r *http.Request) {
	status := h.manager.GetStatus()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"mounts": status,
	})
}
