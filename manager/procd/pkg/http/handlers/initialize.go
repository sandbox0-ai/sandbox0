package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

const defaultMountWaitTimeout = 30 * time.Second

// InitializeHandler handles sandbox initialization requests.
type InitializeHandler struct {
	dispatcher    *webhook.Dispatcher
	fileManager   *file.Manager
	volumeManager *volume.Manager
	httpPort      int
	logger        *zap.Logger
	readyOnce     sync.Once
	watchMu       sync.Mutex
	watchPath     string
	unsubscribe   func() error
}

// NewInitializeHandler creates a new initialize handler.
func NewInitializeHandler(dispatcher *webhook.Dispatcher, fileManager *file.Manager, volumeManager *volume.Manager, httpPort int, logger *zap.Logger) *InitializeHandler {
	return &InitializeHandler{
		dispatcher:    dispatcher,
		fileManager:   fileManager,
		volumeManager: volumeManager,
		httpPort:      httpPort,
		logger:        logger,
	}
}

// InitializeRequest is the request body for initializing sandbox settings.
type InitializeRequest struct {
	SandboxID          string             `json:"sandbox_id"`
	TeamID             string             `json:"team_id,omitempty"`
	Webhook            *InitializeWebhook `json:"webhook,omitempty"`
	Mounts             []InitializeMount  `json:"mounts,omitempty"`
	WaitForMounts      bool               `json:"wait_for_mounts,omitempty"`
	MountWaitTimeoutMs int32              `json:"mount_wait_timeout_ms,omitempty"`
}

type InitializeMount struct {
	SandboxVolumeID string               `json:"sandboxvolume_id"`
	MountPoint      string               `json:"mount_point"`
	VolumeConfig    *volume.VolumeConfig `json:"volume_config,omitempty"`
}

// InitializeWebhook represents webhook configuration.
type InitializeWebhook struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// InitializeResponse is returned after initialization.
type InitializeResponse struct {
	SandboxID       string               `json:"sandbox_id"`
	TeamID          string               `json:"team_id,omitempty"`
	BootstrapMounts []volume.MountStatus `json:"bootstrap_mounts,omitempty"`
}

// Initialize sets sandbox identity and webhook settings.
func (h *InitializeHandler) Initialize(w http.ResponseWriter, r *http.Request) {
	if h.dispatcher == nil {
		writeError(w, http.StatusServiceUnavailable, "webhook_unavailable", "webhook dispatcher not configured")
		return
	}

	var req InitializeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.SandboxID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(r.Context())
	teamID := req.TeamID
	if claims != nil {
		if teamID == "" {
			teamID = claims.TeamID
		} else if claims.TeamID != "" && teamID != claims.TeamID {
			writeError(w, http.StatusForbidden, "forbidden", "team_id does not match token")
			return
		}
	}

	webhookURL := ""
	webhookSecret := ""
	webhookWatchDir := ""
	if req.Webhook != nil {
		webhookURL = req.Webhook.URL
		webhookSecret = req.Webhook.Secret
		webhookWatchDir = req.Webhook.WatchDir
	}

	h.dispatcher.SetConfig(webhookURL, webhookSecret)
	h.dispatcher.SetIdentity(req.SandboxID, teamID)

	h.configureWebhookWatch(strings.TrimSpace(webhookURL), strings.TrimSpace(webhookWatchDir))

	bootstrapMounts, err := h.bootstrapMounts(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_mount_request", err.Error())
		return
	}

	if webhookURL != "" {
		h.readyOnce.Do(func() {
			h.dispatcher.Enqueue(webhook.Event{
				EventType: webhook.EventTypeSandboxReady,
				Payload: map[string]any{
					"http_port":  h.httpPort,
					"sandbox_id": req.SandboxID,
				},
			})
		})
	}

	writeJSON(w, http.StatusOK, InitializeResponse{
		SandboxID:       req.SandboxID,
		TeamID:          teamID,
		BootstrapMounts: bootstrapMounts,
	})
}

func (h *InitializeHandler) bootstrapMounts(ctx context.Context, req InitializeRequest) ([]volume.MountStatus, error) {
	if h.volumeManager == nil || len(req.Mounts) == 0 {
		return nil, nil
	}
	mounts := make([]volume.MountRequest, 0, len(req.Mounts))
	for _, item := range req.Mounts {
		mounts = append(mounts, volume.MountRequest{
			SandboxVolumeID: item.SandboxVolumeID,
			SandboxID:       req.SandboxID,
			MountPoint:      item.MountPoint,
			VolumeConfig:    item.VolumeConfig,
		})
	}
	waitTimeout := time.Duration(req.MountWaitTimeoutMs) * time.Millisecond
	if req.WaitForMounts && waitTimeout <= 0 {
		waitTimeout = defaultMountWaitTimeout
	}
	return h.volumeManager.BootstrapMounts(ctx, mounts, req.WaitForMounts, waitTimeout)
}

func (h *InitializeHandler) configureWebhookWatch(webhookURL, watchDir string) {
	if h.fileManager == nil {
		return
	}

	h.watchMu.Lock()
	defer h.watchMu.Unlock()

	if webhookURL == "" || watchDir == "" {
		if h.unsubscribe != nil {
			_ = h.unsubscribe()
		}
		h.unsubscribe = nil
		h.watchPath = ""
		return
	}

	if watchDir == h.watchPath {
		return
	}

	if h.unsubscribe != nil {
		_ = h.unsubscribe()
		h.unsubscribe = nil
		h.watchPath = ""
	}

	_, unsubscribe, err := h.fileManager.SubscribeWatch(watchDir, true, func(event file.WatchEvent) {
		if event.Type == file.EventInvalidate {
			return
		}
		payload := map[string]any{
			"event_type": event.Type,
			"path":       event.Path,
		}
		if event.OldPath != "" {
			payload["old_path"] = event.OldPath
		}
		h.dispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeFileModified,
			Payload:   payload,
		})
	})
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("Failed to watch webhook directory",
				zap.String("watch_dir", watchDir),
				zap.Error(err),
			)
		}
		return
	}

	h.unsubscribe = unsubscribe
	h.watchPath = watchDir
}
