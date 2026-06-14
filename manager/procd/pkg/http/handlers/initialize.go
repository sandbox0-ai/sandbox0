package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// InitializeHandler handles sandbox initialization requests.
type InitializeHandler struct {
	dispatcher     *webhook.Dispatcher
	fileManager    *file.Manager
	contextManager *ctxpkg.Manager
	httpPort       int
	logger         *zap.Logger
	readyOnce      sync.Once
	watchMu        sync.Mutex
	watchPath      string
	unsubscribe    func() error
}

// NewInitializeHandler creates a new initialize handler.
func NewInitializeHandler(dispatcher *webhook.Dispatcher, fileManager *file.Manager, contextManager *ctxpkg.Manager, httpPort int, logger *zap.Logger) *InitializeHandler {
	return &InitializeHandler{
		dispatcher:     dispatcher,
		fileManager:    fileManager,
		contextManager: contextManager,
		httpPort:       httpPort,
		logger:         logger,
	}
}

// InitializeRequest is the request body for initializing sandbox settings.
type InitializeRequest struct {
	SandboxID string             `json:"sandbox_id"`
	TeamID    string             `json:"team_id,omitempty"`
	EnvVars   map[string]string  `json:"env_vars,omitempty"`
	Webhook   *InitializeWebhook `json:"webhook,omitempty"`
}

// InitializeWebhook represents webhook configuration.
type InitializeWebhook struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// InitializeResponse is returned after initialization.
type InitializeResponse struct {
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id,omitempty"`
}

// UpdateSandboxEnvVarsRequest updates sandbox-level default environment variables.
type UpdateSandboxEnvVarsRequest struct {
	EnvVars map[string]string `json:"env_vars,omitempty"`
}

// UpdateSandboxEnvVarsResponse returns the normalized sandbox environment.
type UpdateSandboxEnvVarsResponse struct {
	EnvVars map[string]string `json:"env_vars"`
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

	if h.contextManager != nil {
		h.contextManager.SetSandboxEnvVars(req.EnvVars)
	}

	h.dispatcher.SetConfig(webhookURL, webhookSecret)
	h.dispatcher.SetIdentity(req.SandboxID, teamID)

	h.configureWebhookWatch(strings.TrimSpace(webhookURL), strings.TrimSpace(webhookWatchDir))

	if webhookURL != "" {
		h.readyOnce.Do(func() {
			if _, err := h.dispatcher.Enqueue(webhook.Event{
				EventType: webhook.EventTypeSandboxReady,
				Payload: map[string]any{
					"http_port":  h.httpPort,
					"sandbox_id": req.SandboxID,
				},
			}); err != nil && h.logger != nil {
				h.logger.Warn("Failed to enqueue sandbox ready webhook", zap.Error(err))
			}
		})
	}

	writeJSON(w, http.StatusOK, InitializeResponse{
		SandboxID: req.SandboxID,
		TeamID:    teamID,
	})
}

// UpdateSandboxEnvVars updates default environment variables for future sandbox processes.
func (h *InitializeHandler) UpdateSandboxEnvVars(w http.ResponseWriter, r *http.Request) {
	var req UpdateSandboxEnvVarsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	envVars := map[string]string{}
	if h.contextManager != nil {
		h.contextManager.SetSandboxEnvVars(req.EnvVars)
		if current := h.contextManager.SandboxEnvVars(); current != nil {
			envVars = current
		}
	}
	writeJSON(w, http.StatusOK, UpdateSandboxEnvVarsResponse{EnvVars: envVars})
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
		if _, err := h.dispatcher.Enqueue(webhook.Event{
			EventType: webhook.EventTypeFileModified,
			Payload:   payload,
		}); err != nil && h.logger != nil {
			h.logger.Warn("Failed to enqueue file webhook", zap.Error(err))
		}
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
