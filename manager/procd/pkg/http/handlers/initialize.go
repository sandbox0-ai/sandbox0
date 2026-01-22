package handlers

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// InitializeHandler handles sandbox initialization requests.
type InitializeHandler struct {
	dispatcher *webhook.Dispatcher
	httpPort   int
	logger     *zap.Logger
	readyOnce  sync.Once
}

// NewInitializeHandler creates a new initialize handler.
func NewInitializeHandler(dispatcher *webhook.Dispatcher, httpPort int, logger *zap.Logger) *InitializeHandler {
	return &InitializeHandler{
		dispatcher: dispatcher,
		httpPort:   httpPort,
		logger:     logger,
	}
}

// InitializeRequest is the request body for initializing sandbox settings.
type InitializeRequest struct {
	SandboxID string             `json:"sandbox_id"`
	TeamID    string             `json:"team_id,omitempty"`
	Webhook   *InitializeWebhook `json:"webhook,omitempty"`
}

// InitializeWebhook represents webhook configuration.
type InitializeWebhook struct {
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}

// InitializeResponse is returned after initialization.
type InitializeResponse struct {
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id,omitempty"`
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
	if req.Webhook != nil {
		webhookURL = req.Webhook.URL
		webhookSecret = req.Webhook.Secret
	}

	h.dispatcher.SetConfig(webhookURL, webhookSecret)
	h.dispatcher.SetIdentity(req.SandboxID, teamID)

	if webhookURL != "" {
		h.readyOnce.Do(func() {
			h.dispatcher.Enqueue(webhook.Event{
				EventType: "sandbox.ready",
				Payload: map[string]any{
					"http_port":  h.httpPort,
					"sandbox_id": req.SandboxID,
				},
			})
		})
	}

	writeJSON(w, http.StatusOK, InitializeResponse{
		SandboxID: req.SandboxID,
		TeamID:    teamID,
	})
}
