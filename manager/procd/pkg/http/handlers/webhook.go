package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
)

// WebhookHandler handles webhook publish requests.
type WebhookHandler struct {
	dispatcher *webhook.Dispatcher
}

// NewWebhookHandler creates a new webhook handler.
func NewWebhookHandler(dispatcher *webhook.Dispatcher) *WebhookHandler {
	return &WebhookHandler{
		dispatcher: dispatcher,
	}
}

// PublishRequest is the request body for publishing a webhook event.
type PublishRequest struct {
	EventID string          `json:"event_id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// PublishResponse is the response for a publish request.
type PublishResponse struct {
	EventID string `json:"event_id"`
}

// Publish publishes an event to the configured webhook target.
func (h *WebhookHandler) Publish(w http.ResponseWriter, r *http.Request) {
	if h.dispatcher == nil {
		writeError(w, http.StatusServiceUnavailable, "webhook_unavailable", "webhook dispatcher not configured")
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	payload := any(map[string]any{})
	if len(req.Payload) > 0 {
		payload = req.Payload
	}

	event := webhook.Event{
		EventID:   req.EventID,
		EventType: webhook.EventTypeAgentEvent,
		Payload:   payload,
	}
	h.dispatcher.Enqueue(event)

	writeJSON(w, http.StatusAccepted, PublishResponse{EventID: event.EventID})
}
