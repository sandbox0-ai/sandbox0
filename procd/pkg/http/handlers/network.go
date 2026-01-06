package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/sandbox0-ai/infra/procd/pkg/network"
	"go.uber.org/zap"
)

// NetworkHandler handles network-related HTTP requests.
type NetworkHandler struct {
	manager *network.Manager
	logger  *zap.Logger
}

// NewNetworkHandler creates a new network handler.
func NewNetworkHandler(manager *network.Manager, logger *zap.Logger) *NetworkHandler {
	return &NetworkHandler{
		manager: manager,
		logger:  logger,
	}
}

// GetPolicy returns the current network policy.
func (h *NetworkHandler) GetPolicy(w http.ResponseWriter, r *http.Request) {
	policy := h.manager.GetPolicy()
	writeJSON(w, http.StatusOK, policy)
}

// UpdatePolicy updates the network policy.
func (h *NetworkHandler) UpdatePolicy(w http.ResponseWriter, r *http.Request) {
	var policy network.NetworkPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if err := h.manager.UpdatePolicy(&policy); err != nil {
		writeError(w, http.StatusInternalServerError, "update_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, h.manager.GetPolicy())
}

// ResetPolicy resets the network policy to default.
func (h *NetworkHandler) ResetPolicy(w http.ResponseWriter, r *http.Request) {
	if err := h.manager.ResetPolicy(); err != nil {
		writeError(w, http.StatusInternalServerError, "reset_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, h.manager.GetPolicy())
}

// AddAllowCIDR adds a CIDR to the allow list.
func (h *NetworkHandler) AddAllowCIDR(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CIDR string `json:"cidr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.CIDR == "" {
		writeError(w, http.StatusBadRequest, "invalid_cidr", "CIDR is required")
		return
	}

	if err := h.manager.AddAllowCIDR(req.CIDR); err != nil {
		writeError(w, http.StatusInternalServerError, "add_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"added": true})
}

// AddAllowDomain adds a domain to the allow list.
func (h *NetworkHandler) AddAllowDomain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain string `json:"domain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.Domain == "" {
		writeError(w, http.StatusBadRequest, "invalid_domain", "Domain is required")
		return
	}

	if err := h.manager.AddAllowDomain(req.Domain); err != nil {
		writeError(w, http.StatusInternalServerError, "add_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"added": true})
}

// AddDenyCIDR adds a CIDR to the deny list.
func (h *NetworkHandler) AddDenyCIDR(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CIDR string `json:"cidr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.CIDR == "" {
		writeError(w, http.StatusBadRequest, "invalid_cidr", "CIDR is required")
		return
	}

	if err := h.manager.AddDenyCIDR(req.CIDR); err != nil {
		writeError(w, http.StatusInternalServerError, "add_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"added": true})
}
