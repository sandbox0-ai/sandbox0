package handlers

import (
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/execution"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type ExecutionScopeHandler struct {
	resolver  *execution.Resolver
	sandboxID func() string
}

func NewExecutionScopeHandler(resolver *execution.Resolver, sandboxID func() string) *ExecutionScopeHandler {
	return &ExecutionScopeHandler{resolver: resolver, sandboxID: sandboxID}
}

func (h *ExecutionScopeHandler) Resolve(w http.ResponseWriter, r *http.Request) {
	claims := internalauth.ClaimsFromContext(r.Context())
	if claims == nil || claims.Caller != internalauth.ServiceNetd {
		writeError(w, http.StatusForbidden, "forbidden", "execution scope resolution is restricted to netd")
		return
	}
	expectedSandboxID := ""
	if h.sandboxID != nil {
		expectedSandboxID = strings.TrimSpace(h.sandboxID())
	}
	if expectedSandboxID == "" || strings.TrimSpace(claims.TeamID) == "" || claims.SandboxID != expectedSandboxID {
		writeError(w, http.StatusForbidden, "forbidden", "execution scope token does not match this sandbox")
		return
	}
	transport := strings.TrimSpace(r.URL.Query().Get("transport"))
	localIP := net.ParseIP(strings.TrimSpace(r.URL.Query().Get("local_ip")))
	if localIP == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "local_ip is required")
		return
	}
	localPort, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("local_port")))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "local_port is required")
		return
	}
	remotePort := 0
	if value := strings.TrimSpace(r.URL.Query().Get("remote_port")); value != "" {
		remotePort, err = strconv.Atoi(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "remote_port is invalid")
			return
		}
	}
	remoteIP := net.ParseIP(strings.TrimSpace(r.URL.Query().Get("remote_ip")))
	if remoteIP == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "remote_ip is required")
		return
	}
	scope, ok, err := h.resolver.ResolveSocket(execution.SocketQuery{
		Transport:  transport,
		LocalIP:    localIP,
		LocalPort:  localPort,
		RemoteIP:   remoteIP,
		RemotePort: remotePort,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	response := struct {
		ExecutionScope *sandboxobservability.ExecutionScope `json:"execution_scope,omitempty"`
	}{}
	if ok {
		response.ExecutionScope = &scope
	}
	writeJSON(w, http.StatusOK, response)
}
