package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	service "github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/pkg/proxy"
	"go.uber.org/zap"
)

// === Process/Context Management Handlers (→ Procd) ===

// createContext creates a new context in a sandbox
func (s *Server) createContext(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	var req createContextRequestPayload
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
			return
		}
	}

	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var (
		exposurePort *int
		publicURL    string
	)
	if req.Type == "cmd" && req.Cmd != nil && req.Cmd.ExposePort != nil {
		if *req.Cmd.ExposePort <= 0 || *req.Cmd.ExposePort > 65535 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid expose_port")
			return
		}
		procdURL, err := s.getProcdURL(c, sandboxID)
		if err != nil {
			return
		}
		if portStr := procdURL.Port(); portStr != "" {
			if p, convErr := strconv.Atoi(portStr); convErr == nil && p == *req.Cmd.ExposePort {
				spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "expose_port conflicts with reserved procd port")
				return
			}
		}
		label, err := naming.BuildExposureHostLabel(sandboxID, *req.Cmd.ExposePort)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		publicURL = s.buildPublicExposureURL(label)
		exposurePort = req.Cmd.ExposePort
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return // Error response already sent
	}

	requestModifier, err := s.buildProcdRequestModifier(c)
	if err != nil {
		return
	}
	client := &http.Client{Timeout: s.cfg.ProxyTimeout.Duration}
	if client.Timeout == 0 {
		client.Timeout = 10 * time.Second
	}
	reqURL := *procdURL
	reqURL.Path = "/api/v1/contexts"
	upReq, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, reqURL.String(), bytes.NewReader(body))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	upReq.Header = c.Request.Header.Clone()
	requestModifier(upReq)

	resp, err := client.Do(upReq)
	if err != nil {
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to connect to sandbox process")
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 300 && exposurePort != nil {
		resume := true
		if req.Cmd != nil && req.Cmd.ExposeResume != nil {
			resume = *req.Cmd.ExposeResume
		}
		// Runtime precedence:
		// 1) sandbox.auto_resume (global gate, default false)
		// 2) cmd.expose_resume / exposed_ports[].resume (per-port gate)
		if err := s.upsertExposePortPolicy(c, sandboxID, *exposurePort, resume); err != nil {
			s.logger.Warn("Failed to update exposed port policy",
				zap.String("sandbox_id", sandboxID),
				zap.Int("port", *exposurePort),
				zap.Bool("resume", resume),
				zap.Error(err),
			)
		}
	}

	if publicURL != "" && len(respBody) > 0 {
		var payload map[string]any
		if json.Unmarshal(respBody, &payload) == nil {
			if data, ok := payload["data"].(map[string]any); ok {
				data["public_url"] = publicURL
				if exposurePort != nil {
					data["exposed_port"] = *exposurePort
				}
				if newBody, mErr := json.Marshal(payload); mErr == nil {
					respBody = newBody
				}
			}
		}
	}

	for k, vs := range resp.Header {
		for _, v := range vs {
			c.Writer.Header().Add(k, v)
		}
	}
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
}

type createContextRequestPayload struct {
	Type string `json:"type"`
	Cmd  *struct {
		Command      []string `json:"command,omitempty"`
		ExposePort   *int     `json:"expose_port,omitempty"`
		ExposeResume *bool    `json:"expose_resume,omitempty"`
	} `json:"cmd,omitempty"`
}

func (s *Server) buildPublicExposureURL(label string) string {
	rootDomain := s.cfg.PublicRootDomain
	if rootDomain == "" {
		rootDomain = "sandbox0.app"
	}
	regionID := s.cfg.PublicRegionID
	if regionID == "" {
		return ""
	}
	return "https://" + label + "." + regionID + "." + rootDomain
}

func (s *Server) upsertExposePortPolicy(c *gin.Context, sandboxID string, port int, resume bool) error {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		return errors.New("missing auth context")
	}
	sandbox, err := s.managerClient.GetSandbox(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID)
	if err != nil {
		return err
	}
	ports := make([]service.ExposedPortConfig, 0, len(sandbox.ExposedPorts)+1)
	updated := false
	for _, item := range sandbox.ExposedPorts {
		if item.Port == port {
			item.Resume = resume
			updated = true
		}
		ports = append(ports, item)
	}
	if !updated {
		ports = append(ports, service.ExposedPortConfig{
			Port:   port,
			Resume: resume,
		})
	}
	return s.managerClient.UpdateSandboxExposedPorts(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID, ports)
}

// listContexts lists all contexts in a sandbox
func (s *Server) listContexts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts"
	s.proxyToProcd(c, procdURL)
}

// getContext gets a specific context
func (s *Server) getContext(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID
	s.proxyToProcd(c, procdURL)
}

// deleteContext deletes a context
func (s *Server) deleteContext(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID
	s.proxyToProcd(c, procdURL)
}

// restartContext restarts a context
func (s *Server) restartContext(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/restart"
	s.proxyToProcd(c, procdURL)
}

// contextInput sends input to a context
func (s *Server) contextInput(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/input"
	s.proxyToProcd(c, procdURL)
}

// contextExec executes context input synchronously
func (s *Server) contextExec(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/exec"
	s.proxyToProcd(c, procdURL)
}

// contextResize resizes a context
func (s *Server) contextResize(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/resize"
	s.proxyToProcd(c, procdURL)
}

// contextSignal sends a signal to a context
func (s *Server) contextSignal(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/signal"
	s.proxyToProcd(c, procdURL)
}

// contextStats gets stats for a context
func (s *Server) contextStats(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/stats"
	s.proxyToProcd(c, procdURL)
}

// contextWebSocket handles WebSocket connections for context
func (s *Server) contextWebSocket(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and ctx_id are required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	requestModifier, err := s.buildProcdRequestModifier(c)
	if err != nil {
		return
	}

	// Handle WebSocket upgrade
	wsProxy := proxy.NewWebSocketProxy(s.logger, proxy.WithRequestModifier(requestModifier))
	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/ws"
	wsProxy.Proxy(procdURL)(c)
}

// getProcdURL resolves the procd URL for a sandbox
// Uses in-memory cache to reduce manager API calls and improve performance
func (s *Server) getProcdURL(c *gin.Context, sandboxID string) (*url.URL, error) {
	authCtx := middleware.GetAuthContext(c)

	// Try to get from cache first
	var addr *url.URL
	if cached, ok := s.sandboxAddrCache.Get(sandboxID); ok {
		addr = cached
		s.logger.Debug("Sandbox cache hit",
			zap.String("sandbox_id", sandboxID),
		)
	} else {
		// Cache miss - fetch from manager
		s.logger.Debug("Sandbox cache miss, fetching from manager",
			zap.String("sandbox_id", sandboxID),
		)

		sandbox, err := s.managerClient.GetSandbox(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID)
		if err != nil {
			s.logger.Error("Failed to get sandbox from manager",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			return nil, err
		}

		// Verify team ownership
		if sandbox.TeamID != authCtx.TeamID {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
			return nil, errors.New("sandbox belongs to a different team")
		}
		if sandbox.Paused && sandbox.AutoResume {
			resumeCtx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
			defer cancel()
			if err := s.managerClient.ResumeSandbox(resumeCtx, sandboxID, authCtx.UserID, authCtx.TeamID); err != nil {
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
				return nil, err
			}
			sandbox, err = s.managerClient.GetSandbox(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID)
			if err != nil {
				spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
				return nil, err
			}
		}
		if sandbox.Paused && !sandbox.AutoResume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused and auto_resume is disabled")
			return nil, errors.New("sandbox auto_resume is disabled")
		}

		// Parse procd address
		addr, err = url.Parse(sandbox.InternalAddr)
		if err != nil {
			s.logger.Error("Invalid procd address",
				zap.String("sandbox_id", sandboxID),
				zap.String("procd_address", sandbox.InternalAddr),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid procd address")
			return nil, err
		}

		// Store in cache for future requests
		s.sandboxAddrCache.Set(sandboxID, addr)
		s.logger.Debug("Sandbox cached",
			zap.String("sandbox_id", sandboxID),
			zap.String("internal_addr", addr.String()),
		)
	}

	return addr, nil
}

func (s *Server) buildProcdRequestModifier(c *gin.Context) (proxy.RequestModifier, error) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for procd
	internalToken, err := s.internalAuthGen.Generate("procd", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
	if err != nil {
		s.logger.Error("Failed to generate internal token for procd",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return nil, err
	}

	// Generate a special token for procd to communicate with storage-proxy
	// This token allows procd to access storage-proxy on behalf of this team
	perms := s.cfg.ProcdStoragePermissions
	if len(perms) == 0 {
		perms = []string{"sandboxvolume:read", "sandboxvolume:write"}
	}
	procdStorageToken, err := s.procdAuthGen.Generate("storage-proxy", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{
		Permissions: perms,
	})
	if err != nil {
		s.logger.Error("Failed to generate procd-storage token",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return nil, err
	}

	return func(req *http.Request) {
		req.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
		req.Header.Set(internalauth.DefaultTokenHeader, internalToken)
		req.Header.Set(internalauth.TokenForProcdHeader, procdStorageToken)
	}, nil
}

// proxyToProcd proxies a request to a specific procd instance
func (s *Server) proxyToProcd(c *gin.Context, procdURL *url.URL) {
	requestModifier, err := s.buildProcdRequestModifier(c)
	if err != nil {
		return
	}

	// Create and execute reverse proxy
	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}
	router, err := proxy.NewRouter(procdURL.String(), s.logger, proxyTimeout, proxy.WithRequestModifier(requestModifier))
	if err != nil {
		s.logger.Error("Failed to create procd proxy router",
			zap.String("procd_url", procdURL.String()),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}

	router.ProxyToTarget(c)
}
