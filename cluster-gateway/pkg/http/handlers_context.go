package http

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

const defaultAutoResumeTimeout = 2 * time.Minute

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

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return // Error response already sent
	}

	requestModifier, err := s.buildProcdRequestModifier(c)
	if err != nil {
		return
	}
	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}
	reqURL := *procdURL
	reqURL.Path = "/api/v1/contexts"
	upReq, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, reqURL.String(), bytes.NewReader(body))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	upReq, cancel := proxy.ApplyRequestTimeout(upReq, proxyTimeout)
	defer cancel()
	upReq.Header = c.Request.Header.Clone()
	requestModifier(upReq)

	client := &http.Client{}
	resp, err := client.Do(upReq)
	if err != nil {
		if proxy.IsTimeoutError(err) {
			spec.JSONError(c, http.StatusGatewayTimeout, spec.CodeUnavailable, "sandbox process request timed out")
			return
		}
		spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "failed to connect to sandbox process")
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	for k, vs := range resp.Header {
		for _, v := range vs {
			c.Writer.Header().Add(k, v)
		}
	}
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
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

type sandboxAddrCacheKey struct {
	teamID    string
	sandboxID string
}

func sandboxCacheKey(teamID, sandboxID string) sandboxAddrCacheKey {
	return sandboxAddrCacheKey{
		teamID:    teamID,
		sandboxID: sandboxID,
	}
}

// getProcdURL resolves the procd URL for a sandbox
// Uses in-memory cache to reduce manager API calls and improve performance
func (s *Server) getProcdURL(c *gin.Context, sandboxID string) (*url.URL, error) {
	authCtx := middleware.GetAuthContext(c)
	cacheKey := sandboxCacheKey(authCtx.TeamID, sandboxID)

	// Try to get from cache first
	var addr *url.URL
	if cached, ok := s.sandboxAddrCache.Get(cacheKey); ok {
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
			if errors.Is(err, client.ErrSandboxNotFound) {
				spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			} else {
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
			}
			return nil, err
		}

		// Verify team ownership
		if sandbox.TeamID != authCtx.TeamID {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
			return nil, errors.New("sandbox belongs to a different team")
		}
		if sandboxWantsPaused(sandbox) && !sandbox.AutoResume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused and auto_resume is disabled")
			return nil, errors.New("sandbox auto_resume is disabled")
		}
		if sandboxWantsPaused(sandbox) {
			resumeCtx, cancel := context.WithTimeout(c.Request.Context(), defaultAutoResumeTimeout)
			defer cancel()
			if err := s.managerClient.ResumeSandbox(resumeCtx, sandboxID, authCtx.UserID, authCtx.TeamID); err != nil {
				s.logger.Warn("Resume sandbox failed",
					zap.String("sandbox_id", sandboxID),
					zap.Error(err),
				)
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
				return nil, err
			}
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
		s.sandboxAddrCache.Set(cacheKey, addr)
		s.logger.Debug("Sandbox cached",
			zap.String("sandbox_id", sandboxID),
			zap.String("internal_addr", addr.String()),
		)
	}

	return addr, nil
}

func sandboxWantsPaused(sandbox *mgr.Sandbox) bool {
	if sandbox == nil {
		return false
	}
	if sandbox.PowerState.Desired == mgr.SandboxPowerStatePaused {
		return true
	}
	return sandbox.Paused
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
