package http

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/db"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/proxy"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// === Process/Context Management Handlers (→ Procd) ===

// createContext creates a new context in a sandbox
func (s *Server) createContext(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return // Error response already sent
	}

	// Rewrite path for procd
	c.Request.URL.Path = "/api/v1/contexts"

	s.proxyToProcd(c, procdURL)
}

// listContexts lists all contexts in a sandbox
func (s *Server) listContexts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id and ctx_id are required"})
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id and ctx_id are required"})
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id and ctx_id are required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/restart"
	s.proxyToProcd(c, procdURL)
}

// executeInContext executes code/command in a context
func (s *Server) executeInContext(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id and ctx_id are required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/execute"
	s.proxyToProcd(c, procdURL)
}

// contextWebSocket handles WebSocket connections for context
func (s *Server) contextWebSocket(c *gin.Context) {
	sandboxID := c.Param("id")
	ctxID := c.Param("ctx_id")
	if sandboxID == "" || ctxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id and ctx_id are required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	// Handle WebSocket upgrade
	wsProxy := proxy.NewWebSocketProxy(s.logger)
	c.Request.URL.Path = "/api/v1/contexts/" + ctxID + "/ws"
	wsProxy.Proxy(procdURL)(c)
}

// getProcdURL resolves the procd URL for a sandbox
func (s *Server) getProcdURL(c *gin.Context, sandboxID string) (*url.URL, error) {
	authCtx := middleware.GetAuthContext(c)

	// Look up sandbox
	sandbox, err := s.repo.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		if err == db.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
		} else {
			s.logger.Error("Failed to get sandbox",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve sandbox"})
		}
		return nil, err
	}

	// Check team ownership
	if sandbox.TeamID != authCtx.TeamID {
		c.JSON(http.StatusForbidden, gin.H{"error": "sandbox belongs to a different team"})
		return nil, db.ErrNotFound
	}

	// Parse procd address
	procdURL, err := url.Parse(sandbox.ProcdAddress)
	if err != nil {
		s.logger.Error("Invalid procd address",
			zap.String("sandbox_id", sandboxID),
			zap.String("procd_address", sandbox.ProcdAddress),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid procd address"})
		return nil, err
	}

	return procdURL, nil
}

// proxyToProcd proxies a request to a specific procd instance
func (s *Server) proxyToProcd(c *gin.Context, procdURL *url.URL) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for procd
	internalToken, err := s.internalAuthGen.Generate("procd", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
	if err != nil {
		s.logger.Error("Failed to generate internal token for procd",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	// Generate a special token for procd to communicate with storage-proxy
	// This token allows procd to access storage-proxy on behalf of this team
	procdStorageToken, err := s.procdAuthGen.Generate("storage-proxy", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{
		Permissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
	})
	if err != nil {
		s.logger.Error("Failed to generate procd-storage token",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	// Set headers
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	c.Request.Header.Set("X-Internal-Token", internalToken)
	c.Request.Header.Set("X-Token-For-Procd", procdStorageToken)

	// Create and execute reverse proxy
	director := func(req *http.Request) {
		req.URL.Scheme = procdURL.Scheme
		req.URL.Host = procdURL.Host
		req.Host = procdURL.Host
	}

	proxy := &reverseProxy{
		director: director,
		logger:   s.logger,
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}

// reverseProxy is a simple reverse proxy implementation
type reverseProxy struct {
	director func(*http.Request)
	logger   *zap.Logger
}

func (p *reverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.director(r)

	client := &http.Client{}
	resp, err := client.Do(r)
	if err != nil {
		p.logger.Error("Proxy request failed", zap.Error(err))
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error": "upstream service unavailable"}`))
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(resp.StatusCode)

	// Copy response body
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
}
