package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

// === File System Handlers (→ Procd) ===

// handleFileOperation handles file operations (GET, POST, DELETE).
// Route: /api/v1/sandboxes/:id/files
func (s *Server) handleFileOperation(c *gin.Context) {
	sandboxID := c.Param("id")
	filePath := c.Query("path")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}
	if filePath == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return // Error response already sent
	}

	c.Request.URL.Path = "/api/v1/files"

	s.proxyToProcd(c, procdURL)
}

// handleFileWatch handles WebSocket connection for file watching
// Route: WS /api/v1/sandboxes/:id/files/watch
func (s *Server) handleFileWatch(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
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

	// Handle WebSocket upgrade for file watching
	wsProxy := proxy.NewWebSocketProxy(s.logger, proxy.WithRequestModifier(requestModifier))
	c.Request.URL.Path = "/api/v1/files/watch"
	wsProxy.Proxy(procdURL)(c)
}

// handleFileMove handles file/directory move operations.
// Route: /api/v1/sandboxes/:id/files/move
func (s *Server) handleFileMove(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/files/move"
	s.proxyToProcd(c, procdURL)
}

// handleFileStat handles file stat operations.
// Route: /api/v1/sandboxes/:id/files/stat
func (s *Server) handleFileStat(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/files/stat"
	s.proxyToProcd(c, procdURL)
}

// handleFileList handles directory listing operations.
// Route: /api/v1/sandboxes/:id/files/list
func (s *Server) handleFileList(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/files/list"
	s.proxyToProcd(c, procdURL)
}
