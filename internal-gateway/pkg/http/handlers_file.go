package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/proxy"
)

// === File System Handlers (→ Procd) ===

// handleFileOperation handles all file operations (GET, POST, DELETE)
// Route: /api/v1/sandboxes/:id/files/*path
func (s *Server) handleFileOperation(c *gin.Context) {
	sandboxID := c.Param("id")
	filePath := c.Param("path")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return // Error response already sent
	}

	// Determine the operation based on query params and method
	method := c.Request.Method
	query := c.Request.URL.Query()

	switch method {
	case http.MethodGet:
		if query.Get("stat") == "true" {
			// File stat operation
			c.Request.URL.Path = "/api/v1/files" + filePath
			c.Request.URL.RawQuery = "stat=true"
		} else if query.Get("list") == "true" {
			// List directory operation
			c.Request.URL.Path = "/api/v1/files" + filePath
			c.Request.URL.RawQuery = "list=true"
		} else {
			// Read file operation
			c.Request.URL.Path = "/api/v1/files" + filePath
		}

	case http.MethodPost:
		if query.Get("mkdir") == "true" {
			// Create directory operation
			c.Request.URL.Path = "/api/v1/files" + filePath
			c.Request.URL.RawQuery = "mkdir=true"
		} else if filePath == "/move" || c.Request.URL.Path == "/api/v1/sandboxes/"+sandboxID+"/files/move" {
			// Move file operation
			c.Request.URL.Path = "/api/v1/files/move"
		} else {
			// Write file operation
			c.Request.URL.Path = "/api/v1/files" + filePath
		}

	case http.MethodDelete:
		// Delete file/directory operation
		c.Request.URL.Path = "/api/v1/files" + filePath

	default:
		c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "method not allowed"})
		return
	}

	s.proxyToProcd(c, procdURL)
}

// handleFileWatch handles WebSocket connection for file watching
// Route: WS /api/v1/sandboxes/:id/files/watch
func (s *Server) handleFileWatch(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	// Handle WebSocket upgrade for file watching
	wsProxy := proxy.NewWebSocketProxy(s.logger)
	c.Request.URL.Path = "/api/v1/files/watch"
	wsProxy.Proxy(procdURL)(c)
}
