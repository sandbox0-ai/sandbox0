package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// === Volume File System Handlers (→ Manager Storage) ===

// handleVolumeFileOperation handles volume file operations (GET, POST, DELETE).
// Route: /api/v1/sandboxvolumes/:id/files
func (s *Server) handleVolumeFileOperation(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files"
	s.proxyToManagerStorage(c)
}

// handleVolumeFileWatch handles volume file watch websocket proxying.
// Route: WS /api/v1/sandboxvolumes/:id/files/watch
func (s *Server) handleVolumeFileWatch(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/watch"
	s.proxyToManagerStorage(c)
}

// handleVolumeFileArchiveImport handles volume tar archive imports.
// Route: /api/v1/sandboxvolumes/:id/files/archive
func (s *Server) handleVolumeFileArchiveImport(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/archive"
	s.proxyToManagerStorage(c)
}

// handleVolumeFileMove handles volume file move operations.
// Route: /api/v1/sandboxvolumes/:id/files/move
func (s *Server) handleVolumeFileMove(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/move"
	s.proxyToManagerStorage(c)
}

// handleVolumeFileStat handles volume file stat operations.
// Route: /api/v1/sandboxvolumes/:id/files/stat
func (s *Server) handleVolumeFileStat(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/stat"
	s.proxyToManagerStorage(c)
}

// handleVolumeFileList handles volume directory listing operations.
// Route: /api/v1/sandboxvolumes/:id/files/list
func (s *Server) handleVolumeFileList(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/list"
	s.proxyToManagerStorage(c)
}
