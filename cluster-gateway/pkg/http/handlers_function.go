package http

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// invokeFunction invokes a sandbox function through procd.
// Route: /api/v1/sandboxes/:id/functions/:name/invoke
func (s *Server) invokeFunction(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	name := c.Param("name")
	if name == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "function name is required")
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/functions/"+url.PathEscape(name)+"/invoke")
}
