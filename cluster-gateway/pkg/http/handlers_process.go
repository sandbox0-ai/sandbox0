package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// createProcess creates a process session in a sandbox.
func (s *Server) createProcess(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes")
}

// listProcesses lists process sessions in a sandbox.
func (s *Server) listProcesses(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes")
}

// getProcess gets a process session.
func (s *Server) getProcess(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID)
}

// deleteProcess deletes a process session.
func (s *Server) deleteProcess(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID)
}

// sendProcessEvent sends an input event to a process session.
func (s *Server) sendProcessEvent(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID+"/events")
}

// streamProcessEvents streams process events.
func (s *Server) streamProcessEvents(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID+"/events")
}

// processSignal sends a signal to a process session.
func (s *Server) processSignal(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID+"/signal")
}

// processPTYSize resizes a process PTY channel.
func (s *Server) processPTYSize(c *gin.Context) {
	sandboxID, processID, ok := requireSandboxProcessIDs(c)
	if !ok {
		return
	}
	channel := c.Param("channel")
	if channel == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "channel is required")
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/processes/"+processID+"/channels/"+channel+"/pty-size")
}

func requireSandboxProcessIDs(c *gin.Context) (string, string, bool) {
	sandboxID := c.Param("id")
	processID := c.Param("process_id")
	if sandboxID == "" || processID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and process_id are required")
		return "", "", false
	}
	return sandboxID, processID, true
}
