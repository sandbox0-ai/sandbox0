package http

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
)

func (s *Server) proxySessionCollection(c *gin.Context) {
	sandboxID, ok := requireSandboxID(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/sessions")
}

func (s *Server) proxySessionItem(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "")
}

func (s *Server) proxySessionDesiredState(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/desired-state")
}

func (s *Server) proxySessionAttempts(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/attempts")
}

func (s *Server) proxySessionInputs(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/inputs")
}

func (s *Server) proxySessionSignals(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/signals")
}

func (s *Server) proxySessionTerminal(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/terminal")
}

func (s *Server) proxySessionEvents(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/events")
}

func (s *Server) proxySessionEventStream(c *gin.Context) {
	s.proxySandboxSessionSuffix(c, "/events/stream")
}

func (s *Server) proxySessionWebSocket(c *gin.Context) {
	sandboxID, sessionID, ok := requireSandboxSessionIDs(c)
	if !ok {
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
	c.Request.URL.Path = "/api/v1/sessions/" + url.PathEscape(sessionID) + "/ws"
	proxy.NewWebSocketProxy(s.logger, proxy.WithRequestModifier(requestModifier)).Proxy(procdURL)(c)
}

func (s *Server) proxySandboxSessionSuffix(c *gin.Context, suffix string) {
	sandboxID, sessionID, ok := requireSandboxSessionIDs(c)
	if !ok {
		return
	}
	s.proxyToSandboxProcdPath(c, sandboxID, "/api/v1/sessions/"+url.PathEscape(sessionID)+suffix)
}

func requireSandboxSessionIDs(c *gin.Context) (string, string, bool) {
	sandboxID := c.Param("id")
	sessionID := c.Param("session_id")
	if sandboxID == "" || sessionID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and session_id are required")
		return "", "", false
	}
	return sandboxID, sessionID, true
}
