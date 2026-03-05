package http

import (
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func (s *Server) proxyPublicExposureNoRoute(c *gin.Context) {
	if !s.cfg.PublicExposureEnabled {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}
	host := normalizeHost(c.Request.Host)
	label, ok := s.exposureLabelFromHost(host)
	if !ok {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}
	sandboxID, port, err := naming.ParseExposureHostLabel(label)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}

	c.Request.Header.Set("X-Sandbox-ID", sandboxID)
	c.Request.Header.Set("X-Exposure-Port", strconv.Itoa(port))

	if s.schedulerRouter == nil {
		s.igRouter.ProxyToTarget(c)
		return
	}

	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid sandbox_id")
		return
	}
	targetURL := s.getClusterFromCache(parsed.ClusterID)
	if targetURL == "" {
		s.proxyToDefaultInternalGateway(c)
		return
	}
	router, err := s.getInternalGatewayProxy(targetURL)
	if err != nil {
		s.proxyToDefaultInternalGateway(c)
		return
	}
	router.ProxyToTarget(c)
}

func (s *Server) exposureLabelFromHost(host string) (string, bool) {
	root := strings.TrimSpace(s.cfg.PublicRootDomain)
	if root == "" {
		root = "sandbox0.app"
	}
	region := strings.TrimSpace(s.cfg.PublicRegionID)
	if region == "" {
		return "", false
	}
	suffix := "." + region + "." + root
	if !strings.HasSuffix(host, suffix) {
		return "", false
	}
	label := strings.TrimSuffix(host, suffix)
	label = strings.TrimSuffix(label, ".")
	if label == "" || strings.Contains(label, ".") {
		return "", false
	}
	return label, true
}

func normalizeHost(hostport string) string {
	host := hostport
	if strings.Contains(hostport, ":") {
		if h, _, err := net.SplitHostPort(hostport); err == nil {
			host = h
		}
	}
	return strings.ToLower(strings.TrimSpace(host))
}
