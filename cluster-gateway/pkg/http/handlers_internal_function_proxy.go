package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
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

const internalFunctionRuntimeReadinessTimeout = 500 * time.Millisecond

func (s *Server) proxyInternalFunctionRuntime(c *gin.Context) {
	_, _, targetURL, ok := s.resolveInternalFunctionRuntimeTarget(c)
	if !ok {
		return
	}

	proxyPath := c.Param("path")
	if proxyPath == "" {
		proxyPath = "/"
	}
	if !strings.HasPrefix(proxyPath, "/") {
		proxyPath = "/" + proxyPath
	}
	c.Request.URL.Path = proxyPath
	c.Request.URL.RawPath = ""
	c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)

	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}
	router, err := proxy.NewRouter(
		targetURL.String(),
		s.logger,
		proxyTimeout,
		proxy.WithHTTPClient(s.outboundHTTPClient()),
		proxy.WithTrustedForwardedHeaders(),
		proxy.WithRequestModifier(stripInternalFunctionProxyHeaders),
	)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	router.ProxyToTarget(c)
}

func (s *Server) checkInternalFunctionRuntimeReadiness(c *gin.Context) {
	_, _, targetURL, ok := s.resolveInternalFunctionRuntimeTarget(c)
	if !ok {
		return
	}

	healthPath := strings.TrimSpace(c.Query("health_path"))
	var err error
	if healthPath != "" {
		err = s.checkInternalFunctionRuntimeHTTPReadiness(c.Request.Context(), targetURL, healthPath)
	} else {
		err = checkInternalFunctionRuntimeTCPReadiness(c.Request.Context(), targetURL.Host)
	}
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) resolveInternalFunctionRuntimeTarget(c *gin.Context) (*mgr.Sandbox, mgr.SandboxAppService, *url.URL, bool) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing internal team context")
		return nil, mgr.SandboxAppService{}, nil, false
	}

	sandboxID := strings.TrimSpace(c.Param("id"))
	serviceID := strings.TrimSpace(c.Param("service_id"))
	if sandboxID == "" || serviceID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and service_id are required")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	if s.managerClient == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager client unavailable")
		return nil, mgr.SandboxAppService{}, nil, false
	}

	sandbox, err := s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Warn("Failed to get function runtime sandbox from manager",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			return nil, mgr.SandboxAppService{}, nil, false
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	if sandbox.TeamID != authCtx.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	if sandboxWantsPaused(sandbox) {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	service, ok := sandboxServiceByID(sandbox.Services, serviceID)
	if !ok {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "service not found")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	if basePort, parseErr := portFromURL(sandbox.InternalAddr); parseErr == nil && basePort == service.Port {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "reserved port is not exposable")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	targetURL, err := withPort(sandbox.InternalAddr, service.Port)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return nil, mgr.SandboxAppService{}, nil, false
	}
	return sandbox, service, targetURL, true
}

func sandboxServiceByID(services []mgr.SandboxAppService, serviceID string) (mgr.SandboxAppService, bool) {
	for _, service := range services {
		if service.ID == serviceID {
			return service, true
		}
	}
	return mgr.SandboxAppService{}, false
}

func (s *Server) checkInternalFunctionRuntimeHTTPReadiness(ctx context.Context, targetURL *url.URL, healthPath string) error {
	healthURL := *targetURL
	if !strings.HasPrefix(healthPath, "/") {
		healthPath = "/" + healthPath
	}
	healthURL.Path = healthPath
	healthURL.RawPath = ""
	healthURL.RawQuery = ""
	healthURL.ForceQuery = false
	healthURL.Fragment = ""

	attemptCtx, cancel := context.WithTimeout(ctx, internalFunctionRuntimeReadinessTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, healthURL.String(), nil)
	if err != nil {
		return err
	}
	healthClient := *s.outboundHTTPClient()
	healthClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := healthClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health check returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func checkInternalFunctionRuntimeTCPReadiness(ctx context.Context, address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("function service target address is empty")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, internalFunctionRuntimeReadinessTimeout)
	defer cancel()
	dialer := net.Dialer{Timeout: internalFunctionRuntimeReadinessTimeout}
	conn, err := dialer.DialContext(attemptCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("function service did not accept TCP connections on %s: %w", address, err)
	}
	_ = conn.Close()
	return nil
}

func stripInternalFunctionProxyHeaders(req *http.Request) {
	req.Header.Del(internalauth.DefaultTokenHeader)
	req.Header.Del(internalauth.TeamIDHeader)
	req.Header.Del("X-User-ID")
	req.Header.Del("X-Auth-Method")
}
