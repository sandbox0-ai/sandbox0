package http

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

const (
	defaultPublicRootDomain = "sandbox0.app"

	forwardedExposureSandboxIDHeader = "X-Sandbox-ID"
	forwardedExposurePortHeader      = "X-Exposure-Port"
)

func (s *Server) handlePublicExposureNoRoute(c *gin.Context) {
	if !s.cfg.PublicExposureEnabled {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}
	if s.managerClient == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager client unavailable")
		return
	}

	sandboxID, port, _, err := s.resolveExposureFromRequest(c)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "not found")
		return
	}

	sandbox, err := s.getSandboxForPublicExposure(c, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to get sandbox for public exposure",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound,
				fmt.Sprintf("sandbox %s not found", sandboxID))
		} else {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
		}
		return
	}
	// Ownership is authoritative after the manager lookup. Keep Team Quota
	// admission ahead of route and policy checks so rejected requests cannot
	// bypass request, connection, or network accounting.
	releaseTeamQuota, ok := s.teamQuotaController.AdmitPublicExposure(c, sandbox.TeamID)
	if !ok {
		return
	}
	defer releaseTeamQuota()
	match := matchSandboxServiceRoute(sandbox.Services, port, c.Request.URL.Path, c.Request.Method)
	if !match.pathMatched {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "route is not exposed")
		return
	}
	route := match.route
	if c.Request.Method == http.MethodOptions && route.CORS != nil {
		if !s.enforceSandboxServiceRoute(c, sandboxID, sandbox.TeamID, route) {
			return
		}
	}
	if !match.methodAllowed {
		spec.JSONError(c, http.StatusMethodNotAllowed, spec.CodeForbidden, "method is not allowed")
		return
	}
	if !s.enforceSandboxServiceRoute(c, sandboxID, sandbox.TeamID, route) {
		return
	}
	finishAudit, ok := s.beginExposureAudit(c, sandbox, match.service, route)
	if !ok {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.Set(exposureAuditPanickedKey, true)
			finishAudit()
			panic(recovered)
		}
		finishAudit()
	}()
	if route.Resume && !clientServiceHasRestartableRuntime(match.service) {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "resume-enabled route requires a restartable service runtime")
		return
	}
	if !sandboxServiceHasTimeout(route) {
		c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
		_ = proxy.DisableResponseWriteDeadline(c.Writer)
	}
	needsRuntimeRefetch := sandboxRuntimeMissing(sandbox)
	if sandboxNeedsRuntime(sandbox) {
		if !sandbox.AutoResume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is not running and auto_resume is disabled")
			return
		}
		if !route.Resume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is not running and resume is disabled")
			return
		}
		if err := s.managerClient.ResumeSandbox(c.Request.Context(), sandboxID, "", sandbox.TeamID); err != nil {
			s.logger.Warn("Auto resume failed", zap.String("sandbox_id", sandboxID), zap.Error(err))
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
			return
		}
		s.invalidateSandboxInternalCache(c.Request.Context(), sandboxID)
		if needsRuntimeRefetch {
			sandbox, err = s.getSandboxForPublicExposure(c, sandboxID)
			if err != nil {
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
				return
			}
		}
	}

	if sandboxServiceIsFunction(match.service) {
		s.executeSandboxFunctionExposure(c, sandbox, match.service, route)
		return
	}

	if basePort, parseErr := portFromURL(sandbox.InternalAddr); parseErr == nil && basePort == port {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "reserved port is not exposable")
		return
	}

	if err := s.ensureSandboxServiceRuntime(c.Request.Context(), sandbox, match.service, route); err != nil {
		s.logger.Warn("Sandbox service runtime is not ready",
			zap.String("sandbox_id", sandboxID),
			zap.String("service_id", match.service.ID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox service runtime unavailable")
		return
	}

	targetURL, err := withPort(sandbox.InternalAddr, port)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return
	}
	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 10 * time.Second
	}
	proxyTimeout = sandboxServiceProxyTimeout(proxyTimeout, route)
	if !sandboxServiceHasTimeout(route) {
		c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
	}
	router, err := proxy.NewRouter(targetURL.String(), s.logger, proxyTimeout, proxy.WithHTTPClient(s.outboundHTTPClient()))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	router.ProxyToTarget(c)
}

func clientServiceHasRestartableRuntime(service *mgr.SandboxAppService) bool {
	return service != nil && mgr.SandboxAppServiceHasRestartableRuntime(*service)
}

func (s *Server) resolveExposureFromRequest(c *gin.Context) (sandboxID string, port int, _ string, err error) {
	forwardedSandboxID := strings.TrimSpace(c.GetHeader(forwardedExposureSandboxIDHeader))
	forwardedPort := strings.TrimSpace(c.GetHeader(forwardedExposurePortHeader))
	hasForwardedIdentity := forwardedSandboxID != "" || forwardedPort != ""
	trustedForward := hasForwardedIdentity && s.isTrustedRegionalExposureForward(c)

	// Forwarding identity and its proof are hop-by-hop metadata. Never let
	// client-supplied values reach the exposed sandbox service.
	c.Request.Header.Del(forwardedExposureSandboxIDHeader)
	c.Request.Header.Del(forwardedExposurePortHeader)
	c.Request.Header.Del(internalauth.DefaultTokenHeader)

	if trustedForward {
		if forwardedSandboxID == "" {
			return "", 0, "", fmt.Errorf("missing forwarded sandbox id")
		}
		p := forwardedPort
		parsedPort, convErr := strconv.Atoi(p)
		if convErr != nil || parsedPort <= 0 || parsedPort > 65535 {
			return "", 0, "", fmt.Errorf("invalid forwarded exposure port")
		}
		return forwardedSandboxID, parsedPort, "", nil
	}

	host := hostWithoutPort(c.Request.Host)
	rootDomain := strings.TrimSpace(s.cfg.PublicRootDomain)
	if rootDomain == "" {
		rootDomain = defaultPublicRootDomain
	}
	regionID := strings.TrimSpace(s.cfg.PublicRegionID)
	if regionID == "" {
		return "", 0, "", fmt.Errorf("missing public region")
	}
	expectedSuffix := "." + regionID + "." + rootDomain
	if !strings.HasSuffix(host, expectedSuffix) {
		return "", 0, "", fmt.Errorf("host does not match exposure domain")
	}
	label := strings.TrimSuffix(host, expectedSuffix)
	label = strings.TrimSuffix(label, ".")
	if label == "" || strings.Contains(label, ".") {
		return "", 0, "", fmt.Errorf("invalid exposure label")
	}

	sandboxID, port, err = naming.ParseExposureHostLabel(label)
	if err != nil {
		return "", 0, "", err
	}
	return sandboxID, port, label, nil
}

func (s *Server) isTrustedRegionalExposureForward(c *gin.Context) bool {
	if s == nil || s.authMiddleware == nil || c == nil {
		return false
	}
	_, claims, err := s.authMiddleware.AuthenticateRequest(c)
	return err == nil &&
		claims != nil &&
		claims.IsSystem &&
		claims.Caller == internalauth.ServiceRegionalGateway
}

func withPort(raw string, port int) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	host := base.Hostname()
	if host == "" {
		return nil, fmt.Errorf("empty host")
	}
	base.Host = net.JoinHostPort(host, strconv.Itoa(port))
	return base, nil
}

func hostWithoutPort(hostport string) string {
	host := hostport
	if strings.Contains(hostport, ":") {
		if h, _, err := net.SplitHostPort(hostport); err == nil {
			host = h
		}
	}
	return strings.ToLower(strings.TrimSpace(host))
}

func portFromURL(raw string) (int, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return 0, err
	}
	p := u.Port()
	if p == "" {
		return 0, fmt.Errorf("port missing")
	}
	return strconv.Atoi(p)
}
