package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

const (
	defaultPublicRootDomain = "sandbox0.app"
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

	sandbox, err := s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
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
	policy, ok := findExposedPortPolicy(sandbox, port)
	if !ok {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "port is not exposed")
		return
	}
	if sandbox.Paused {
		// Resume precedence:
		// sandbox.auto_resume must be true, then per-port exposed_ports[].resume must be true.
		if !sandbox.AutoResume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused and auto_resume is disabled")
			return
		}
		if !policy.Resume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused and resume is disabled")
			return
		}
		resumeCtx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
		defer cancel()
		if err := s.managerClient.ResumeSandbox(resumeCtx, sandboxID, "", sandbox.TeamID); err != nil {
			s.logger.Warn("Auto resume failed", zap.String("sandbox_id", sandboxID), zap.Error(err))
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
			return
		}
		s.sandboxAddrCache.Delete(sandboxID)
		sandbox, err = s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
		if err != nil {
			s.logger.Warn("Failed to get sandbox after resume for public exposure",
				zap.String("sandbox_id", sandboxID),
				zap.Error(err),
			)
			if errors.Is(err, client.ErrSandboxNotFound) {
				spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
			} else {
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
			}
			return
		}
	}

	if basePort, parseErr := portFromURL(sandbox.InternalAddr); parseErr == nil && basePort == port {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "reserved port is not exposable")
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
	router, err := proxy.NewRouter(targetURL.String(), s.logger, proxyTimeout)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	router.ProxyToTarget(c)
}

func (s *Server) resolveExposureFromRequest(c *gin.Context) (sandboxID string, port int, _ string, err error) {
	if sb := strings.TrimSpace(c.GetHeader("X-Sandbox-ID")); sb != "" {
		p := strings.TrimSpace(c.GetHeader("X-Exposure-Port"))
		parsedPort, convErr := strconv.Atoi(p)
		if convErr != nil || parsedPort <= 0 || parsedPort > 65535 {
			return "", 0, "", fmt.Errorf("invalid forwarded exposure port")
		}
		return sb, parsedPort, "", nil
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

func findExposedPortPolicy(sandbox *mgr.Sandbox, port int) (mgr.ExposedPortConfig, bool) {
	for _, item := range sandbox.ExposedPorts {
		if item.Port == port {
			return item, true
		}
	}
	return mgr.ExposedPortConfig{}, false
}
