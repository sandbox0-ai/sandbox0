package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const defaultFunctionAutoResumeTimeout = 30 * time.Second

type functionRouteMatch struct {
	route         *mgr.SandboxAppServiceRoute
	pathMatched   bool
	methodAllowed bool
}

func (s *Server) serveFunctionRevision(c *gin.Context, fn *functions.Function, rev *functions.Revision) {
	var service mgr.SandboxAppService
	if err := json.Unmarshal(rev.ServiceSnapshot, &service); err != nil {
		s.logger.Warn("Failed to decode function service snapshot",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.Error(err),
		)
		spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function revision is invalid")
		return
	}

	match := matchFunctionRoute(service, c.Request.URL.Path, c.Request.Method)
	if !match.pathMatched {
		spec.JSONError(c, nethttp.StatusNotFound, spec.CodeNotFound, "route is not exposed")
		return
	}
	if c.Request.Method == nethttp.MethodOptions && match.route.CORS != nil {
		if !s.enforceFunctionRoute(c, fn.ID, match.route) {
			return
		}
	}
	if !match.methodAllowed {
		spec.JSONError(c, nethttp.StatusMethodNotAllowed, spec.CodeForbidden, "method is not allowed")
		return
	}
	if !s.enforceFunctionRoute(c, fn.ID, match.route) {
		return
	}

	sandbox, err := s.getSandboxFromClusterGateway(c.Request.Context(), rev.SourceSandboxID)
	if err != nil {
		s.writeFunctionRuntimeError(c, fn, rev, "function source sandbox is not available", err)
		return
	}
	if sandbox.TeamID != fn.TeamID {
		spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function source sandbox is invalid")
		return
	}
	if functionSandboxWantsPaused(sandbox) {
		resumeCtx, cancel := context.WithTimeout(c.Request.Context(), defaultFunctionAutoResumeTimeout)
		defer cancel()
		if err := s.resumeSandboxViaClusterGateway(resumeCtx, sandbox.ID, fn.TeamID, rev.CreatedBy); err != nil {
			s.logger.Warn("Function source sandbox resume failed",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("sandbox_id", sandbox.ID),
				zap.Error(err),
			)
			spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function source sandbox is waking up")
			return
		}
	}

	if basePort, parseErr := portFromURL(sandbox.InternalAddr); parseErr == nil && basePort == service.Port {
		spec.JSONError(c, nethttp.StatusForbidden, spec.CodeForbidden, "reserved port is not exposable")
		return
	}
	targetURL, err := withPort(sandbox.InternalAddr, service.Port)
	if err != nil {
		spec.JSONError(c, nethttp.StatusInternalServerError, spec.CodeInternal, "invalid sandbox address")
		return
	}
	proxyTimeout := s.cfg.ProxyTimeout.Duration
	if proxyTimeout == 0 {
		proxyTimeout = 30 * time.Second
	}
	proxyTimeout = functionRouteProxyTimeout(proxyTimeout, match.route)
	if !functionRouteHasTimeout(match.route) {
		c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
	}
	router, err := proxy.NewRouter(targetURL.String(), s.logger, proxyTimeout, proxy.WithHTTPClient(s.outboundHTTPClient()))
	if err != nil {
		spec.JSONError(c, nethttp.StatusInternalServerError, spec.CodeInternal, "proxy initialization failed")
		return
	}
	router.ProxyToTarget(c)
}

func matchFunctionRoute(service mgr.SandboxAppService, path string, method string) functionRouteMatch {
	if !service.Ingress.Public {
		return functionRouteMatch{}
	}
	requestMethod := strings.ToUpper(strings.TrimSpace(method))
	var best *mgr.SandboxAppServiceRoute
	bestLen := -1
	for i := range service.Ingress.Routes {
		route := &service.Ingress.Routes[i]
		prefix := route.PathPrefix
		if prefix == "" {
			prefix = "/"
		}
		if !functionPathMatchesPrefix(path, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			best = route
			bestLen = len(prefix)
		}
	}
	if best == nil {
		return functionRouteMatch{}
	}
	return functionRouteMatch{
		route:         best,
		pathMatched:   true,
		methodAllowed: functionRouteMethodAllowed(best, requestMethod),
	}
}

func functionPathMatchesPrefix(path, prefix string) bool {
	if prefix == "" || prefix == "/" {
		return true
	}
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, strings.TrimRight(prefix, "/")+"/")
}

func functionRouteMethodAllowed(route *mgr.SandboxAppServiceRoute, method string) bool {
	if route == nil || len(route.Methods) == 0 {
		return true
	}
	for _, allowed := range route.Methods {
		if allowed == "*" || strings.EqualFold(allowed, method) {
			return true
		}
	}
	return false
}

func (s *Server) enforceFunctionRoute(c *gin.Context, functionID string, route *mgr.SandboxAppServiceRoute) bool {
	if route == nil {
		return true
	}
	if handled := handleFunctionCORS(c, route); handled {
		return false
	}
	if !authorizeFunctionRoute(c, route) {
		return false
	}
	if !s.allowFunctionRoute(functionID, route) {
		spec.JSONError(c, nethttp.StatusTooManyRequests, spec.CodeUnavailable, "rate limit exceeded")
		return false
	}
	if route.RewritePrefix != nil {
		rewriteFunctionPath(c, route.PathPrefix, *route.RewritePrefix)
	}
	return true
}

func handleFunctionCORS(c *gin.Context, route *mgr.SandboxAppServiceRoute) bool {
	cors := route.CORS
	if cors == nil {
		return false
	}
	origin := c.GetHeader("Origin")
	if origin == "" {
		return false
	}
	if !functionOriginAllowed(origin, cors.AllowedOrigins) {
		if c.Request.Method == nethttp.MethodOptions {
			spec.JSONError(c, nethttp.StatusForbidden, spec.CodeForbidden, "origin is not allowed")
			return true
		}
		return false
	}
	c.Header("Access-Control-Allow-Origin", functionAllowedOriginHeader(origin, cors.AllowedOrigins))
	c.Header("Vary", "Origin")
	if cors.AllowCredentials {
		c.Header("Access-Control-Allow-Credentials", "true")
	}
	if len(cors.ExposeHeaders) > 0 {
		c.Header("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ", "))
	}
	if c.Request.Method != nethttp.MethodOptions {
		return false
	}
	requestMethod := strings.ToUpper(strings.TrimSpace(c.GetHeader("Access-Control-Request-Method")))
	if requestMethod != "" && !functionRouteCORSMethodAllowed(route, requestMethod) {
		spec.JSONError(c, nethttp.StatusMethodNotAllowed, spec.CodeForbidden, "method is not allowed")
		return true
	}
	allowedMethods := cors.AllowedMethods
	if len(allowedMethods) == 0 {
		allowedMethods = route.Methods
	}
	if len(allowedMethods) > 0 {
		c.Header("Access-Control-Allow-Methods", strings.Join(allowedMethods, ", "))
	}
	if len(cors.AllowedHeaders) > 0 {
		c.Header("Access-Control-Allow-Headers", strings.Join(cors.AllowedHeaders, ", "))
	}
	if cors.MaxAgeSeconds > 0 {
		c.Header("Access-Control-Max-Age", strconv.Itoa(cors.MaxAgeSeconds))
	}
	c.Status(nethttp.StatusNoContent)
	return true
}

func functionRouteCORSMethodAllowed(route *mgr.SandboxAppServiceRoute, method string) bool {
	if route == nil || route.CORS == nil || len(route.CORS.AllowedMethods) == 0 {
		return functionRouteMethodAllowed(route, method)
	}
	for _, allowed := range route.CORS.AllowedMethods {
		if allowed == "*" || strings.EqualFold(allowed, method) {
			return true
		}
	}
	return false
}

func functionOriginAllowed(origin string, allowed []string) bool {
	for _, candidate := range allowed {
		if candidate == "*" || strings.EqualFold(candidate, origin) {
			return true
		}
	}
	return false
}

func functionAllowedOriginHeader(origin string, allowed []string) string {
	for _, candidate := range allowed {
		if candidate == "*" {
			return "*"
		}
		if strings.EqualFold(candidate, origin) {
			return origin
		}
	}
	return ""
}

func authorizeFunctionRoute(c *gin.Context, route *mgr.SandboxAppServiceRoute) bool {
	if route.Auth == nil || route.Auth.Mode == "" || route.Auth.Mode == mgr.PublicGatewayAuthModeNone {
		return true
	}
	switch route.Auth.Mode {
	case mgr.PublicGatewayAuthModeBearer:
		token := strings.TrimSpace(c.GetHeader("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(token, prefix) {
			spec.JSONError(c, nethttp.StatusUnauthorized, spec.CodeUnauthorized, "missing bearer token")
			return false
		}
		if !sha256HexMatches(strings.TrimSpace(strings.TrimPrefix(token, prefix)), route.Auth.BearerTokenSHA256) {
			spec.JSONError(c, nethttp.StatusUnauthorized, spec.CodeUnauthorized, "invalid bearer token")
			return false
		}
	case mgr.PublicGatewayAuthModeHeader:
		if !sha256HexMatches(c.GetHeader(route.Auth.HeaderName), route.Auth.HeaderValueSHA256) {
			spec.JSONError(c, nethttp.StatusUnauthorized, spec.CodeUnauthorized, "invalid header credential")
			return false
		}
	default:
		spec.JSONError(c, nethttp.StatusUnauthorized, spec.CodeUnauthorized, "unsupported auth mode")
		return false
	}
	return true
}

func sha256HexMatches(value, expectedHex string) bool {
	expected, err := hex.DecodeString(strings.TrimSpace(expectedHex))
	if err != nil || len(expected) != sha256.Size {
		return false
	}
	sum := sha256.Sum256([]byte(value))
	return subtle.ConstantTimeCompare(sum[:], expected) == 1
}

func (s *Server) allowFunctionRoute(functionID string, route *mgr.SandboxAppServiceRoute) bool {
	if route == nil || route.RateLimit == nil {
		return true
	}
	key := functionID + ":" + route.ID
	limiter := rate.NewLimiter(rate.Limit(route.RateLimit.RPS), route.RateLimit.Burst)
	actual, _ := s.routeLimiters.LoadOrStore(key, limiter)
	return actual.(*rate.Limiter).Allow()
}

func functionRouteProxyTimeout(defaultTimeout time.Duration, route *mgr.SandboxAppServiceRoute) time.Duration {
	if route == nil || route.TimeoutSeconds <= 0 {
		return defaultTimeout
	}
	return time.Duration(route.TimeoutSeconds) * time.Second
}

func functionRouteHasTimeout(route *mgr.SandboxAppServiceRoute) bool {
	return route != nil && route.TimeoutSeconds > 0
}

func rewriteFunctionPath(c *gin.Context, matchedPrefix, rewritePrefix string) {
	original := c.Request.URL.Path
	matchedPrefix = strings.TrimRight(matchedPrefix, "/")
	if matchedPrefix == "" {
		matchedPrefix = "/"
	}
	suffix := original
	if matchedPrefix != "/" {
		suffix = strings.TrimPrefix(original, matchedPrefix)
	}
	rewritePrefix = strings.TrimRight(rewritePrefix, "/")
	if rewritePrefix == "" {
		rewritePrefix = "/"
	}
	if suffix == "" {
		c.Request.URL.Path = rewritePrefix
		return
	}
	if rewritePrefix == "/" {
		c.Request.URL.Path = "/" + strings.TrimLeft(suffix, "/")
		return
	}
	c.Request.URL.Path = rewritePrefix + "/" + strings.TrimLeft(suffix, "/")
}

func functionSandboxWantsPaused(sandbox *mgr.Sandbox) bool {
	if sandbox == nil {
		return false
	}
	if sandbox.PowerState.Desired == mgr.SandboxPowerStatePaused {
		return true
	}
	return sandbox.Paused
}

func (s *Server) resumeSandboxViaClusterGateway(ctx context.Context, sandboxID, teamID, userID string) error {
	clusterGatewayURL := strings.TrimRight(strings.TrimSpace(s.cfg.DefaultClusterGatewayURL), "/")
	if clusterGatewayURL == "" {
		return fmt.Errorf("cluster gateway is not configured")
	}
	token, err := s.internalAuthGen.Generate(internalauth.ServiceClusterGateway, teamID, userID, internalauth.GenerateOptions{})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, clusterGatewayURL+"/internal/v1/sandboxes/"+url.PathEscape(sandboxID)+"/resume", bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("create cluster gateway request: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == nethttp.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("cluster gateway resume returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *Server) writeFunctionRuntimeError(c *gin.Context, fn *functions.Function, rev *functions.Revision, message string, err error) {
	if err != nil {
		s.logger.Warn("Function runtime unavailable",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.String("source_sandbox_id", rev.SourceSandboxID),
			zap.String("source_service_id", rev.SourceServiceID),
			zap.Error(err),
		)
	}
	spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, message, gin.H{
		"function_id":       fn.ID,
		"revision_id":       rev.ID,
		"revision_number":   rev.RevisionNumber,
		"source_sandbox_id": rev.SourceSandboxID,
		"source_service_id": rev.SourceServiceID,
	})
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
