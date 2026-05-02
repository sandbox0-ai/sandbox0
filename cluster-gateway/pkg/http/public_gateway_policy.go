package http

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	nethttp "net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"golang.org/x/time/rate"
)

type publicGatewayMatch struct {
	route         *mgr.PublicGatewayRoute
	pathMatched   bool
	methodAllowed bool
}

func (s *Server) getSandboxForPublicExposure(c *gin.Context, sandboxID string) (*mgr.Sandbox, error) {
	if s.exposureSandboxCache != nil {
		if sandbox, ok := s.exposureSandboxCache.Get(sandboxID); ok {
			return sandbox, nil
		}
	}
	sandbox, err := s.managerClient.GetSandboxInternal(c.Request.Context(), sandboxID)
	if err != nil {
		return nil, err
	}
	if s.exposureSandboxCache != nil {
		s.exposureSandboxCache.Set(sandboxID, sandbox)
	}
	return sandbox, nil
}

func matchPublicGatewayRoute(cfg *mgr.PublicGatewayConfig, port int, path string, method string) publicGatewayMatch {
	if cfg == nil || !cfg.Enabled {
		return publicGatewayMatch{}
	}
	requestMethod := strings.ToUpper(strings.TrimSpace(method))
	var best *mgr.PublicGatewayRoute
	bestLen := -1
	for i := range cfg.Routes {
		route := &cfg.Routes[i]
		if route.Port != port {
			continue
		}
		prefix := route.PathPrefix
		if prefix == "" {
			prefix = "/"
		}
		if !pathMatchesPrefix(path, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			best = route
			bestLen = len(prefix)
		}
	}
	if best == nil {
		return publicGatewayMatch{}
	}
	return publicGatewayMatch{
		route:         best,
		pathMatched:   true,
		methodAllowed: publicGatewayMethodAllowed(best, requestMethod),
	}
}

func pathMatchesPrefix(path, prefix string) bool {
	if prefix == "" || prefix == "/" {
		return true
	}
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, strings.TrimRight(prefix, "/")+"/")
}

func publicGatewayMethodAllowed(route *mgr.PublicGatewayRoute, method string) bool {
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

func (s *Server) enforcePublicGatewayRoute(c *gin.Context, sandboxID string, route *mgr.PublicGatewayRoute) bool {
	if route == nil {
		return true
	}
	if handled := s.handlePublicGatewayCORS(c, route); handled {
		return false
	}
	if !s.authorizePublicGatewayRoute(c, route) {
		return false
	}
	if !s.allowPublicGatewayRate(c, sandboxID, route) {
		spec.JSONError(c, nethttp.StatusTooManyRequests, spec.CodeUnavailable, "rate limit exceeded")
		return false
	}
	if route.RewritePrefix != nil {
		rewritePublicGatewayPath(c, route.PathPrefix, *route.RewritePrefix)
	}
	return true
}

func (s *Server) handlePublicGatewayCORS(c *gin.Context, route *mgr.PublicGatewayRoute) bool {
	cors := route.CORS
	if cors == nil {
		return false
	}
	origin := c.GetHeader("Origin")
	if origin == "" {
		return false
	}
	if !originAllowed(origin, cors.AllowedOrigins) {
		if c.Request.Method == nethttp.MethodOptions {
			spec.JSONError(c, nethttp.StatusForbidden, spec.CodeForbidden, "origin is not allowed")
			return true
		}
		return false
	}
	c.Header("Access-Control-Allow-Origin", allowedOriginHeader(origin, cors.AllowedOrigins))
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
	if requestMethod != "" && !publicGatewayCORSMethodAllowed(route, requestMethod) {
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

func publicGatewayCORSMethodAllowed(route *mgr.PublicGatewayRoute, method string) bool {
	if route == nil || route.CORS == nil || len(route.CORS.AllowedMethods) == 0 {
		return publicGatewayMethodAllowed(route, method)
	}
	for _, allowed := range route.CORS.AllowedMethods {
		if allowed == "*" || strings.EqualFold(allowed, method) {
			return true
		}
	}
	return false
}

func originAllowed(origin string, allowed []string) bool {
	for _, candidate := range allowed {
		if candidate == "*" || strings.EqualFold(candidate, origin) {
			return true
		}
	}
	return false
}

func allowedOriginHeader(origin string, allowed []string) string {
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

func (s *Server) authorizePublicGatewayRoute(c *gin.Context, route *mgr.PublicGatewayRoute) bool {
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
		spec.JSONError(c, nethttp.StatusForbidden, spec.CodeForbidden, "unsupported route auth")
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

func (s *Server) allowPublicGatewayRate(c *gin.Context, sandboxID string, route *mgr.PublicGatewayRoute) bool {
	if route.RateLimit == nil {
		return true
	}
	key := fmt.Sprintf("%s:%s:%d:%d", sandboxID, route.ID, route.RateLimit.RPS, route.RateLimit.Burst)
	limiter := rate.NewLimiter(rate.Limit(route.RateLimit.RPS), route.RateLimit.Burst)
	actual, _ := s.publicGatewayLimiters.LoadOrStore(key, limiter)
	if !actual.(*rate.Limiter).Allow() {
		c.Header("Retry-After", "1")
		return false
	}
	return true
}

func publicGatewayProxyTimeout(defaultTimeout time.Duration, route *mgr.PublicGatewayRoute) time.Duration {
	if route != nil && route.TimeoutSeconds > 0 {
		return time.Duration(route.TimeoutSeconds) * time.Second
	}
	return defaultTimeout
}

func publicGatewayHasTimeout(route *mgr.PublicGatewayRoute) bool {
	return route != nil && route.TimeoutSeconds > 0
}

func rewritePublicGatewayPath(c *gin.Context, matchedPrefix, rewritePrefix string) {
	req := c.Request
	if req == nil || req.URL == nil {
		return
	}
	prefix := matchedPrefix
	if prefix == "" {
		prefix = "/"
	}
	suffix := strings.TrimPrefix(req.URL.Path, prefix)
	if prefix != "/" && req.URL.Path != prefix && suffix == req.URL.Path {
		return
	}
	req.URL.Path = joinGatewayPath(rewritePrefix, suffix)
	req.URL.RawPath = ""
}

func joinGatewayPath(prefix, suffix string) string {
	if prefix == "" {
		prefix = "/"
	}
	if suffix == "" {
		return prefix
	}
	return strings.TrimRight(prefix, "/") + "/" + strings.TrimLeft(suffix, "/")
}
