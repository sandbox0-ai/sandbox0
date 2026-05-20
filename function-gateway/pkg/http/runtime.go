package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

const defaultFunctionAutoResumeTimeout = 30 * time.Second
const defaultFunctionRuntimeStartTimeout = 30 * time.Second
const functionRuntimeRestoreLockPrefix = "function-runtime-restore:"
const functionServiceReadinessProbeInterval = 200 * time.Millisecond
const functionServiceReadinessTCPTimeout = 500 * time.Millisecond

type functionRouteMatch struct {
	route         *mgr.SandboxAppServiceRoute
	pathMatched   bool
	methodAllowed bool
}

type functionContextResponse struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Alias   string            `json:"alias"`
	Command []string          `json:"command,omitempty"`
	CWD     string            `json:"cwd"`
	EnvVars map[string]string `json:"env_vars"`
	Running bool              `json:"running"`
	Paused  bool              `json:"paused"`
}

type functionContextListResponse struct {
	Contexts []functionContextResponse `json:"contexts"`
}

type functionRuntimeHTTPError struct {
	status int
	body   string
}

func (e functionRuntimeHTTPError) Error() string {
	return fmt.Sprintf("cluster gateway returned status %d: %s", e.status, e.body)
}

func (s *Server) serveFunctionRevision(c *gin.Context, fn *functions.Function, rev *functions.Revision) {
	requestStarted := time.Now()
	routeID := ""
	servedRevision := rev
	var runtimeInstance *functions.RuntimeInstance
	defer func() {
		s.observeFunctionRequest(c, fn, servedRevision, routeID, runtimeInstance, requestStarted)
	}()

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
	if match.route != nil {
		routeID = strings.TrimSpace(match.route.ID)
	}
	setFunctionSpanAttributes(c.Request.Context(), fn, rev, routeID, nil)
	if !match.pathMatched {
		spec.JSONError(c, nethttp.StatusNotFound, spec.CodeNotFound, "route is not exposed")
		return
	}
	if c.Request.Method == nethttp.MethodOptions && match.route.CORS != nil {
		if !s.enforceFunctionRoute(c, fn.TeamID, fn.ID, match.route) {
			return
		}
	}
	if !match.methodAllowed {
		spec.JSONError(c, nethttp.StatusMethodNotAllowed, spec.CodeForbidden, "method is not allowed")
		return
	}
	if !s.enforceFunctionRoute(c, fn.TeamID, fn.ID, match.route) {
		return
	}

	acquireCtx, acquireSpan := s.startFunctionSpan(c.Request.Context(), "function.runtime.acquire", fn, rev,
		attribute.String("sandbox0.function_route_id", routeID),
	)
	lease, rev, err := s.acquireFunctionRuntime(acquireCtx, fn, rev, service)
	finishSpan(acquireSpan, err)
	if err != nil {
		s.writeFunctionRuntimeError(c, fn, rev, "function sandbox is not available", err)
		return
	}
	servedRevision = rev
	if lease == nil || lease.Sandbox == nil {
		spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function sandbox is not available")
		return
	}
	defer lease.Done()
	runtimeInstance = lease.Instance
	setFunctionSpanAttributes(c.Request.Context(), fn, rev, routeID, runtimeInstance)
	sandbox := lease.Sandbox
	if sandbox.TeamID != fn.TeamID {
		spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function sandbox is invalid")
		return
	}
	if functionSandboxWantsPaused(sandbox) {
		resumeCtx, cancel := context.WithTimeout(c.Request.Context(), defaultFunctionAutoResumeTimeout)
		defer cancel()
		if err := s.resumeSandboxViaClusterGateway(resumeCtx, sandbox.ID, fn.TeamID, rev.CreatedBy); err != nil {
			s.logger.Warn("Function sandbox resume failed",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("sandbox_id", sandbox.ID),
				zap.Error(err),
			)
			spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "function sandbox is waking up")
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
	proxyCtx, proxySpan := s.startFunctionSpan(c.Request.Context(), "function.proxy", fn, rev,
		attribute.String("sandbox0.function_route_id", routeID),
		attribute.String("sandbox0.runtime_sandbox_id", sandbox.ID),
		attribute.Int("sandbox0.service_port", service.Port),
	)
	c.Request = c.Request.WithContext(proxyCtx)
	router.ProxyToTarget(c)
	status := c.Writer.Status()
	proxySpan.SetAttributes(attribute.Int("http.status_code", status))
	if status >= 500 {
		proxySpan.SetStatus(codes.Error, nethttp.StatusText(status))
	}
	proxySpan.End()
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

func (s *Server) enforceFunctionRoute(c *gin.Context, teamID, functionID string, route *mgr.SandboxAppServiceRoute) bool {
	if route == nil {
		return true
	}
	if handled := handleFunctionCORS(c, route); handled {
		return false
	}
	if !authorizeFunctionRoute(c, route) {
		return false
	}
	allowed, err := s.allowFunctionRoute(c, teamID, functionID, route)
	if err != nil {
		s.logger.Warn("Function route rate limiter failed",
			zap.String("function_id", functionID),
			zap.String("team_id", teamID),
			zap.String("route_id", route.ID),
			zap.Error(err),
		)
		spec.JSONError(c, nethttp.StatusServiceUnavailable, spec.CodeUnavailable, "rate limiter unavailable")
		return false
	}
	if !allowed {
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
	if route.Auth == nil || route.Auth.Mode == "" || route.Auth.Mode == mgr.SandboxAppServiceRouteAuthModeNone {
		return true
	}
	switch route.Auth.Mode {
	case mgr.SandboxAppServiceRouteAuthModeBearer:
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
	case mgr.SandboxAppServiceRouteAuthModeHeader:
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

func (s *Server) allowFunctionRoute(c *gin.Context, teamID, functionID string, route *mgr.SandboxAppServiceRoute) (bool, error) {
	if route == nil || route.RateLimit == nil {
		return true, nil
	}
	key := "function:team:" + teamID + ":function:" + functionID + ":route:" + route.ID
	decision, err := s.routeLimiter.Allow(c.Request.Context(), key, ratelimit.Limit{
		RPS:   route.RateLimit.RPS,
		Burst: route.RateLimit.Burst,
	})
	if err != nil {
		return false, err
	}
	if !decision.Allowed {
		c.Header("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(decision.RetryAfter)))
		return false, nil
	}
	return true, nil
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

func (s *Server) resolveFunctionSandbox(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService) (*mgr.Sandbox, *functions.Revision, error) {
	lease, updated, err := s.acquireFunctionRuntime(ctx, fn, rev, service)
	if err != nil {
		return nil, updated, err
	}
	if lease == nil {
		return nil, updated, fmt.Errorf("function runtime lease is empty")
	}
	defer lease.Done()
	return lease.Sandbox, updated, nil
}

func (s *Server) acquireFunctionRuntime(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService) (*functionRuntimeLease, *functions.Revision, error) {
	autoscaler := s.autoscaler
	if autoscaler == nil {
		autoscaler = newFunctionAutoscaler(s)
	}
	return autoscaler.acquire(ctx, fn, rev, service)
}

func (s *Server) withRevisionRuntimeDistributedLock(ctx context.Context, revisionID string, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	revisionID = strings.TrimSpace(revisionID)
	if s == nil || s.runtimeRestoreLocks == nil || revisionID == "" {
		return fn(ctx)
	}
	return s.runtimeRestoreLocks.WithExclusive(ctx, functionRuntimeRestoreLockPrefix+revisionID, fn)
}

func (s *Server) revisionRuntimeLock(revisionID string) *sync.Mutex {
	lock := &sync.Mutex{}
	actual, _ := s.runtimeLocks.LoadOrStore(revisionID, lock)
	return actual.(*sync.Mutex)
}

func errorsIsFunctionNotFound(err error) bool {
	return errors.Is(err, functions.ErrNotFound)
}

func (s *Server) claimFunctionSandboxViaClusterGateway(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService) (*mgr.ClaimResponse, error) {
	clusterGatewayURL := s.defaultClusterGatewayURL()
	targetService := internalauth.ServiceClusterGateway
	requestPath := "/api/v1/sandboxes"
	if schedulerURL := s.schedulerURL(); schedulerURL != "" {
		clusterGatewayURL = schedulerURL
		targetService = internalauth.ServiceScheduler
	}
	if clusterGatewayURL == "" {
		return nil, fmt.Errorf("cluster gateway is not configured")
	}
	autoResume := true
	runtimeService := service
	runtimeService.Ingress = mgr.SandboxAppServiceIngress{}
	claimMounts, err := s.claimMountsFromRevision(rev)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(mgr.ClaimRequest{
		Template: rev.SourceTemplateID,
		Config: &mgr.SandboxConfig{
			AutoResume: &autoResume,
			Services:   []mgr.SandboxAppService{runtimeService},
		},
		Mounts: claimMounts,
	})
	if err != nil {
		return nil, err
	}
	token, err := s.internalAuthGen.Generate(targetService, fn.TeamID, rev.CreatedBy, internalauth.GenerateOptions{
		Permissions: []string{
			authn.PermSandboxCreate,
			authn.PermSandboxRead,
			authn.PermSandboxWrite,
			authn.PermSandboxVolumeRead,
			authn.PermSandboxVolumeWrite,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, clusterGatewayURL+requestPath, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, functionRuntimeHTTPError{status: resp.StatusCode, body: strings.TrimSpace(string(body))}
	}
	claim, apiErr, err := spec.DecodeResponse[mgr.ClaimResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, errors.New(apiErr.Message)
	}
	if claim == nil || strings.TrimSpace(claim.SandboxID) == "" {
		return nil, fmt.Errorf("cluster gateway returned an empty sandbox claim")
	}
	return claim, nil
}

func (s *Server) claimMountsFromRevision(rev *functions.Revision) ([]mgr.ClaimMount, error) {
	mounts := rev.RestoreMounts
	if len(mounts) == 0 {
		return nil, nil
	}
	out := make([]mgr.ClaimMount, 0, len(mounts))
	for _, mount := range mounts {
		volumeID := strings.TrimSpace(mount.SandboxVolumeID)
		mountPoint := strings.TrimSpace(mount.MountPoint)
		if volumeID == "" {
			return nil, fmt.Errorf("function revision restore mount is missing sandbox volume id")
		}
		if mountPoint == "" {
			return nil, fmt.Errorf("function revision restore mount is missing mount point")
		}
		out = append(out, mgr.ClaimMount{
			SandboxVolumeID: volumeID,
			MountPoint:      mountPoint,
		})
	}
	return out, nil
}

func (s *Server) ensureFunctionServiceRuntime(ctx context.Context, fn *functions.Function, rev *functions.Revision, sandbox *mgr.Sandbox, service mgr.SandboxAppService) (string, error) {
	if sandbox == nil {
		return "", fmt.Errorf("sandbox is nil")
	}
	if rev.RuntimeContextID != nil && strings.TrimSpace(*rev.RuntimeContextID) != "" {
		contextID := strings.TrimSpace(*rev.RuntimeContextID)
		current, err := s.getFunctionRuntimeContext(ctx, sandbox.ID, fn.TeamID, rev.CreatedBy, contextID)
		if err == nil && functionRuntimeContextCanServe(current, service) {
			return contextID, nil
		}
		if err != nil && !isFunctionRuntimeStatus(err, nethttp.StatusNotFound) {
			s.logger.Warn("Failed to inspect function runtime context",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("sandbox_id", sandbox.ID),
				zap.String("context_id", contextID),
				zap.Error(err),
			)
		}
	}
	contextID, err := s.startFunctionServiceRuntime(ctx, sandbox.ID, fn.TeamID, rev.CreatedBy, service)
	if err != nil {
		return "", err
	}
	startCtx, cancel := context.WithTimeout(ctx, defaultFunctionRuntimeStartTimeout)
	defer cancel()
	if err := s.waitForFunctionServiceReadiness(startCtx, sandbox.InternalAddr, service); err != nil {
		return "", err
	}
	return contextID, nil
}

func (s *Server) getFunctionRuntimeContext(ctx context.Context, sandboxID, teamID, userID, contextID string) (*functionContextResponse, error) {
	resp, err := s.doFunctionRuntimeContextRequest(ctx, nethttp.MethodGet, sandboxID, teamID, userID, contextID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, functionRuntimeHTTPError{status: resp.StatusCode, body: strings.TrimSpace(string(body))}
	}
	out, err := decodeFunctionContextResponse(resp.Body)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) startFunctionServiceRuntime(ctx context.Context, sandboxID, teamID, userID string, service mgr.SandboxAppService) (string, error) {
	if service.Runtime == nil {
		return "", fmt.Errorf("function service runtime is missing")
	}
	if service.Runtime.Type == mgr.SandboxAppServiceRuntimeWarmProcess {
		return s.resolveFunctionWarmProcessRuntimeContext(ctx, sandboxID, teamID, userID, service.Runtime.WarmProcessName)
	}
	payload := map[string]any{
		"cwd":      service.Runtime.CWD,
		"env_vars": service.Runtime.EnvVars,
	}
	switch service.Runtime.Type {
	case mgr.SandboxAppServiceRuntimeCMD:
		payload["type"] = "cmd"
		payload["cmd"] = map[string]any{"command": service.Runtime.Command}
	default:
		return "", fmt.Errorf("unsupported function service runtime type %q", service.Runtime.Type)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	resp, err := s.doFunctionRuntimeContextRequest(ctx, nethttp.MethodPost, sandboxID, teamID, userID, "", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return "", functionRuntimeHTTPError{status: resp.StatusCode, body: strings.TrimSpace(string(respBody))}
	}
	out, err := decodeFunctionContextResponse(resp.Body)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out.ID) == "" {
		return "", fmt.Errorf("cluster gateway returned an empty runtime context")
	}
	return out.ID, nil
}

func (s *Server) resolveFunctionWarmProcessRuntimeContext(ctx context.Context, sandboxID, teamID, userID, warmProcessName string) (string, error) {
	contexts, err := s.getFunctionRuntimeContexts(ctx, sandboxID, teamID, userID)
	if err != nil {
		return "", err
	}
	selected, err := selectFunctionWarmProcessContext(contexts, warmProcessName)
	if err != nil {
		return "", err
	}
	return selected.ID, nil
}

func (s *Server) getFunctionRuntimeContexts(ctx context.Context, sandboxID, teamID, userID string) ([]functionContextResponse, error) {
	resp, err := s.doFunctionRuntimeContextRequest(ctx, nethttp.MethodGet, sandboxID, teamID, userID, "", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, functionRuntimeHTTPError{status: resp.StatusCode, body: strings.TrimSpace(string(body))}
	}
	out, err := decodeFunctionContextListResponse(resp.Body)
	if err != nil {
		return nil, err
	}
	return out.Contexts, nil
}

func functionRuntimeContextCanServe(current *functionContextResponse, service mgr.SandboxAppService) bool {
	if current == nil || (!current.Running && !current.Paused) || service.Runtime == nil {
		return false
	}
	if service.Runtime.Type != mgr.SandboxAppServiceRuntimeWarmProcess {
		return true
	}
	_, err := selectFunctionWarmProcessContext([]functionContextResponse{*current}, service.Runtime.WarmProcessName)
	return err == nil
}

func selectFunctionWarmProcessContext(contexts []functionContextResponse, warmProcessName string) (*functionContextResponse, error) {
	name := strings.TrimSpace(warmProcessName)
	if name == "" {
		return nil, fmt.Errorf("warm_process_name is required for warm process functions")
	}
	for i := range contexts {
		if strings.TrimSpace(contexts[i].Alias) == name {
			return validateFunctionWarmProcessContext(&contexts[i], name)
		}
	}
	for i := range contexts {
		if strings.TrimSpace(contexts[i].ID) == name {
			return validateFunctionWarmProcessContext(&contexts[i], name)
		}
	}
	return nil, fmt.Errorf("warm process %q was not found", name)
}

func validateFunctionWarmProcessContext(ctx *functionContextResponse, warmProcessName string) (*functionContextResponse, error) {
	if ctx == nil {
		return nil, fmt.Errorf("warm process %q was not found", warmProcessName)
	}
	if strings.TrimSpace(ctx.ID) == "" {
		return nil, fmt.Errorf("warm process %q context id is empty", warmProcessName)
	}
	if strings.TrimSpace(ctx.Type) != "cmd" {
		return nil, fmt.Errorf("warm process %q must reference a cmd context, got %q", warmProcessName, ctx.Type)
	}
	if !ctx.Running && !ctx.Paused {
		return nil, fmt.Errorf("warm process %q is not running", warmProcessName)
	}
	return ctx, nil
}

func decodeFunctionContextResponse(r io.Reader) (*functionContextResponse, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Success bool                     `json:"success"`
		Data    *functionContextResponse `json:"data,omitempty"`
		Error   *spec.Error              `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && (envelope.Success || envelope.Data != nil || envelope.Error != nil) {
		if envelope.Error != nil {
			return nil, errors.New(envelope.Error.Message)
		}
		if !envelope.Success {
			return nil, fmt.Errorf("context response was not successful")
		}
		if envelope.Data == nil {
			return nil, fmt.Errorf("context response missing data")
		}
		return envelope.Data, nil
	}
	var out functionContextResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func decodeFunctionContextListResponse(r io.Reader) (*functionContextListResponse, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Success bool                         `json:"success"`
		Data    *functionContextListResponse `json:"data,omitempty"`
		Error   *spec.Error                  `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && (envelope.Success || envelope.Data != nil || envelope.Error != nil) {
		if envelope.Error != nil {
			return nil, errors.New(envelope.Error.Message)
		}
		if !envelope.Success {
			return nil, fmt.Errorf("context list response was not successful")
		}
		if envelope.Data == nil {
			return nil, fmt.Errorf("context list response missing data")
		}
		return envelope.Data, nil
	}
	var out functionContextListResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Server) doFunctionRuntimeContextRequest(ctx context.Context, method, sandboxID, teamID, userID, contextID string, body io.Reader) (*nethttp.Response, error) {
	clusterGatewayURL, err := s.clusterGatewayURLForSandbox(ctx, sandboxID)
	if err != nil || clusterGatewayURL == "" {
		return nil, fmt.Errorf("cluster gateway is not configured")
	}
	token, err := s.internalAuthGen.Generate(internalauth.ServiceClusterGateway, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead, authn.PermSandboxWrite},
	})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}
	path := clusterGatewayURL + "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/contexts"
	if strings.TrimSpace(contextID) != "" {
		path += "/" + url.PathEscape(contextID)
	}
	req, err := nethttp.NewRequestWithContext(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return s.outboundHTTPClient().Do(req)
}

func (s *Server) deleteFunctionRuntimeSandboxBestEffort(fn *functions.Function, rev *functions.Revision, sandboxID, reason string) {
	if s == nil || fn == nil || rev == nil || strings.TrimSpace(sandboxID) == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.deleteSandboxViaClusterGateway(cleanupCtx, sandboxID, fn.TeamID, rev.CreatedBy); err != nil {
		logger := s.logger
		if logger == nil {
			logger = zap.NewNop()
		}
		logger.Warn("Failed to delete failed function runtime sandbox",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.String("sandbox_id", sandboxID),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func (s *Server) deleteSandboxViaClusterGateway(ctx context.Context, sandboxID, teamID, userID string) error {
	clusterGatewayURL, err := s.clusterGatewayURLForSandbox(ctx, sandboxID)
	if err != nil || clusterGatewayURL == "" {
		return fmt.Errorf("cluster gateway is not configured")
	}
	if s.internalAuthGen == nil {
		return fmt.Errorf("internal auth generator is not configured")
	}
	token, err := s.internalAuthGen.Generate(internalauth.ServiceClusterGateway, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxDelete, authn.PermSandboxRead},
	})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodDelete, clusterGatewayURL+"/api/v1/sandboxes/"+url.PathEscape(sandboxID), nil)
	if err != nil {
		return err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == nethttp.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return functionRuntimeHTTPError{status: resp.StatusCode, body: strings.TrimSpace(string(body))}
}

func isFunctionRuntimeStatus(err error, status int) bool {
	var runtimeErr functionRuntimeHTTPError
	if errors.As(err, &runtimeErr) {
		return runtimeErr.status == status
	}
	return false
}

func (s *Server) waitForFunctionServiceReadiness(ctx context.Context, internalAddr string, service mgr.SandboxAppService) error {
	if service.HealthCheck != nil && strings.TrimSpace(service.HealthCheck.Path) != "" {
		return s.waitForFunctionServiceHTTPHealth(ctx, internalAddr, service.Port, service.HealthCheck.Path)
	}
	return waitForFunctionServicePort(ctx, internalAddr, service.Port)
}

func (s *Server) waitForFunctionServiceHTTPHealth(ctx context.Context, internalAddr string, port int, healthPath string) error {
	healthURL, err := functionServiceHealthURL(internalAddr, port, healthPath)
	if err != nil {
		return err
	}
	healthClient := *s.outboundHTTPClient()
	healthClient.CheckRedirect = func(_ *nethttp.Request, _ []*nethttp.Request) error {
		return nethttp.ErrUseLastResponse
	}
	ticker := time.NewTicker(functionServiceReadinessProbeInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, healthURL.String(), nil)
		if err != nil {
			return err
		}
		resp, err := healthClient.Do(req)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				_ = resp.Body.Close()
				return nil
			}
			lastErr = fmt.Errorf("health check returned HTTP %d", resp.StatusCode)
			_ = resp.Body.Close()
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return fmt.Errorf("function service health check %s did not return HTTP 2xx before timeout: %w", healthURL.String(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitForFunctionServicePort(ctx context.Context, internalAddr string, port int) error {
	targetURL, err := withPort(internalAddr, port)
	if err != nil {
		return err
	}
	address := targetURL.Host
	if address == "" {
		return fmt.Errorf("function service target address is empty")
	}
	ticker := time.NewTicker(functionServiceReadinessProbeInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", address, functionServiceReadinessTCPTimeout)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("function service did not start listening on %s: %w", address, lastErr)
		case <-ticker.C:
		}
	}
}

func functionServiceHealthURL(internalAddr string, port int, healthPath string) (*url.URL, error) {
	targetURL, err := withPort(internalAddr, port)
	if err != nil {
		return nil, err
	}
	path := strings.TrimSpace(healthPath)
	if path == "" {
		return nil, fmt.Errorf("function service health check path is empty")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	targetURL.Path = path
	targetURL.RawPath = ""
	targetURL.RawQuery = ""
	targetURL.ForceQuery = false
	targetURL.Fragment = ""
	return targetURL, nil
}

func (s *Server) resumeSandboxViaClusterGateway(ctx context.Context, sandboxID, teamID, userID string) error {
	clusterGatewayURL, err := s.clusterGatewayURLForSandbox(ctx, sandboxID)
	if err != nil || clusterGatewayURL == "" {
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
