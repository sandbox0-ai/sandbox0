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
	"go.uber.org/zap"
)

const defaultFunctionAutoResumeTimeout = 30 * time.Second
const defaultFunctionRuntimeStartTimeout = 30 * time.Second

type functionRouteMatch struct {
	route         *mgr.SandboxAppServiceRoute
	pathMatched   bool
	methodAllowed bool
}

type functionContextResponse struct {
	ID      string `json:"id"`
	Running bool   `json:"running"`
	Paused  bool   `json:"paused"`
}

type functionRuntimeHTTPError struct {
	status int
	body   string
}

func (e functionRuntimeHTTPError) Error() string {
	return fmt.Sprintf("cluster gateway returned status %d: %s", e.status, e.body)
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

	sandbox, rev, err := s.resolveFunctionSandbox(c.Request.Context(), fn, rev, service)
	if err != nil {
		s.writeFunctionRuntimeError(c, fn, rev, "function sandbox is not available", err)
		return
	}
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
	sourceSandbox, err := s.getSandboxFromClusterGateway(ctx, rev.SourceSandboxID)
	if err == nil {
		return sourceSandbox, rev, nil
	}
	if !isPublishNotFound(err) {
		return nil, rev, err
	}
	return s.ensureRestoredFunctionSandbox(ctx, fn, rev, service)
}

func (s *Server) ensureRestoredFunctionSandbox(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService) (*mgr.Sandbox, *functions.Revision, error) {
	lock := s.revisionRuntimeLock(rev.ID)
	lock.Lock()
	defer lock.Unlock()

	latest, err := s.functionRepo.GetRevision(ctx, fn.TeamID, fn.ID, rev.ID)
	if err == nil {
		rev = latest
	} else if err != nil && !errorsIsFunctionNotFound(err) {
		return nil, rev, err
	}

	if rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "" {
		sandbox, err := s.getSandboxFromClusterGateway(ctx, strings.TrimSpace(*rev.RuntimeSandboxID))
		if err == nil {
			contextID, runtimeErr := s.ensureFunctionServiceRuntime(ctx, fn, rev, sandbox, service)
			if runtimeErr != nil {
				return nil, rev, runtimeErr
			}
			if rev.RuntimeContextID == nil || strings.TrimSpace(*rev.RuntimeContextID) != contextID {
				updated, updateErr := s.functionRepo.SetRevisionRuntime(ctx, fn.TeamID, fn.ID, rev.ID, sandbox.ID, contextID)
				if updateErr != nil {
					return nil, rev, updateErr
				}
				rev = updated
			}
			return sandbox, rev, nil
		}
		if !isPublishNotFound(err) {
			return nil, rev, err
		}
		if clearErr := s.functionRepo.ClearRevisionRuntime(ctx, fn.TeamID, fn.ID, rev.ID); clearErr != nil && !errorsIsFunctionNotFound(clearErr) {
			s.logger.Warn("Failed to clear stale function runtime sandbox",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("runtime_sandbox_id", strings.TrimSpace(*rev.RuntimeSandboxID)),
				zap.Error(clearErr),
			)
		}
		rev.RuntimeSandboxID = nil
		rev.RuntimeContextID = nil
		rev.RuntimeUpdatedAt = nil
	}

	claim, err := s.claimFunctionSandboxViaClusterGateway(ctx, fn, rev, service)
	if err != nil {
		return nil, rev, err
	}
	sandbox, err := s.getSandboxFromClusterGateway(ctx, claim.SandboxID)
	if err != nil {
		return nil, rev, err
	}
	contextID, err := s.ensureFunctionServiceRuntime(ctx, fn, rev, sandbox, service)
	if err != nil {
		s.deleteFunctionRuntimeSandboxBestEffort(fn, rev, sandbox.ID, "runtime startup failed")
		return nil, rev, err
	}
	updated, err := s.functionRepo.SetRevisionRuntime(ctx, fn.TeamID, fn.ID, rev.ID, sandbox.ID, contextID)
	if err != nil {
		return nil, rev, err
	}
	return sandbox, updated, nil
}

func (s *Server) revisionRuntimeLock(revisionID string) *sync.Mutex {
	lock := &sync.Mutex{}
	actual, _ := s.runtimeLocks.LoadOrStore(revisionID, lock)
	return actual.(*sync.Mutex)
}

func errorsIsFunctionNotFound(err error) bool {
	return errors.Is(err, functions.ErrNotFound)
}

func isPublishNotFound(err error) bool {
	var publishErr publishError
	if errors.As(err, &publishErr) {
		return publishErr.status == nethttp.StatusNotFound
	}
	return false
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
		if err == nil && (current.Running || current.Paused) {
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
	if err := waitForFunctionServicePort(startCtx, sandbox.InternalAddr, service.Port); err != nil {
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
	payload := map[string]any{
		"cwd":      service.Runtime.CWD,
		"env_vars": service.Runtime.EnvVars,
	}
	switch service.Runtime.Type {
	case mgr.SandboxAppServiceRuntimeCMD:
		payload["type"] = "cmd"
		payload["cmd"] = map[string]any{"command": service.Runtime.Command}
	case mgr.SandboxAppServiceRuntimeWarmProcess:
		if strings.TrimSpace(service.Runtime.WarmProcessName) == "" {
			return "", fmt.Errorf("warm_process_name is required for warm process functions")
		}
		payload["type"] = "repl"
		payload["repl"] = map[string]any{"alias": strings.TrimSpace(service.Runtime.WarmProcessName)}
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

func waitForFunctionServicePort(ctx context.Context, internalAddr string, port int) error {
	targetURL, err := withPort(internalAddr, port)
	if err != nil {
		return err
	}
	address := targetURL.Host
	if address == "" {
		return fmt.Errorf("function service target address is empty")
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
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
