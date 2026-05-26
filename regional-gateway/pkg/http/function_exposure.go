package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	service "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type functionClaimResponse struct {
	SandboxID string  `json:"sandbox_id"`
	ClusterID *string `json:"cluster_id,omitempty"`
}

type functionContextResponse struct {
	ID string `json:"id"`
}

func (s *Server) proxyFunctionNoRoute(c *gin.Context) bool {
	if !s.cfg.PublicExposureEnabled || s.functionRepo == nil {
		return false
	}
	host := normalizeHost(c.Request.Host)
	label, ok := s.functionLabelFromHost(host)
	if !ok {
		return false
	}
	active, err := s.functionRepo.GetActiveRevisionByDomainLabel(c.Request.Context(), label)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return true
		}
		s.logger.Error("Failed to resolve function host", zap.String("host", host), zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function registry unavailable")
		return true
	}
	if !active.Function.Enabled {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function is disabled")
		return true
	}
	runtime, err := s.ensureFunctionRuntime(c, active)
	if err != nil {
		s.logger.Warn("Failed to prepare function runtime",
			zap.String("function_id", active.Function.ID),
			zap.String("revision_id", active.Revision.ID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function is starting")
		return true
	}

	c.Request.Header.Del("X-Sandbox-ID")
	c.Request.Header.Del("X-Exposure-Port")
	c.Request.Header.Del("X-Sandbox0-Function-ID")
	c.Request.Header.Set("X-Sandbox-ID", runtime.RuntimeSandboxID)
	c.Request.Header.Set("X-Exposure-Port", fmt.Sprintf("%d", runtime.Spec.Service.Port))
	c.Request.Header.Set("X-Sandbox0-Function-ID", active.Function.ID)
	c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)

	targetURL := s.cfg.DefaultClusterGatewayURL
	if s.schedulerRouter != nil && strings.TrimSpace(runtime.RuntimeClusterID) != "" {
		authCtx := &authn.AuthContext{TeamID: active.Function.TeamID, UserID: active.Function.CreatedBy}
		if clusterURL, err := s.getClusterGatewayURLForCluster(c.Request.Context(), runtime.RuntimeClusterID, authCtx); err == nil && clusterURL != "" {
			targetURL = clusterURL
		}
	}
	router, err := s.getClusterGatewayProxy(targetURL)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route function")
		return true
	}
	router.ProxyToTarget(c)
	return true
}

func (s *Server) functionLabelFromHost(host string) (string, bool) {
	root := strings.TrimSpace(s.cfg.PublicRootDomain)
	if root == "" {
		root = "sandbox0.app"
	}
	region := strings.TrimSpace(s.cfg.PublicRegionID)
	if region == "" {
		return "", false
	}
	suffix := ".fn." + region + "." + root
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

func (s *Server) ensureFunctionRuntime(c *gin.Context, active *functions.ActiveRevision) (*functions.FunctionRevision, error) {
	revision := active.Revision
	if strings.TrimSpace(revision.RuntimeSandboxID) != "" {
		return &revision, nil
	}
	err := s.functionRepo.WithRevisionLock(c.Request.Context(), revision.ID, func(ctx context.Context, locked *functions.FunctionRevision) error {
		if strings.TrimSpace(locked.RuntimeSandboxID) != "" {
			revision = *locked
			return nil
		}
		prepared, err := s.createFunctionRuntime(ctx, active.Function, locked)
		if err != nil {
			return err
		}
		locked.RuntimeSandboxID = prepared.RuntimeSandboxID
		locked.RuntimeClusterID = prepared.RuntimeClusterID
		locked.RuntimeContextID = prepared.RuntimeContextID
		revision = *locked
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

func (s *Server) createFunctionRuntime(ctx context.Context, fn functions.Function, revision *functions.FunctionRevision) (*functions.FunctionRevision, error) {
	userID := strings.TrimSpace(fn.CreatedBy)
	if userID == "" {
		userID = fn.TeamID
	}

	claimMounts := make([]map[string]string, 0, len(revision.Spec.Mounts))
	for _, mount := range revision.Spec.Mounts {
		volumeID, err := s.materializeFunctionVolume(ctx, fn.TeamID, userID, mount.SnapshotID)
		if err != nil {
			return nil, err
		}
		claimMounts = append(claimMounts, map[string]string{
			"sandboxvolume_id": volumeID,
			"mount_point":      mount.MountPath,
		})
	}

	serviceSpec := revision.Spec.Service
	serviceSpec.Ingress.Public = true
	for i := range serviceSpec.Ingress.Routes {
		serviceSpec.Ingress.Routes[i].Resume = true
	}

	ttl := fn.Scale.IdleTimeoutSeconds
	if ttl <= 0 {
		ttl = functions.DefaultScalePolicy().IdleTimeoutSeconds
	}
	claimBody := map[string]any{
		"template": revision.Spec.Template,
		"config": map[string]any{
			"auto_resume": true,
			"ttl":         ttl,
			"hard_ttl":    0,
			"services":    []any{serviceSpec},
			"env_vars":    revision.Spec.EnvVars,
		},
		"mounts": claimMounts,
	}
	targetURL := s.cfg.DefaultClusterGatewayURL
	targetService := internalauth.ServiceClusterGateway
	if s.schedulerRouter != nil {
		targetURL = s.cfg.SchedulerURL
		targetService = internalauth.ServiceScheduler
	}
	var claim functionClaimResponse
	if err := s.doFunctionJSON(ctx, targetURL, targetService, http.MethodPost, "/api/v1/sandboxes", fn.TeamID, userID, []string{authn.PermSandboxCreate, authn.PermSandboxVolumeRead}, claimBody, &claim); err != nil {
		return nil, fmt.Errorf("claim function runtime sandbox: %w", err)
	}
	if claim.SandboxID == "" {
		return nil, fmt.Errorf("claim function runtime returned empty sandbox_id")
	}
	runtime := *revision
	runtime.RuntimeSandboxID = claim.SandboxID
	if claim.ClusterID != nil {
		runtime.RuntimeClusterID = *claim.ClusterID
	}

	clusterURL := s.cfg.DefaultClusterGatewayURL
	if strings.TrimSpace(runtime.RuntimeClusterID) != "" && s.schedulerRouter != nil {
		authCtx := &authn.AuthContext{TeamID: fn.TeamID, UserID: userID}
		resolvedURL, err := s.getClusterGatewayURLForCluster(ctx, runtime.RuntimeClusterID, authCtx)
		if err != nil {
			return nil, err
		}
		if resolvedURL == "" {
			return nil, fmt.Errorf("runtime cluster not found")
		}
		clusterURL = resolvedURL
	}
	contextID, err := s.startFunctionService(ctx, clusterURL, fn.TeamID, userID, runtime.RuntimeSandboxID, serviceSpec)
	if err != nil {
		return nil, err
	}
	runtime.RuntimeContextID = contextID
	if err := s.waitFunctionServiceReady(ctx, clusterURL, fn.TeamID, userID, runtime.RuntimeSandboxID, serviceSpec, fn.Scale.StartupTimeoutSeconds); err != nil {
		return nil, err
	}
	return &runtime, nil
}

func (s *Server) startFunctionService(ctx context.Context, clusterURL, teamID, userID, sandboxID string, serviceSpec service.SandboxAppService) (string, error) {
	if serviceSpec.Runtime == nil {
		return "", nil
	}
	switch serviceSpec.Runtime.Type {
	case service.SandboxAppServiceRuntimeCMD:
		body := map[string]any{
			"type":            "cmd",
			"wait_until_done": false,
			"cmd": map[string]any{
				"command": serviceSpec.Runtime.Command,
			},
			"cwd":      serviceSpec.Runtime.CWD,
			"env_vars": serviceSpec.Runtime.EnvVars,
			"ttl_sec":  0,
		}
		var created functionContextResponse
		path := "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/contexts"
		if err := s.doFunctionJSON(ctx, clusterURL, internalauth.ServiceClusterGateway, http.MethodPost, path, teamID, userID, []string{authn.PermSandboxWrite}, body, &created); err != nil {
			return "", fmt.Errorf("start function service command: %w", err)
		}
		return created.ID, nil
	case service.SandboxAppServiceRuntimeWarmProcess:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported function service runtime %q", serviceSpec.Runtime.Type)
	}
}

func (s *Server) waitFunctionServiceReady(ctx context.Context, clusterURL, teamID, userID, sandboxID string, serviceSpec service.SandboxAppService, timeoutSeconds int) error {
	if serviceSpec.HealthCheck == nil || strings.TrimSpace(serviceSpec.HealthCheck.Path) == "" {
		return nil
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = functions.DefaultScalePolicy().StartupTimeoutSeconds
	}
	deadline, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	path := "/internal/v1/functions/runtime/sandboxes/" + url.PathEscape(sandboxID) + "/services/" + url.PathEscape(serviceSpec.ID) + "/health"
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		var ready map[string]any
		err := s.doFunctionJSON(deadline, clusterURL, internalauth.ServiceClusterGateway, http.MethodGet, path, teamID, userID, []string{authn.PermSandboxRead}, nil, &ready)
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-deadline.Done():
			return fmt.Errorf("function service did not become ready: %w", lastErr)
		case <-ticker.C:
		}
	}
}
