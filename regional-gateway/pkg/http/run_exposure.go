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
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/runs"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type runClaimResponse struct {
	SandboxID string  `json:"sandbox_id"`
	ClusterID *string `json:"cluster_id,omitempty"`
}

type runContextResponse struct {
	ID string `json:"id"`
}

func (s *Server) proxyRunNoRoute(c *gin.Context) bool {
	if !s.cfg.PublicExposureEnabled || s.runRepo == nil {
		return false
	}
	host := normalizeHost(c.Request.Host)
	label, ok := s.runLabelFromHost(host)
	if !ok {
		return false
	}
	active, err := s.runRepo.GetActiveRevisionByDomainLabel(c.Request.Context(), label)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "run not found")
			return true
		}
		s.logger.Error("Failed to resolve run host", zap.String("host", host), zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "run registry unavailable")
		return true
	}
	if !active.Run.Enabled {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "run is disabled")
		return true
	}
	runtime, err := s.ensureRunRuntime(c, active)
	if err != nil {
		s.logger.Warn("Failed to prepare run runtime",
			zap.String("run_id", active.Run.ID),
			zap.String("revision_id", active.Revision.ID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "run is starting")
		return true
	}

	c.Request.Header.Del("X-Sandbox-ID")
	c.Request.Header.Del("X-Exposure-Port")
	c.Request.Header.Del("X-Sandbox0-Run-ID")
	c.Request.Header.Set("X-Sandbox-ID", runtime.RuntimeSandboxID)
	c.Request.Header.Set("X-Exposure-Port", fmt.Sprintf("%d", runtime.Spec.Service.Port))
	c.Request.Header.Set("X-Sandbox0-Run-ID", active.Run.ID)
	c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)

	targetURL := s.cfg.DefaultClusterGatewayURL
	if s.schedulerRouter != nil && strings.TrimSpace(runtime.RuntimeClusterID) != "" {
		authCtx := &authn.AuthContext{TeamID: active.Run.TeamID, UserID: active.Run.CreatedBy}
		if clusterURL, err := s.getClusterGatewayURLForCluster(c.Request.Context(), runtime.RuntimeClusterID, authCtx); err == nil && clusterURL != "" {
			targetURL = clusterURL
		}
	}
	router, err := s.getClusterGatewayProxy(targetURL)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to route run")
		return true
	}
	router.ProxyToTarget(c)
	return true
}

func (s *Server) runLabelFromHost(host string) (string, bool) {
	root := strings.TrimSpace(s.cfg.PublicRunRootDomain)
	if root == "" {
		root = runs.DefaultPublicRunRootDomain
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

func (s *Server) ensureRunRuntime(c *gin.Context, active *runs.ActiveRevision) (*runs.RunRevision, error) {
	revision := active.Revision
	if strings.TrimSpace(revision.RuntimeSandboxID) != "" {
		return &revision, nil
	}
	err := s.runRepo.WithRevisionLock(c.Request.Context(), revision.ID, func(ctx context.Context, locked *runs.RunRevision) error {
		if strings.TrimSpace(locked.RuntimeSandboxID) != "" {
			revision = *locked
			return nil
		}
		prepared, err := s.createRunRuntime(ctx, active.Run, locked)
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

func (s *Server) createRunRuntime(ctx context.Context, fn runs.Run, revision *runs.RunRevision) (*runs.RunRevision, error) {
	userID := strings.TrimSpace(fn.CreatedBy)
	if userID == "" {
		userID = fn.TeamID
	}

	claimMounts := make([]map[string]string, 0, len(revision.Spec.Mounts))
	for _, mount := range revision.Spec.Mounts {
		volumeID, err := s.materializeRunVolume(ctx, fn.TeamID, userID, mount.SnapshotID)
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
		ttl = runs.DefaultScalePolicy().IdleTimeoutSeconds
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
	var claim runClaimResponse
	if err := s.doRunJSON(ctx, targetURL, targetService, http.MethodPost, "/api/v1/sandboxes", fn.TeamID, userID, []string{authn.PermSandboxCreate, authn.PermSandboxVolumeRead}, claimBody, &claim); err != nil {
		return nil, fmt.Errorf("claim run runtime sandbox: %w", err)
	}
	if claim.SandboxID == "" {
		return nil, fmt.Errorf("claim run runtime returned empty sandbox_id")
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
	contextID, err := s.startRunService(ctx, clusterURL, fn.TeamID, userID, runtime.RuntimeSandboxID, serviceSpec)
	if err != nil {
		return nil, err
	}
	runtime.RuntimeContextID = contextID
	if err := s.waitRunServiceReady(ctx, clusterURL, fn.TeamID, userID, runtime.RuntimeSandboxID, serviceSpec, fn.Scale.StartupTimeoutSeconds); err != nil {
		return nil, err
	}
	return &runtime, nil
}

func (s *Server) startRunService(ctx context.Context, clusterURL, teamID, userID, sandboxID string, serviceSpec service.SandboxAppService) (string, error) {
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
		var created runContextResponse
		path := "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/contexts"
		if err := s.doRunJSON(ctx, clusterURL, internalauth.ServiceClusterGateway, http.MethodPost, path, teamID, userID, []string{authn.PermSandboxWrite}, body, &created); err != nil {
			return "", fmt.Errorf("start run service command: %w", err)
		}
		return created.ID, nil
	case service.SandboxAppServiceRuntimeWarmProcess:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported run service runtime %q", serviceSpec.Runtime.Type)
	}
}

func (s *Server) waitRunServiceReady(ctx context.Context, clusterURL, teamID, userID, sandboxID string, serviceSpec service.SandboxAppService, timeoutSeconds int) error {
	if serviceSpec.HealthCheck == nil || strings.TrimSpace(serviceSpec.HealthCheck.Path) == "" {
		return nil
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = runs.DefaultScalePolicy().StartupTimeoutSeconds
	}
	deadline, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	path := "/internal/v1/runs/runtime/sandboxes/" + url.PathEscape(sandboxID) + "/services/" + url.PathEscape(serviceSpec.ID) + "/health"
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		var ready map[string]any
		err := s.doRunJSON(deadline, clusterURL, internalauth.ServiceClusterGateway, http.MethodGet, path, teamID, userID, []string{authn.PermSandboxRead}, nil, &ready)
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-deadline.Done():
			return fmt.Errorf("run service did not become ready: %w", lastErr)
		case <-ticker.C:
		}
	}
}
