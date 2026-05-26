package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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
	if s.functionRepo == nil {
		return false
	}
	label, ok := s.functionLabelFromHost(hostWithoutPort(c.Request.Host))
	if !ok {
		return false
	}
	active, err := s.functionRepo.GetActiveRevisionByDomainLabel(c.Request.Context(), label)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return true
		}
		s.logger.Error("Failed to resolve function host", zap.String("host", c.Request.Host), zap.Error(err))
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
	c.Request.Header.Set("X-Sandbox-ID", runtime.RuntimeSandboxID)
	c.Request.Header.Set("X-Exposure-Port", fmt.Sprintf("%d", runtime.Spec.Service.Port))
	s.handlePublicExposureNoRoute(c)
	return true
}

func (s *Server) functionLabelFromHost(host string) (string, bool) {
	root := strings.TrimSpace(s.cfg.PublicRootDomain)
	if root == "" {
		root = defaultPublicRootDomain
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
		claimMounts = append(claimMounts, map[string]string{"sandboxvolume_id": volumeID, "mount_point": mount.MountPath})
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
	var claim functionClaimResponse
	if err := s.doFunctionJSON(ctx, s.cfg.ManagerURL, internalauth.ServiceManager, http.MethodPost, "/api/v1/sandboxes", fn.TeamID, userID, []string{gatewayauthn.PermSandboxCreate, gatewayauthn.PermSandboxVolumeRead}, claimBody, &claim); err != nil {
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
	sandbox, err := s.managerClient.GetSandbox(ctx, runtime.RuntimeSandboxID, userID, fn.TeamID)
	if err != nil {
		return nil, err
	}
	contextID, err := s.startFunctionService(ctx, sandbox.InternalAddr, fn.TeamID, userID, serviceSpec)
	if err != nil {
		return nil, err
	}
	runtime.RuntimeContextID = contextID
	if err := s.waitFunctionServiceReady(ctx, sandbox.InternalAddr, serviceSpec, fn.Scale.StartupTimeoutSeconds); err != nil {
		return nil, err
	}
	return &runtime, nil
}

func (s *Server) startFunctionService(ctx context.Context, procdURL, teamID, userID string, serviceSpec mgr.SandboxAppService) (string, error) {
	if serviceSpec.Runtime == nil {
		return "", nil
	}
	switch serviceSpec.Runtime.Type {
	case mgr.SandboxAppServiceRuntimeCMD:
		body := map[string]any{
			"type":            "cmd",
			"wait_until_done": false,
			"cmd":             map[string]any{"command": serviceSpec.Runtime.Command},
			"cwd":             serviceSpec.Runtime.CWD,
			"env_vars":        serviceSpec.Runtime.EnvVars,
			"ttl_sec":         0,
		}
		var created functionContextResponse
		if err := s.doFunctionJSON(ctx, procdURL, internalauth.ServiceProcd, http.MethodPost, "/api/v1/contexts", teamID, userID, []string{gatewayauthn.PermSandboxWrite}, body, &created); err != nil {
			return "", fmt.Errorf("start function service command: %w", err)
		}
		return created.ID, nil
	case mgr.SandboxAppServiceRuntimeWarmProcess:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported function service runtime %q", serviceSpec.Runtime.Type)
	}
}

func (s *Server) waitFunctionServiceReady(ctx context.Context, sandboxInternalAddr string, serviceSpec mgr.SandboxAppService, timeoutSeconds int) error {
	if serviceSpec.HealthCheck == nil || strings.TrimSpace(serviceSpec.HealthCheck.Path) == "" {
		return nil
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = functions.DefaultScalePolicy().StartupTimeoutSeconds
	}
	target, err := withPort(sandboxInternalAddr, serviceSpec.Port)
	if err != nil {
		return err
	}
	target.Path = serviceSpec.HealthCheck.Path
	deadline, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		req, err := http.NewRequestWithContext(deadline, http.MethodGet, target.String(), nil)
		if err == nil {
			resp, doErr := s.outboundHTTPClient().Do(req)
			if doErr == nil && resp != nil {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 400 {
					return nil
				}
				lastErr = fmt.Errorf("health returned %d", resp.StatusCode)
			} else if doErr != nil {
				lastErr = doErr
			}
		} else {
			lastErr = err
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("function service did not become ready: %w", lastErr)
		case <-ticker.C:
		}
	}
}
