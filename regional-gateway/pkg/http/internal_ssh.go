package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	internalmiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap"
)

func (s *Server) resolveInternalSSHTarget(c *gin.Context) {
	userID := strings.TrimSpace(c.GetHeader(internalauth.UserIDHeader))
	if userID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "user_id header is required")
		return
	}

	sandboxID := strings.TrimSpace(c.Param("id"))
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	targetURL, err := s.resolveClusterGatewayTarget(c, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to resolve cluster-gateway for SSH target",
			zap.String("sandbox_id", sandboxID),
			zap.String("user_id", userID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "cluster gateway unavailable")
		return
	}

	sandbox, statusCode, err := s.getSandboxFromClusterGateway(c.Request.Context(), targetURL, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to fetch sandbox metadata for SSH target",
			zap.String("sandbox_id", sandboxID),
			zap.String("user_id", userID),
			zap.String("cluster_gateway_url", targetURL),
			zap.Error(err),
		)
		switch statusCode {
		case http.StatusNotFound:
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		default:
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox unavailable")
		}
		return
	}

	if _, err := s.teamMembership.GetTeamMember(c.Request.Context(), sandbox.TeamID, userID); err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox access denied")
			return
		}
		s.logger.Error("Failed to authorize SSH sandbox membership",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", sandbox.TeamID),
			zap.String("user_id", userID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "authorization failed")
		return
	}

	if sandboxWantsPausedRegional(sandbox) {
		if !sandbox.AutoResume {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is paused and auto_resume is disabled")
			return
		}
		if sandbox.PowerState.Desired != mgr.SandboxPowerStateActive {
			if err := s.resumeSandboxViaClusterGateway(c.Request.Context(), targetURL, sandboxID, sandbox.TeamID, userID); err != nil {
				s.logger.Warn("Failed to request SSH sandbox resume via cluster-gateway",
					zap.String("sandbox_id", sandboxID),
					zap.String("team_id", sandbox.TeamID),
					zap.String("user_id", userID),
					zap.String("cluster_gateway_url", targetURL),
					zap.Error(err),
				)
				spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox resume failed")
				return
			}
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
		return
	}

	if strings.TrimSpace(sandbox.InternalAddr) == "" {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox is waking up")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, &sharedssh.ResolvedTarget{
		SandboxID: sandboxID,
		TeamID:    sandbox.TeamID,
		UserID:    userID,
		ProcdURL:  sandbox.InternalAddr,
	})
}

func (s *Server) resolveClusterGatewayTarget(c *gin.Context, sandboxID string) (string, error) {
	if s.schedulerRouter == nil {
		if strings.TrimSpace(s.cfg.DefaultClusterGatewayURL) == "" {
			return "", fmt.Errorf("default cluster gateway URL is not configured")
		}
		return s.cfg.DefaultClusterGatewayURL, nil
	}

	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		return "", fmt.Errorf("parse sandbox name: %w", err)
	}

	authCtx := internalmiddleware.GetAuthContext(c)
	if authCtx == nil {
		authCtx = &authn.AuthContext{AuthMethod: authn.AuthMethodInternal}
	}

	targetURL, err := s.getClusterGatewayURLForCluster(c.Request.Context(), parsed.ClusterID, authCtx)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(targetURL) == "" {
		return "", fmt.Errorf("cluster gateway url not found for cluster %q", parsed.ClusterID)
	}
	return targetURL, nil
}

func sandboxWantsPausedRegional(sandbox *mgr.Sandbox) bool {
	if sandbox == nil {
		return false
	}
	if sandbox.PowerState.Desired == mgr.SandboxPowerStatePaused {
		return true
	}
	return sandbox.Paused
}

func (s *Server) getSandboxFromClusterGateway(ctx context.Context, clusterGatewayURL, sandboxID string) (*mgr.Sandbox, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(clusterGatewayURL, "/")+"/internal/v1/sandboxes/"+sandboxID, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("create cluster-gateway sandbox request: %w", err)
	}

	token, err := s.internalAuthGen.GenerateSystem(internalauth.ServiceClusterGateway, internalauth.GenerateOptions{})
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("generate cluster-gateway token: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req, cancel := proxy.ApplyRequestTimeout(req, s.cfg.ProxyTimeout.Duration)
	defer cancel()

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("call cluster-gateway sandbox endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, http.StatusNotFound, fmt.Errorf("sandbox not found")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, fmt.Errorf("cluster-gateway sandbox endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
	if err != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("decode cluster-gateway sandbox response: %w", err)
	}
	if apiErr != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("cluster-gateway sandbox response error: %s", apiErr.Message)
	}
	if sandbox == nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("cluster-gateway sandbox response was empty")
	}
	return sandbox, http.StatusOK, nil
}

func (s *Server) resumeSandboxViaClusterGateway(ctx context.Context, clusterGatewayURL, sandboxID, teamID, userID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(clusterGatewayURL, "/")+"/internal/v1/sandboxes/"+sandboxID+"/resume", strings.NewReader("{}"))
	if err != nil {
		return fmt.Errorf("create cluster-gateway resume request: %w", err)
	}

	token, err := s.internalAuthGen.Generate(
		internalauth.ServiceClusterGateway,
		teamID,
		userID,
		internalauth.GenerateOptions{},
	)
	if err != nil {
		return fmt.Errorf("generate cluster-gateway resume token: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set(internalauth.TeamIDHeader, teamID)
	req.Header.Set(internalauth.UserIDHeader, userID)
	req.Header.Set("Content-Type", "application/json")

	req, cancel := proxy.ApplyRequestTimeout(req, s.cfg.ProxyTimeout.Duration)
	defer cancel()

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("call cluster-gateway resume endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster-gateway resume endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
