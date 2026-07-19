package http

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

func (s *Server) getSandboxDetail(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	targetURL, err := s.resolveClusterGatewayTarget(c, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to resolve cluster-gateway for sandbox detail",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "cluster gateway unavailable")
		return
	}

	sandbox, statusCode, err := s.getAuditedSandboxDetailFromClusterGateway(c, targetURL, sandboxID, authCtx)
	if err != nil {
		if coreteamquota.IsUnavailable(err) {
			c.Header("Retry-After", "1")
		}
		s.logger.Warn("Failed to fetch sandbox detail from cluster-gateway",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
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

	if sandbox.TeamId != authCtx.TeamID {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		return
	}

	sshInfo := sharedssh.BuildConnectionInfo(s.cfg.SSHEndpointHost, s.cfg.SSHEndpointPort, sandbox.Id)
	if sshInfo != nil {
		sandbox.Ssh = &apispec.SandboxSSHConnection{Host: sshInfo.Host, Port: sshInfo.Port, Username: sshInfo.Username}
	} else {
		sandbox.Ssh = nil
	}
	spec.JSONSuccess(c, http.StatusOK, sandbox)
}

func (s *Server) getAuditedSandboxDetailFromClusterGateway(c *gin.Context, clusterGatewayURL, sandboxID string, authCtx *authn.AuthContext) (*apispec.Sandbox, int, error) {
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, strings.TrimRight(clusterGatewayURL, "/")+"/api/v1/sandboxes/"+sandboxID, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("create audited cluster-gateway sandbox request: %w", err)
	}
	token, err := s.generateForwardingInternalToken(
		authCtx,
		internalauth.ServiceClusterGateway,
		req,
	)
	if err != nil {
		status := http.StatusInternalServerError
		if coreteamquota.IsUnavailable(err) {
			status = http.StatusServiceUnavailable
		}
		return nil, status, fmt.Errorf("generate audited cluster-gateway token: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req, cancel := proxy.ApplyRequestTimeout(req, s.cfg.ProxyTimeout.Duration)
	defer cancel()
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("call audited cluster-gateway sandbox endpoint: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, fmt.Errorf("audited cluster-gateway sandbox endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	sandbox, apiErr, err := spec.DecodeResponse[apispec.Sandbox](resp.Body)
	if err != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("decode audited sandbox response: %w", err)
	}
	if apiErr != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("audited sandbox response error: %s", apiErr.Message)
	}
	if sandbox == nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("audited sandbox response was empty")
	}
	return sandbox, http.StatusOK, nil
}
