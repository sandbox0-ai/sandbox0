package http

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functionapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
)

func (s *Server) resolveFunctionClusterGatewayURL(ctx context.Context, sandboxID string) (string, error) {
	if s.schedulerRouter == nil {
		return strings.TrimRight(strings.TrimSpace(s.cfg.DefaultClusterGatewayURL), "/"), nil
	}
	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		return "", err
	}
	return s.getClusterGatewayURLForCluster(ctx, parsed.ClusterID, nil)
}

func (s *Server) functionSandboxLookup(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
	clusterGatewayURL, err := s.resolveFunctionClusterGatewayURL(ctx, sandboxID)
	if err != nil {
		return nil, functionapi.SandboxNotFoundError()
	}
	clusterGatewayURL = strings.TrimRight(strings.TrimSpace(clusterGatewayURL), "/")
	if clusterGatewayURL == "" {
		return nil, functionapi.SandboxUnavailableError("cluster gateway is not configured")
	}
	if s.internalAuthGen == nil {
		return nil, functionapi.SandboxUnavailableError("internal auth generator is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clusterGatewayURL+"/internal/v1/sandboxes/"+url.PathEscape(sandboxID), nil)
	if err != nil {
		return nil, functionapi.SandboxUnavailableError("failed to create cluster gateway request")
	}
	token, err := s.internalAuthGen.GenerateSystem(internalauth.ServiceClusterGateway, internalauth.GenerateOptions{})
	if err != nil {
		return nil, functionapi.SandboxUnavailableError("failed to generate internal token")
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, functionapi.SandboxUnavailableError("cluster gateway unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, functionapi.SandboxNotFoundError()
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("Function sandbox lookup failed",
			zap.String("sandbox_id", sandboxID),
			zap.Int("status", resp.StatusCode),
			zap.String("body", strings.TrimSpace(string(body))),
		)
		return nil, functionapi.SandboxUnavailableError("sandbox unavailable")
	}
	sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
	if err != nil {
		return nil, functionapi.SandboxUnavailableError("failed to decode sandbox response")
	}
	if apiErr != nil {
		return nil, functionapi.SandboxUnavailableError(apiErr.Message)
	}
	if sandbox == nil {
		return nil, functionapi.SandboxUnavailableError("sandbox response was empty")
	}
	return sandbox, nil
}
