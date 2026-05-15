package http

import (
	"context"
	"io"
	"net/http"
	neturl "net/url"
	"strings"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) getSandboxFromClusterGateway(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
	clusterGatewayURL, err := s.clusterGatewayURLForSandbox(ctx, sandboxID)
	if err != nil || clusterGatewayURL == "" {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway is not configured"}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clusterGatewayURL+"/internal/v1/sandboxes/"+neturl.PathEscape(sandboxID), nil)
	if err != nil {
		return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to create cluster gateway request"}
	}
	token, err := s.internalAuthGen.GenerateSystem(internalauth.ServiceClusterGateway, internalauth.GenerateOptions{})
	if err != nil {
		return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to generate internal token"}
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway unavailable"}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, publishError{status: http.StatusNotFound, code: spec.CodeNotFound, message: "sandbox not found"}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("Cluster gateway sandbox lookup failed",
			zap.String("sandbox_id", sandboxID),
			zap.Int("status", resp.StatusCode),
			zap.String("body", strings.TrimSpace(string(body))),
		)
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "sandbox unavailable"}
	}

	sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
	if err != nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "failed to decode sandbox response"}
	}
	if apiErr != nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: apiErr.Message}
	}
	if sandbox == nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "sandbox response was empty"}
	}
	return sandbox, nil
}

type publishError struct {
	status  int
	code    string
	message string
}

func (e publishError) Error() string {
	return e.message
}
