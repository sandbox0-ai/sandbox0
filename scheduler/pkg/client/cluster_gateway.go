package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

// ClusterGatewayClient provides methods to call cluster-gateway APIs
type ClusterGatewayClient struct {
	internalAuthGen *internalauth.Generator
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewClusterGatewayClient creates a new cluster-gateway client
func NewClusterGatewayClient(internalAuthGen *internalauth.Generator, logger *zap.Logger, obsProvider *observability.Provider) *ClusterGatewayClient {
	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: 30 * time.Second,
	})

	return &ClusterGatewayClient{
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient:      httpClient,
	}
}

// GetSandboxTemplateSource gets durable source template context from the
// sandbox's owning cluster.
func (c *ClusterGatewayClient) GetSandboxTemplateSource(
	ctx context.Context,
	baseURL, sandboxID, teamID, userID string,
	permissions []string,
) (*template.SandboxTemplateSource, error) {
	token, err := c.internalAuthGen.Generate(
		internalauth.ServiceClusterGateway,
		teamID,
		userID,
		internalauth.GenerateOptions{Permissions: permissions},
	)
	if err != nil {
		return nil, fmt.Errorf("generate cluster-gateway token: %w", err)
	}
	requestURL := fmt.Sprintf("%s/internal/v1/sandboxes/%s/template-source", baseURL, url.PathEscape(sandboxID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create template source request: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		switch resp.StatusCode {
		case http.StatusNotFound:
			return nil, template.ErrTemplateSourceNotFound
		case http.StatusForbidden:
			return nil, template.ErrTemplateSourceForbidden
		case http.StatusConflict:
			return nil, template.ErrTemplateSourceNotReady
		default:
			return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceUnavailable, clusterGatewayStatusError(resp.StatusCode, body))
		}
	}
	source, apiErr, err := spec.DecodeResponse[template.SandboxTemplateSource](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode template source response: %w", err)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("%w: %s", template.ErrTemplateSourceUnavailable, apiErr.Message)
	}
	return source, nil
}

// ListSandboxes lists sandboxes from cluster-gateway with the given query parameters
func (c *ClusterGatewayClient) ListSandboxes(ctx context.Context, baseURL, teamID, userID, query string, permissions []string) (*apispec.SuccessSandboxListResponse, error) {
	// Preserve the caller team/user context so cluster-gateway and manager
	// can apply the same team-scoped authorization as other sandbox routes.
	token, err := c.internalAuthGen.Generate("cluster-gateway", teamID, userID, internalauth.GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Build request URL with query parameters
	url := fmt.Sprintf("%s/api/v1/sandboxes?%s", baseURL, query)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalauth.TeamIDHeader, teamID)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, clusterGatewayStatusError(resp.StatusCode, body)
	}

	var result apispec.SuccessSandboxListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if !bool(result.Success) {
		return nil, fmt.Errorf("cluster-gateway returned unsuccessful sandbox list response")
	}
	if result.Data == nil {
		return nil, fmt.Errorf("cluster-gateway sandbox list response missing data")
	}

	return &result, nil
}

func clusterGatewayStatusError(statusCode int, body []byte) error {
	if message, ok := spec.DecodeErrorMessage(body); ok {
		return fmt.Errorf("cluster-gateway error: %s", message)
	}
	return fmt.Errorf("unexpected status code %d: %s", statusCode, string(body))
}
