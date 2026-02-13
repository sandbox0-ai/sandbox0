package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/observability"
	httpobs "github.com/sandbox0-ai/infra/pkg/observability/http"
	"go.uber.org/zap"
)

// InternalGatewayClient provides methods to call internal-gateway APIs
type InternalGatewayClient struct {
	internalAuthGen *internalauth.Generator
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewInternalGatewayClient creates a new internal-gateway client
func NewInternalGatewayClient(internalAuthGen *internalauth.Generator, logger *zap.Logger, obsProvider *observability.Provider) *InternalGatewayClient {
	httpClient := obsProvider.HTTP.NewClient(httpobs.Config{
		Timeout: 30 * time.Second,
	})

	return &InternalGatewayClient{
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient:      httpClient,
	}
}

// ClusterSummary represents the cluster capacity and status
type ClusterSummary struct {
	ClusterID      string `json:"cluster_id"`
	NodeCount      int    `json:"node_count"`
	IdlePodCount   int32  `json:"idle_pod_count"`
	ActivePodCount int32  `json:"active_pod_count"`
	TotalPodCount  int32  `json:"total_pod_count"`
}

// TemplateStat represents statistics for a single template
type TemplateStat struct {
	TemplateID  string `json:"template_id"`
	IdleCount   int32  `json:"idle_count"`
	ActiveCount int32  `json:"active_count"`
	MinIdle     int32  `json:"min_idle"`
	MaxIdle     int32  `json:"max_idle"`
}

// TemplateStats represents statistics for all templates in a cluster
type TemplateStats struct {
	Templates []TemplateStat `json:"templates"`
}

// GetClusterSummary gets cluster summary from internal-gateway
func (c *InternalGatewayClient) GetClusterSummary(ctx context.Context, baseURL string) (*ClusterSummary, error) {
	// Generate system token for internal-gateway
	token, err := c.internalAuthGen.GenerateSystem("internal-gateway", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return nil, fmt.Errorf("generate system token: %w", err)
	}

	// Build request URL
	url := fmt.Sprintf("%s/internal/v1/cluster/summary", baseURL)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_, apiErr, err := spec.DecodeResponse[map[string]any](bytes.NewReader(body))
		if err == nil && apiErr != nil {
			return nil, fmt.Errorf("internal-gateway error: %s", apiErr.Message)
		}
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	summary, apiErr, err := spec.DecodeResponse[ClusterSummary](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("internal-gateway error: %s", apiErr.Message)
	}

	return summary, nil
}

// GetTemplateStats gets template statistics from internal-gateway
func (c *InternalGatewayClient) GetTemplateStats(ctx context.Context, baseURL string) (*TemplateStats, error) {
	// Generate system token for internal-gateway
	token, err := c.internalAuthGen.GenerateSystem("internal-gateway", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return nil, fmt.Errorf("generate system token: %w", err)
	}

	// Build request URL
	url := fmt.Sprintf("%s/internal/v1/templates/stats", baseURL)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_, apiErr, err := spec.DecodeResponse[map[string]any](bytes.NewReader(body))
		if err == nil && apiErr != nil {
			return nil, fmt.Errorf("internal-gateway error: %s", apiErr.Message)
		}
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	stats, apiErr, err := spec.DecodeResponse[TemplateStats](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("internal-gateway error: %s", apiErr.Message)
	}

	return stats, nil
}

// CreateOrUpdateTemplate creates or updates a template in a cluster via internal-gateway.
// The template name must be a Kubernetes DNS-1123 label.
func (c *InternalGatewayClient) CreateOrUpdateTemplate(ctx context.Context, baseURL string, template *v1alpha1.SandboxTemplate) error {
	// Generate system token for internal-gateway
	token, err := c.internalAuthGen.GenerateSystem("internal-gateway", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return fmt.Errorf("generate system token: %w", err)
	}

	// First, try to get the template to determine if it exists
	templateID := template.Name
	getURL := fmt.Sprintf("%s/internal/v1/templates/%s", baseURL, templateID)
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return fmt.Errorf("create get request: %w", err)
	}
	getReq.Header.Set(internalauth.DefaultTokenHeader, token)

	getResp, err := c.httpClient.Do(getReq)
	if err != nil {
		return fmt.Errorf("execute get request: %w", err)
	}
	getResp.Body.Close()

	templateExists := getResp.StatusCode == http.StatusOK

	// Build request body
	bodyJSON, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	var method string
	var url string
	if templateExists {
		// Update existing template
		method = http.MethodPut
		url = fmt.Sprintf("%s/internal/v1/templates/%s", baseURL, templateID)
	} else {
		// Create new template
		method = http.MethodPost
		url = fmt.Sprintf("%s/internal/v1/templates", baseURL)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		_, apiErr, err := spec.DecodeResponse[map[string]any](bytes.NewReader(respBody))
		if err == nil && apiErr != nil {
			return fmt.Errorf("internal-gateway error: %s", apiErr.Message)
		}
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(respBody))
	}

	c.logger.Debug("Template synced to cluster",
		zap.String("template_id", templateID),
		zap.String("base_url", baseURL),
		zap.Bool("created", !templateExists),
	)

	return nil
}

// DeleteTemplate deletes a template from a cluster via internal-gateway
func (c *InternalGatewayClient) DeleteTemplate(ctx context.Context, baseURL string, templateID string) error {
	// Generate system token for internal-gateway
	token, err := c.internalAuthGen.GenerateSystem("internal-gateway", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return fmt.Errorf("generate system token: %w", err)
	}

	// Build request URL
	url := fmt.Sprintf("%s/internal/v1/templates/%s", baseURL, templateID)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code (404 is OK, means already deleted)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		_, apiErr, err := spec.DecodeResponse[map[string]any](bytes.NewReader(body))
		if err == nil && apiErr != nil {
			return fmt.Errorf("internal-gateway error: %s", apiErr.Message)
		}
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	c.logger.Debug("Template deleted from cluster",
		zap.String("template_id", templateID),
		zap.String("base_url", baseURL),
	)

	return nil
}
