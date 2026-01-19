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
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// InternalGatewayClient provides methods to call internal-gateway APIs
type InternalGatewayClient struct {
	internalAuthGen *internalauth.Generator
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewInternalGatewayClient creates a new internal-gateway client
func NewInternalGatewayClient(internalAuthGen *internalauth.Generator, timeout time.Duration, logger *zap.Logger) *InternalGatewayClient {
	return &InternalGatewayClient{
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient: &http.Client{
			Timeout: timeout,
		},
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
	Namespace   string `json:"namespace"`
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
	// Generate internal token for internal-gateway
	token, err := c.internalAuthGen.Generate("internal-gateway", "scheduler", "scheduler", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
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
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var summary ClusterSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &summary, nil
}

// GetTemplateStats gets template statistics from internal-gateway
func (c *InternalGatewayClient) GetTemplateStats(ctx context.Context, baseURL string) (*TemplateStats, error) {
	// Generate internal token for internal-gateway
	token, err := c.internalAuthGen.Generate("internal-gateway", "scheduler", "scheduler", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
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
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var stats TemplateStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &stats, nil
}

// CreateOrUpdateTemplate creates or updates a template in a cluster via internal-gateway
func (c *InternalGatewayClient) CreateOrUpdateTemplate(ctx context.Context, baseURL string, templateID, namespace string, spec v1alpha1.SandboxTemplateSpec) error {
	// Generate internal token for internal-gateway
	token, err := c.internalAuthGen.Generate("internal-gateway", "scheduler", "scheduler", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}

	// First, try to get the template to determine if it exists
	getURL := fmt.Sprintf("%s/api/v1/templates/%s?namespace=%s", baseURL, templateID, namespace)
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
	body := map[string]interface{}{
		"name":      templateID,
		"namespace": namespace,
		"spec":      spec,
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	var method string
	var url string
	if templateExists {
		// Update existing template
		method = http.MethodPut
		url = fmt.Sprintf("%s/api/v1/templates/%s", baseURL, templateID)
	} else {
		// Create new template
		method = http.MethodPost
		url = fmt.Sprintf("%s/api/v1/templates", baseURL)
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
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(respBody))
	}

	c.logger.Debug("Template synced to cluster",
		zap.String("template_id", templateID),
		zap.String("namespace", namespace),
		zap.String("base_url", baseURL),
		zap.Bool("created", !templateExists),
	)

	return nil
}

// DeleteTemplate deletes a template from a cluster via internal-gateway
func (c *InternalGatewayClient) DeleteTemplate(ctx context.Context, baseURL string, templateID, namespace string) error {
	// Generate internal token for internal-gateway
	token, err := c.internalAuthGen.Generate("internal-gateway", "scheduler", "scheduler", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}

	// Build request URL
	url := fmt.Sprintf("%s/api/v1/templates/%s?namespace=%s", baseURL, templateID, namespace)

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
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	c.logger.Debug("Template deleted from cluster",
		zap.String("template_id", templateID),
		zap.String("namespace", namespace),
		zap.String("base_url", baseURL),
	)

	return nil
}
