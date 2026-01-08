package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/db"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// ManagerClient provides methods to call manager APIs
type ManagerClient struct {
	baseURL         string
	internalAuthGen *internalauth.Generator
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewManagerClient creates a new manager client
func NewManagerClient(baseURL string, internalAuthGen *internalauth.Generator, logger *zap.Logger) *ManagerClient {
	return &ManagerClient{
		baseURL:         baseURL,
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetSandbox retrieves sandbox information from manager
func (c *ManagerClient) GetSandbox(ctx context.Context, sandboxID, teamID string) (*db.Sandbox, error) {
	// Generate internal token for manager
	token, err := c.internalAuthGen.Generate("manager", teamID, "", internalauth.GenerateOptions{})
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	// Build request URL
	url := fmt.Sprintf("%s/api/v1/sandboxes/%s", c.baseURL, sandboxID)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set("X-Internal-Token", token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("sandbox not found: %s", sandboxID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var sandbox db.Sandbox
	if err := json.NewDecoder(resp.Body).Decode(&sandbox); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	c.logger.Debug("Retrieved sandbox from manager",
		zap.String("sandbox_id", sandboxID),
		zap.String("team_id", sandbox.TeamID),
		zap.String("procd_address", sandbox.ProcdAddress),
	)

	return &sandbox, nil
}
