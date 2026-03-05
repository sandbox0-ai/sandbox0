package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// Sentinel errors for ManagerClient operations.
// Callers should use errors.Is() to check for specific error types.
var (
	// ErrSandboxNotFound indicates the requested sandbox does not exist.
	ErrSandboxNotFound = errors.New("sandbox not found")

	// ErrManagerUnavailable indicates the manager service is unreachable or returned an unexpected error.
	ErrManagerUnavailable = errors.New("manager service unavailable")
)

// ManagerClient provides methods to call manager APIs
type ManagerClient struct {
	baseURL         string
	internalAuthGen *internalauth.Generator
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewManagerClient creates a new manager client
func NewManagerClient(baseURL string, internalAuthGen *internalauth.Generator, logger *zap.Logger, timeout time.Duration) *ManagerClient {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &ManagerClient{
		baseURL:         baseURL,
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// GetSandbox retrieves sandbox information from manager
func (c *ManagerClient) GetSandbox(ctx context.Context, sandboxID, userID, teamID string) (*mgr.Sandbox, error) {
	// Generate internal token for manager
	token, err := c.internalAuthGen.Generate("manager", teamID, userID, internalauth.GenerateOptions{})
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
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", ErrSandboxNotFound, sandboxID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_, apiErr, err := spec.DecodeResponse[map[string]any](bytes.NewReader(body))
		if err == nil && apiErr != nil {
			return nil, fmt.Errorf("%w: %s", ErrManagerUnavailable, apiErr.Message)
		}
		return nil, fmt.Errorf("%w: unexpected status code %d: %s", ErrManagerUnavailable, resp.StatusCode, string(body))
	}

	// Parse response
	sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: decode response: %w", ErrManagerUnavailable, err)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("%w: %s", ErrManagerUnavailable, apiErr.Message)
	}

	c.logger.Debug("Retrieved sandbox from manager",
		zap.String("sandbox_id", sandboxID),
		zap.String("team_id", sandbox.TeamID),
	)

	return sandbox, nil
}

// GetSandboxInternal retrieves sandbox information for trusted internal routing.
func (c *ManagerClient) GetSandboxInternal(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
	token, err := c.internalAuthGen.GenerateSystem("manager", internalauth.GenerateOptions{})
	if err != nil {
		return nil, fmt.Errorf("generate system token: %w", err)
	}

	url := fmt.Sprintf("%s/internal/v1/sandboxes/%s", c.baseURL, sandboxID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", ErrSandboxNotFound, sandboxID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: unexpected status code %d: %s", ErrManagerUnavailable, resp.StatusCode, string(body))
	}

	sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: decode response: %w", ErrManagerUnavailable, err)
	}
	if apiErr != nil {
		return nil, fmt.Errorf("%w: %s", ErrManagerUnavailable, apiErr.Message)
	}
	return sandbox, nil
}

// ResumeSandbox asks manager to resume a paused sandbox.
func (c *ManagerClient) ResumeSandbox(ctx context.Context, sandboxID, userID, teamID string) error {
	token, err := c.internalAuthGen.Generate("manager", teamID, userID, internalauth.GenerateOptions{})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/sandboxes/%s/resume", c.baseURL, sandboxID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrSandboxNotFound, sandboxID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		if msg, ok := payload["message"].(string); ok && msg != "" {
			return fmt.Errorf("%w: %s", ErrManagerUnavailable, msg)
		}
		return fmt.Errorf("%w: unexpected status code %d", ErrManagerUnavailable, resp.StatusCode)
	}
	return nil
}
