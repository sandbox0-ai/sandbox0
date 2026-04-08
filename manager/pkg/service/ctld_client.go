package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

// CtldClientConfig holds configuration for the node-local ctld client.
type CtldClientConfig struct {
	Timeout time.Duration
}

// CtldClient is an HTTP client for node-local ctld APIs.
type CtldClient struct {
	httpClient *http.Client
}

// NewCtldClient creates a new ctld client.
func NewCtldClient(config CtldClientConfig) *CtldClient {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &CtldClient{httpClient: &http.Client{Timeout: timeout}}
}

// NewCtldClientWithHTTPClient creates a ctld client with a custom HTTP client.
func NewCtldClientWithHTTPClient(httpClient *http.Client) *CtldClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &CtldClient{httpClient: httpClient}
}

func (c *CtldClient) Pause(ctx context.Context, ctldAddress, sandboxID string) (*ctldapi.PauseResponse, error) {
	return doCtldRequest[ctldapi.PauseResponse](ctx, c.httpClient, ctldAddress, sandboxID, "/pause")
}

func (c *CtldClient) Resume(ctx context.Context, ctldAddress, sandboxID string) (*ctldapi.ResumeResponse, error) {
	return doCtldRequest[ctldapi.ResumeResponse](ctx, c.httpClient, ctldAddress, sandboxID, "/resume")
}

func doCtldRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, sandboxID, suffix string) (*T, error) {
	base := strings.TrimRight(ctldAddress, "/")
	url := fmt.Sprintf("%s/api/v1/sandboxes/%s%s", base, sandboxID, suffix)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result T
	if len(body) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return &result, fmt.Errorf("ctld request failed with status %d", resp.StatusCode)
	}
	return &result, nil
}
