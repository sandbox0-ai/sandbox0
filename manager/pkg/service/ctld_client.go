package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

const defaultCtldClientTimeout = 15 * time.Second

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
		timeout = defaultCtldClientTimeout
	}
	return &CtldClient{httpClient: &http.Client{Timeout: timeout}}
}

// NewCtldClientWithHTTPClient creates a ctld client with a custom HTTP client.
func NewCtldClientWithHTTPClient(httpClient *http.Client) *CtldClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultCtldClientTimeout}
	}
	return &CtldClient{httpClient: httpClient}
}

func (c *CtldClient) Pause(ctx context.Context, ctldAddress, sandboxID string) (*ctldapi.PauseResponse, error) {
	return doCtldRequest[ctldapi.PauseResponse](ctx, c.httpClient, ctldAddress, sandboxID, "/pause")
}

func (c *CtldClient) Resume(ctx context.Context, ctldAddress, sandboxID string) (*ctldapi.ResumeResponse, error) {
	return doCtldRequest[ctldapi.ResumeResponse](ctx, c.httpClient, ctldAddress, sandboxID, "/resume")
}

func (c *CtldClient) Probe(ctx context.Context, ctldAddress, sandboxID string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	return doCtldRequest[sandboxprobe.Response](ctx, c.httpClient, ctldAddress, sandboxID, "/probes/"+string(kind))
}

func (c *CtldClient) ProbePod(ctx context.Context, ctldAddress, namespace, podName string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	path := fmt.Sprintf("/api/v1/pods/%s/%s/probes/%s", url.PathEscape(namespace), url.PathEscape(podName), url.PathEscape(string(kind)))
	return doCtldPathRequest[sandboxprobe.Response](ctx, c.httpClient, ctldAddress, path)
}

func doCtldRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, sandboxID, suffix string) (*T, error) {
	path := fmt.Sprintf("/api/v1/sandboxes/%s%s", url.PathEscape(sandboxID), suffix)
	return doCtldPathRequest[T](ctx, httpClient, ctldAddress, path)
}

func doCtldPathRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, path string) (*T, error) {
	base := strings.TrimRight(ctldAddress, "/")
	url := base + path

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
