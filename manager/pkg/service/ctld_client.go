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

func (c *CtldClient) Probe(ctx context.Context, ctldAddress, sandboxID string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	return doCtldRequest[sandboxprobe.Response](ctx, c.httpClient, ctldAddress, sandboxID, "/probes/"+string(kind))
}

func (c *CtldClient) ProbePod(ctx context.Context, ctldAddress, namespace, podName string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	path := fmt.Sprintf("/api/v1/pods/%s/%s/probes/%s", url.PathEscape(namespace), url.PathEscape(podName), url.PathEscape(string(kind)))
	return doCtldPathRequest[sandboxprobe.Response](ctx, c.httpClient, ctldAddress, path)
}

func (c *CtldClient) BindVolumePortal(ctx context.Context, ctldAddress string, req ctldapi.BindVolumePortalRequest) (*ctldapi.BindVolumePortalResponse, error) {
	return doCtldJSONRequest[ctldapi.BindVolumePortalResponse](ctx, c.httpClient, ctldAddress, "/api/v1/volume-portals/bind", req)
}

func (c *CtldClient) UnbindVolumePortal(ctx context.Context, ctldAddress string, req ctldapi.UnbindVolumePortalRequest) (*ctldapi.UnbindVolumePortalResponse, error) {
	return doCtldJSONRequest[ctldapi.UnbindVolumePortalResponse](ctx, c.httpClient, ctldAddress, "/api/v1/volume-portals/unbind", req)
}

func (c *CtldClient) PrepareVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.PrepareVolumePortalHandoffRequest) (*ctldapi.PrepareVolumePortalHandoffResponse, error) {
	return doCtldJSONRequest[ctldapi.PrepareVolumePortalHandoffResponse](ctx, c.httpClient, ctldAddress, "/api/v1/volume-portals/handoffs/prepare", req)
}

func (c *CtldClient) CompleteVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.CompleteVolumePortalHandoffRequest) (*ctldapi.CompleteVolumePortalHandoffResponse, error) {
	return doCtldJSONRequest[ctldapi.CompleteVolumePortalHandoffResponse](ctx, c.httpClient, ctldAddress, "/api/v1/volume-portals/handoffs/complete", req)
}

func (c *CtldClient) AbortVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.AbortVolumePortalHandoffRequest) (*ctldapi.AbortVolumePortalHandoffResponse, error) {
	return doCtldJSONRequest[ctldapi.AbortVolumePortalHandoffResponse](ctx, c.httpClient, ctldAddress, "/api/v1/volume-portals/handoffs/abort", req)
}

func doCtldRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, sandboxID, suffix string) (*T, error) {
	path := fmt.Sprintf("/api/v1/sandboxes/%s%s", url.PathEscape(sandboxID), suffix)
	return doCtldPathRequest[T](ctx, httpClient, ctldAddress, path)
}

func doCtldPathRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, path string) (*T, error) {
	return doCtldJSONRequest[T](ctx, httpClient, ctldAddress, path, nil)
}

func doCtldJSONRequest[T any](ctx context.Context, httpClient *http.Client, ctldAddress, path string, requestBody any) (*T, error) {
	base := strings.TrimRight(ctldAddress, "/")
	requestURL := base + path

	var reader io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		reader = strings.NewReader(string(payload))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, reader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result T
	if len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return &result, fmt.Errorf("ctld request failed with status %d", resp.StatusCode)
	}
	return &result, nil
}
