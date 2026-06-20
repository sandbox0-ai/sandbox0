package service

import (
	"context"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

const defaultCtldClientTimeout = ctldapi.DefaultRequestTimeout

// CtldClientConfig holds configuration for the node-local ctld client.
type CtldClientConfig struct {
	Timeout time.Duration
}

// CtldClient is an HTTP client for node-local ctld APIs.
type CtldClient struct {
	httpClient *http.Client
	api        *ctldapi.Client
}

// NewCtldClient creates a new ctld client.
func NewCtldClient(config CtldClientConfig) *CtldClient {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = defaultCtldClientTimeout
	}
	httpClient := &http.Client{Timeout: timeout}
	return &CtldClient{httpClient: httpClient, api: ctldapi.NewClient(httpClient)}
}

// NewCtldClientWithHTTPClient creates a ctld client with a custom HTTP client.
func NewCtldClientWithHTTPClient(httpClient *http.Client) *CtldClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultCtldClientTimeout}
	}
	return &CtldClient{httpClient: httpClient, api: ctldapi.NewClient(httpClient)}
}

func (c *CtldClient) Probe(ctx context.Context, ctldAddress, sandboxID string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	return c.apiOrDefault().Probe(ctx, ctldAddress, sandboxID, kind)
}

func (c *CtldClient) ProbePod(ctx context.Context, ctldAddress, namespace, podName string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	return c.apiOrDefault().ProbePod(ctx, ctldAddress, namespace, podName, kind)
}

func (c *CtldClient) BindVolumePortal(ctx context.Context, ctldAddress string, req ctldapi.BindVolumePortalRequest) (*ctldapi.BindVolumePortalResponse, error) {
	return c.apiOrDefault().BindVolumePortal(ctx, ctldAddress, req)
}

func (c *CtldClient) UnbindVolumePortal(ctx context.Context, ctldAddress string, req ctldapi.UnbindVolumePortalRequest) (*ctldapi.UnbindVolumePortalResponse, error) {
	return c.apiOrDefault().UnbindVolumePortal(ctx, ctldAddress, req)
}

func (c *CtldClient) CheckVolumePortals(ctx context.Context, ctldAddress string, req ctldapi.CheckVolumePortalsRequest) (*ctldapi.CheckVolumePortalsResponse, error) {
	return c.apiOrDefault().CheckVolumePortals(ctx, ctldAddress, req)
}

func (c *CtldClient) InspectRootFS(ctx context.Context, ctldAddress string, req ctldapi.InspectRootFSRequest) (*ctldapi.InspectRootFSResponse, error) {
	return c.apiOrDefault().InspectRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) SaveRootFS(ctx context.Context, ctldAddress string, req ctldapi.SaveRootFSRequest) (*ctldapi.SaveRootFSResponse, error) {
	return c.apiOrDefault().SaveRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) SaveRootFSWithTimeout(ctx context.Context, ctldAddress string, req ctldapi.SaveRootFSRequest, timeout time.Duration) (*ctldapi.SaveRootFSResponse, error) {
	return c.apiWithTimeout(timeout).SaveRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) ApplyRootFS(ctx context.Context, ctldAddress string, req ctldapi.ApplyRootFSRequest) (*ctldapi.ApplyRootFSResponse, error) {
	return c.apiOrDefault().ApplyRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) ApplyRootFSWithTimeout(ctx context.Context, ctldAddress string, req ctldapi.ApplyRootFSRequest, timeout time.Duration) (*ctldapi.ApplyRootFSResponse, error) {
	return c.apiWithTimeout(timeout).ApplyRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) apiOrDefault() *ctldapi.Client {
	if c != nil && c.api != nil {
		return c.api
	}
	if c != nil && c.httpClient != nil {
		return ctldapi.NewClient(c.httpClient)
	}
	return ctldapi.NewClient(nil)
}

func (c *CtldClient) apiWithTimeout(timeout time.Duration) *ctldapi.Client {
	if timeout <= 0 {
		return c.apiOrDefault()
	}
	if c != nil && c.httpClient != nil {
		clone := *c.httpClient
		clone.Timeout = timeout
		return ctldapi.NewClient(&clone)
	}
	return ctldapi.NewClientWithTimeout(timeout)
}
