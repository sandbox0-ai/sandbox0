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

func (c *CtldClient) PrepareVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.PrepareVolumePortalHandoffRequest) (*ctldapi.PrepareVolumePortalHandoffResponse, error) {
	return c.apiOrDefault().PrepareVolumePortalHandoff(ctx, ctldAddress, req)
}

func (c *CtldClient) CompleteVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.CompleteVolumePortalHandoffRequest) (*ctldapi.CompleteVolumePortalHandoffResponse, error) {
	return c.apiOrDefault().CompleteVolumePortalHandoff(ctx, ctldAddress, req)
}

func (c *CtldClient) AbortVolumePortalHandoff(ctx context.Context, ctldAddress string, req ctldapi.AbortVolumePortalHandoffRequest) (*ctldapi.AbortVolumePortalHandoffResponse, error) {
	return c.apiOrDefault().AbortVolumePortalHandoff(ctx, ctldAddress, req)
}

func (c *CtldClient) PrepareRootFS(ctx context.Context, ctldAddress string, req ctldapi.PrepareRootFSRequest) (*ctldapi.PrepareRootFSResponse, error) {
	return c.apiOrDefault().PrepareRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) CheckpointRootFS(ctx context.Context, ctldAddress string, req ctldapi.CheckpointRootFSRequest) (*ctldapi.CheckpointRootFSResponse, error) {
	return c.apiOrDefault().CheckpointRootFS(ctx, ctldAddress, req)
}

func (c *CtldClient) ReleaseRootFS(ctx context.Context, ctldAddress string, req ctldapi.ReleaseRootFSRequest) (*ctldapi.ReleaseRootFSResponse, error) {
	return c.apiOrDefault().ReleaseRootFS(ctx, ctldAddress, req)
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
