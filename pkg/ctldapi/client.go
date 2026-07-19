package ctldapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

// RootFSTokenProvider signs one short-lived, team-scoped token for ctld's
// manager-only rootfs mutation surface.
type RootFSTokenProvider func(ctx context.Context, teamID, sandboxID string) (string, error)

// DefaultRequestTimeout is the default timeout for node-local ctld HTTP calls.
const DefaultRequestTimeout = 15 * time.Second

const (
	pathVolumePortalBind         = "/api/v1/volume-portals/bind"
	pathVolumePortalUnbind       = "/api/v1/volume-portals/unbind"
	pathVolumePortalCheck        = "/api/v1/volume-portals/check"
	pathVolumePortalOwnerAttach  = "/api/v1/volume-portals/owners/attach"
	pathVolumePortalOwnerRelease = "/api/v1/volume-portals/owners/release"
	pathVolumeSnapshotPrepare    = "/api/v1/volume-portals/snapshot-checkpoints/prepare"
	pathVolumeSnapshotComplete   = "/api/v1/volume-portals/snapshot-checkpoints/complete"
	pathVolumeSnapshotAbort      = "/api/v1/volume-portals/snapshot-checkpoints/abort"
	pathRootFSInspect            = "/api/v1/rootfs/inspect"
	pathRootFSSnapshotPrepare    = "/api/v1/rootfs/snapshots/prepare"
	pathRootFSSnapshotPublish    = "/api/v1/rootfs/snapshots/publish"
	pathRootFSSnapshotAbort      = "/api/v1/rootfs/snapshots/abort"
	pathRootFSApply              = "/api/v1/rootfs/apply"
)

var defaultHTTPClient = &http.Client{Timeout: DefaultRequestTimeout}

// Client calls node-local ctld APIs.
type Client struct {
	httpClient          *http.Client
	rootFSTokenProvider RootFSTokenProvider
}

// NewClient returns a ctld API client using httpClient or the package default.
func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = defaultHTTPClient
	}
	return &Client{httpClient: httpClient}
}

// NewClientWithRootFSAuth returns a client that authenticates every mutating
// rootfs request with a freshly generated manager token.
func NewClientWithRootFSAuth(httpClient *http.Client, provider RootFSTokenProvider) *Client {
	client := NewClient(httpClient)
	client.rootFSTokenProvider = provider
	return client
}

// NewClientWithTimeout returns a ctld API client with a dedicated HTTP timeout.
func NewClientWithTimeout(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	return NewClient(&http.Client{Timeout: timeout})
}

// RequestError describes a non-2xx response from ctld.
type RequestError struct {
	StatusCode int
	Message    string
}

func (e *RequestError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Message) != "" {
		return fmt.Sprintf("ctld request failed with status %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("ctld request failed with status %d", e.StatusCode)
}

// IsConflictError reports whether err is a ctld HTTP 409 response.
func IsConflictError(err error) bool {
	var reqErr *RequestError
	return errors.As(err, &reqErr) && reqErr != nil && reqErr.StatusCode == http.StatusConflict
}

func (c *Client) Probe(ctx context.Context, ctldAddress, sandboxID string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	path := fmt.Sprintf("/api/v1/sandboxes/%s/probes/%s", url.PathEscape(sandboxID), url.PathEscape(string(kind)))
	return PostJSON[sandboxprobe.Response](ctx, c.httpClientOrDefault(), ctldAddress, path, nil)
}

func (c *Client) ProbePod(ctx context.Context, ctldAddress, namespace, podName string, kind sandboxprobe.Kind) (*sandboxprobe.Response, error) {
	path := fmt.Sprintf("/api/v1/pods/%s/%s/probes/%s", url.PathEscape(namespace), url.PathEscape(podName), url.PathEscape(string(kind)))
	return PostJSON[sandboxprobe.Response](ctx, c.httpClientOrDefault(), ctldAddress, path, nil)
}

func (c *Client) BindVolumePortal(ctx context.Context, ctldAddress string, req BindVolumePortalRequest) (*BindVolumePortalResponse, error) {
	return PostJSON[BindVolumePortalResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumePortalBind, req)
}

func (c *Client) UnbindVolumePortal(ctx context.Context, ctldAddress string, req UnbindVolumePortalRequest) (*UnbindVolumePortalResponse, error) {
	return PostJSON[UnbindVolumePortalResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumePortalUnbind, req)
}

func (c *Client) CheckVolumePortals(ctx context.Context, ctldAddress string, req CheckVolumePortalsRequest) (*CheckVolumePortalsResponse, error) {
	return PostJSON[CheckVolumePortalsResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumePortalCheck, req)
}

func (c *Client) AttachVolumeOwner(ctx context.Context, ctldAddress string, req AttachVolumeOwnerRequest) (*AttachVolumeOwnerResponse, error) {
	return PostJSON[AttachVolumeOwnerResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumePortalOwnerAttach, req)
}

func (c *Client) ReleaseVolumeOwner(ctx context.Context, ctldAddress string, req ReleaseVolumeOwnerRequest) (*ReleaseVolumeOwnerResponse, error) {
	return PostJSON[ReleaseVolumeOwnerResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumePortalOwnerRelease, req)
}

func (c *Client) PrepareVolumeSnapshotCheckpoint(ctx context.Context, ctldAddress string, req PrepareVolumeSnapshotCheckpointRequest) (*PrepareVolumeSnapshotCheckpointResponse, error) {
	return PostJSON[PrepareVolumeSnapshotCheckpointResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumeSnapshotPrepare, req)
}

func (c *Client) CompleteVolumeSnapshotCheckpoint(ctx context.Context, ctldAddress string, req CompleteVolumeSnapshotCheckpointRequest) (*CompleteVolumeSnapshotCheckpointResponse, error) {
	return PostJSON[CompleteVolumeSnapshotCheckpointResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumeSnapshotComplete, req)
}

func (c *Client) AbortVolumeSnapshotCheckpoint(ctx context.Context, ctldAddress string, req AbortVolumeSnapshotCheckpointRequest) (*AbortVolumeSnapshotCheckpointResponse, error) {
	return PostJSON[AbortVolumeSnapshotCheckpointResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathVolumeSnapshotAbort, req)
}

func (c *Client) InspectRootFS(ctx context.Context, ctldAddress string, req InspectRootFSRequest) (*InspectRootFSResponse, error) {
	return PostJSON[InspectRootFSResponse](ctx, c.httpClientOrDefault(), ctldAddress, pathRootFSInspect, req)
}

func (c *Client) PrepareRootFSSnapshot(ctx context.Context, ctldAddress string, req PrepareRootFSSnapshotRequest) (*PrepareRootFSSnapshotResponse, error) {
	return postRootFSJSON[PrepareRootFSSnapshotResponse](ctx, c, ctldAddress, pathRootFSSnapshotPrepare, req.TeamID, req.SandboxID, req)
}

func (c *Client) PublishRootFSSnapshot(ctx context.Context, ctldAddress string, req PublishRootFSSnapshotRequest) (*PublishRootFSSnapshotResponse, error) {
	return postRootFSJSON[PublishRootFSSnapshotResponse](ctx, c, ctldAddress, pathRootFSSnapshotPublish, req.TeamID, req.SandboxID, req)
}

func (c *Client) AbortRootFSSnapshot(ctx context.Context, ctldAddress string, req AbortRootFSSnapshotRequest) (*AbortRootFSSnapshotResponse, error) {
	return postRootFSJSON[AbortRootFSSnapshotResponse](ctx, c, ctldAddress, pathRootFSSnapshotAbort, req.TeamID, req.SandboxID, req)
}

func (c *Client) ApplyRootFS(ctx context.Context, ctldAddress string, req ApplyRootFSRequest) (*ApplyRootFSResponse, error) {
	return postRootFSJSON[ApplyRootFSResponse](ctx, c, ctldAddress, pathRootFSApply, req.TeamID, req.SandboxID, req)
}

func postRootFSJSON[T any](
	ctx context.Context,
	client *Client,
	baseURL, path, teamID, sandboxID string,
	request any,
) (*T, error) {
	var token string
	if client != nil && client.rootFSTokenProvider != nil {
		var err error
		token, err = client.rootFSTokenProvider(ctx, strings.TrimSpace(teamID), strings.TrimSpace(sandboxID))
		if err != nil {
			return nil, fmt.Errorf("generate ctld rootfs internal token: %w", err)
		}
		if strings.TrimSpace(token) == "" {
			return nil, fmt.Errorf("generate ctld rootfs internal token: empty token")
		}
	}
	return postJSONWithToken[T](ctx, client.httpClientOrDefault(), baseURL, path, request, token)
}

func (c *Client) httpClientOrDefault() *http.Client {
	if c != nil && c.httpClient != nil {
		return c.httpClient
	}
	return defaultHTTPClient
}

// PostJSON sends a JSON POST request to ctld and decodes the response.
func PostJSON[T any](ctx context.Context, httpClient *http.Client, baseURL, path string, request any) (*T, error) {
	return postJSONWithToken[T](ctx, httpClient, baseURL, path, request, "")
}

func postJSONWithToken[T any](ctx context.Context, httpClient *http.Client, baseURL, path string, request any, token string) (*T, error) {
	if httpClient == nil {
		httpClient = defaultHTTPClient
	}

	var reader io.Reader
	if request != nil {
		payload, err := json.Marshal(request)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, reader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(token) != "" {
		req.Header.Set("X-Internal-Token", token)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var out T
	if len(body) > 0 {
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return &out, &RequestError{StatusCode: resp.StatusCode, Message: responseError(&out)}
	}
	return &out, nil
}

func responseError(resp any) string {
	switch typed := resp.(type) {
	case *PauseResponse:
		return strings.TrimSpace(typed.Error)
	case *ResumeResponse:
		return strings.TrimSpace(typed.Error)
	case *AttachVolumeOwnerResponse:
		return strings.TrimSpace(typed.Error)
	case *ReleaseVolumeOwnerResponse:
		return strings.TrimSpace(typed.Error)
	case *CheckVolumePortalsResponse:
		return strings.TrimSpace(typed.Error)
	case *PrepareVolumeSnapshotCheckpointResponse:
		return strings.TrimSpace(typed.Error)
	case *CompleteVolumeSnapshotCheckpointResponse:
		return strings.TrimSpace(typed.Error)
	case *AbortVolumeSnapshotCheckpointResponse:
		return strings.TrimSpace(typed.Error)
	case *BindVolumePortalResponse:
		return strings.TrimSpace(typed.Error)
	case *UnbindVolumePortalResponse:
		return strings.TrimSpace(typed.Error)
	case *InspectRootFSResponse:
		return strings.TrimSpace(typed.Error)
	case *PrepareRootFSSnapshotResponse:
		return strings.TrimSpace(typed.Error)
	case *PublishRootFSSnapshotResponse:
		return strings.TrimSpace(typed.Error)
	case *AbortRootFSSnapshotResponse:
		return strings.TrimSpace(typed.Error)
	case *ApplyRootFSResponse:
		return strings.TrimSpace(typed.Error)
	default:
		return ""
	}
}
