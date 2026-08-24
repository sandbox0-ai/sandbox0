package runtimeslot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
)

const (
	DefaultNodeControlTimeout = 10 * time.Second
	maxNodeControlBytes       = 1 << 20
)

// NodeClientConfig constrains root-owned driver control calls to one local
// socket directory. It never honors ambient proxy configuration.
type NodeClientConfig struct {
	AllowedSocketRoot string
	Timeout           time.Duration
}

// NodeClient calls one task driver's root-only Unix control socket.
type NodeClient struct {
	allowedSocketRoot string
	timeout           time.Duration
	expectedSocketUID uint32
}

func NewNodeClient(config NodeClientConfig) (*NodeClient, error) {
	return newNodeClient(config, 0)
}

func newNodeClient(config NodeClientConfig, expectedSocketUID uint32) (*NodeClient, error) {
	root := filepath.Clean(strings.TrimSpace(config.AllowedSocketRoot))
	if !filepath.IsAbs(root) || root == string(filepath.Separator) {
		return nil, fmt.Errorf("allowed node control socket root must be a non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve allowed node control socket root: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("allowed node control socket root must be an existing directory: %w", errdefs.ErrInvalidArgument)
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultNodeControlTimeout
	}
	return &NodeClient{
		allowedSocketRoot: resolved, timeout: config.Timeout, expectedSocketUID: expectedSocketUID,
	}, nil
}

// Claim delivers the one-shot writer grant and waits until runsc has started.
func (c *NodeClient) Claim(ctx context.Context, endpoint string, request NodeClaimControlRequest) (NodeControlResponse, error) {
	if err := request.ValidateRegional(); err != nil {
		return NodeControlResponse{}, fmt.Errorf("validate regional node claim: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return c.exchange(ctx, endpoint, NodeClaimControlPath, request)
}

// CommandReady submits the exact authenticated procd command proof.
func (c *NodeClient) CommandReady(ctx context.Context, endpoint string, request CommandReadyControlRequest) (NodeControlResponse, error) {
	if err := request.Proof.Validate(); err != nil {
		return NodeControlResponse{}, fmt.Errorf("validate node command readiness: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return c.exchange(ctx, endpoint, NodeCommandReadyControlPath, request)
}

func (c *NodeClient) exchange(ctx context.Context, endpoint, path string, body any) (NodeControlResponse, error) {
	if c == nil {
		return NodeControlResponse{}, fmt.Errorf("node control client is not initialized: %w", errdefs.ErrUnavailable)
	}
	socketPath, err := c.socketPath(endpoint)
	if err != nil {
		return NodeControlResponse{}, err
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return NodeControlResponse{}, fmt.Errorf("encode node control request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if len(payload) > maxNodeControlBytes {
		return NodeControlResponse{}, fmt.Errorf("node control request exceeds 1 MiB: %w", errdefs.ErrInvalidArgument)
	}

	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return dialSecureNodeSocket(dialCtx, socketPath, c.expectedSocketUID)
		},
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   c.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("node control redirects are forbidden")
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://unix"+path, bytes.NewReader(payload))
	if err != nil {
		return NodeControlResponse{}, fmt.Errorf("create node control request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return NodeControlResponse{}, err
		}
		return NodeControlResponse{}, fmt.Errorf("call node control socket: %w: %w", err, errdefs.ErrUnavailable)
	}
	defer response.Body.Close()
	responsePayload, err := io.ReadAll(io.LimitReader(response.Body, maxNodeControlBytes+1))
	if err != nil || len(responsePayload) > maxNodeControlBytes {
		return NodeControlResponse{}, fmt.Errorf("read node control response: response exceeds 1 MiB: %w", errdefs.ErrUnavailable)
	}
	if response.StatusCode != http.StatusOK {
		return NodeControlResponse{}, nodeControlResponseError(response.StatusCode, responsePayload)
	}
	var result NodeControlResponse
	decoder := json.NewDecoder(bytes.NewReader(responsePayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return NodeControlResponse{}, fmt.Errorf("decode node control response: %w: %w", err, errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return NodeControlResponse{}, fmt.Errorf("node control response contains trailing data: %w", errdefs.ErrUnavailable)
	}
	if err := result.Validate(); err != nil {
		return NodeControlResponse{}, fmt.Errorf("validate node control response: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result, nil
}

func (c *NodeClient) socketPath(endpoint string) (string, error) {
	if err := validateControlEndpoint(endpoint); err != nil {
		return "", fmt.Errorf("validate node control endpoint: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "unix" || parsed.Host != "" || parsed.Opaque != "" || parsed.RawPath != "" {
		return "", fmt.Errorf("node control endpoint must be a canonical local Unix URL: %w", errdefs.ErrInvalidArgument)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(parsed.Path))
	if err != nil {
		return "", fmt.Errorf("resolve node control socket: %w: %w", err, errdefs.ErrUnavailable)
	}
	relative, err := filepath.Rel(c.allowedSocketRoot, resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("node control socket is outside its allowed root: %w", errdefs.ErrPermissionDenied)
	}
	if err := validateSecureNodeSocket(resolved, c.expectedSocketUID); err != nil {
		return "", err
	}
	return resolved, nil
}

func nodeControlResponseError(status int, payload []byte) error {
	var response struct {
		Error string `json:"error"`
	}
	message := http.StatusText(status)
	if len(payload) > 0 && json.Unmarshal(payload, &response) == nil && strings.TrimSpace(response.Error) != "" {
		message = strings.TrimSpace(response.Error)
	}
	base := fmt.Sprintf("node control returned %d: %s", status, message)
	switch status {
	case http.StatusBadRequest, http.StatusMethodNotAllowed:
		return fmt.Errorf("%s: %w", base, errdefs.ErrInvalidArgument)
	case http.StatusForbidden:
		return fmt.Errorf("%s: %w", base, errdefs.ErrPermissionDenied)
	case http.StatusNotFound:
		return fmt.Errorf("%s: %w", base, errdefs.ErrNotFound)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return fmt.Errorf("%s: %w", base, errdefs.ErrFailedPrecondition)
	default:
		return fmt.Errorf("%s: %w", base, errdefs.ErrUnavailable)
	}
}
