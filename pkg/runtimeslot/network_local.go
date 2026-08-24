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
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const (
	RuntimeSlotNetworkRegisterPath = "/internal/v1/runtime-slot-network/register"
	RuntimeSlotNetworkPreparePath  = "/internal/v1/runtime-slot-network/prepare"
	RuntimeSlotNetworkCleanupPath  = "/internal/v1/runtime-slot-network/cleanup"
	RuntimeSlotNetworkHealthPath   = "/internal/v1/runtime-slot-network/health"

	defaultRuntimeSlotNetworkTimeout = 10 * time.Second
	maxRuntimeSlotNetworkBytes       = 1 << 20

	// RuntimeSlotWarmNetworkPolicy is the canonical policy ctld applies before
	// a region is allowed to claim or expose a warm slot.
	RuntimeSlotWarmNetworkPolicy = `{"version":"v1","sandboxId":"sandbox0-runtime-slot-warm","teamId":"sandbox0-runtime-slot-warm","mode":"block-all"}`
	RuntimeSlotWarmSandboxID     = "sandbox0-runtime-slot-warm"
	RuntimeSlotWarmTeamID        = "sandbox0-runtime-slot-warm"
)

// RuntimeSlotNetworkRegistrationRequest binds one locally journaled warm slot
// to the exact host network namespace that ctld must keep default-denied before
// regional claim authority can expose the slot as fast-path ready.
type RuntimeSlotNetworkRegistrationRequest struct {
	SlotID            string `json:"slot_id"`
	ClusterID         string `json:"cluster_id"`
	AllocationID      string `json:"allocation_id"`
	NodeID            string `json:"node_id"`
	NodeUID           string `json:"node_uid"`
	NodeBootID        string `json:"node_boot_id"`
	NetNSIdentity     string `json:"netns_identity"`
	NetNSRelativePath string `json:"netns_relative_path"`
}

// Validate rejects incomplete physical identity or a path that could escape
// ctld's configured host-netns mount.
func (r RuntimeSlotNetworkRegistrationRequest) Validate() error {
	for name, value := range map[string]string{
		"slot_id": r.SlotID, "cluster_id": r.ClusterID, "allocation_id": r.AllocationID,
		"node_id": r.NodeID, "node_uid": r.NodeUID, "node_boot_id": r.NodeBootID,
		"netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	return validateRuntimeSlotNetworkRelativePath(r.NetNSRelativePath)
}

// MatchesPrepare reports whether a region-authenticated claim addresses the
// same physical warm-slot incarnation.
func (r RuntimeSlotNetworkRegistrationRequest) MatchesPrepare(request NodeNetworkPrepareControlRequest) bool {
	return r.SlotID == request.SlotID && r.ClusterID == request.ClusterID &&
		r.AllocationID == request.AllocationID && r.NodeID == request.NodeID &&
		r.NodeUID == request.NodeUID && r.NodeBootID == request.NodeBootID &&
		r.NetNSIdentity == request.NetNSIdentity
}

// IncarnationID returns the same physical identity later bound into the
// region-visible network policy token.
func (r RuntimeSlotNetworkRegistrationRequest) IncarnationID() string {
	return runtimeSlotNetworkIncarnationID(
		r.ClusterID,
		r.SlotID,
		r.AllocationID,
		r.NodeID,
		r.NodeUID,
		r.NodeBootID,
		r.NetNSIdentity,
	)
}

// RuntimeSlotNetworkRegistrationResponse proves that the warm default-deny
// policy is present in the last successful node redirect synchronization.
type RuntimeSlotNetworkRegistrationResponse struct {
	NetworkPolicyApplied bool `json:"network_policy_applied"`
}

// RuntimeSlotNetworkPrepareRequest adds only the ctld-local namespace path to
// the region-authenticated network command. The relative path is resolved
// below ctld's configured host-netns mount and is never accepted as authority
// for the expected namespace incarnation.
type RuntimeSlotNetworkPrepareRequest struct {
	Request           NodeNetworkPrepareControlRequest `json:"request"`
	NetNSRelativePath string                           `json:"netns_relative_path"`
}

// Validate rejects a request that could escape ctld's configured netns root.
func (r RuntimeSlotNetworkPrepareRequest) Validate() error {
	if err := r.Request.Validate(); err != nil {
		return err
	}
	return validateRuntimeSlotNetworkRelativePath(r.NetNSRelativePath)
}

func validateRuntimeSlotNetworkRelativePath(path string) error {
	if path == "" || len(path) > 4096 || filepath.IsAbs(path) || filepath.Clean(path) != path ||
		path == "." || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return fmt.Errorf("netns_relative_path must be a canonical relative path")
	}
	return nil
}

// RuntimeSlotNetworkCleanupResponse proves that the durable ctld desired set
// and the last successful node redirect synchronization both exclude a slot.
type RuntimeSlotNetworkCleanupResponse struct {
	NetworkPolicyAbsent bool `json:"network_policy_absent"`
}

// RuntimeSlotNetworkClient calls the root-owned ctld primary Unix endpoint.
// It validates socket ownership and mode for every request so an HA socket
// replacement cannot silently redirect authority to an untrusted process.
type RuntimeSlotNetworkClient struct {
	socketPath string
	timeout    time.Duration
	ownerUID   uint32
}

// NewRuntimeSlotNetworkClient validates immutable local transport policy. The
// socket itself may appear later when the ctld HA primary is elected.
func NewRuntimeSlotNetworkClient(socketPath string, timeout time.Duration) (*RuntimeSlotNetworkClient, error) {
	return newRuntimeSlotNetworkClient(socketPath, timeout, 0)
}

func newRuntimeSlotNetworkClient(
	socketPath string,
	timeout time.Duration,
	ownerUID uint32,
) (*RuntimeSlotNetworkClient, error) {
	socketPath = strings.TrimSpace(socketPath)
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || socketPath == string(filepath.Separator) {
		return nil, fmt.Errorf("ctld runtime slot network socket must be a canonical non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	if timeout <= 0 {
		timeout = defaultRuntimeSlotNetworkTimeout
	}
	return &RuntimeSlotNetworkClient{socketPath: socketPath, timeout: timeout, ownerUID: ownerUID}, nil
}

// Register waits until ctld has durably stored and synchronized the exact warm
// slot under its default-deny policy.
func (c *RuntimeSlotNetworkClient) Register(
	ctx context.Context,
	request RuntimeSlotNetworkRegistrationRequest,
) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate ctld network registration request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	payload, err := c.exchange(ctx, http.MethodPut, RuntimeSlotNetworkRegisterPath, request)
	if err != nil {
		return err
	}
	var result RuntimeSlotNetworkRegistrationResponse
	if err := decodeRuntimeSlotNetworkResponse(payload, &result); err != nil {
		return err
	}
	if !result.NetworkPolicyApplied {
		return fmt.Errorf("ctld did not prove warm default-deny policy application: %w", errdefs.ErrUnavailable)
	}
	return nil
}

// Prepare waits until ctld durably stores and successfully synchronizes the
// exact policy incarnation.
func (c *RuntimeSlotNetworkClient) Prepare(
	ctx context.Context,
	request RuntimeSlotNetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	if err := request.Validate(); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("validate ctld network prepare request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	payload, err := c.exchange(ctx, http.MethodPut, RuntimeSlotNetworkPreparePath, request)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	var token rootfshandoff.NetworkPolicyToken
	if err := decodeRuntimeSlotNetworkResponse(payload, &token); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if err := token.Validate(); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("validate ctld network policy token: %w: %w", err, errdefs.ErrUnavailable)
	}
	if token.AllocationID != request.Request.AllocationID || token.ClaimID != request.Request.ClaimID ||
		token.PolicyDigest != request.Request.PolicyDigest || token.NetNSIdentity != request.Request.NetNSIdentity ||
		token.NetworkIncarnationID != RuntimeSlotNetworkIncarnationID(request.Request) {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("ctld network policy token belongs to another request: %w", errdefs.ErrUnavailable)
	}
	return token, nil
}

// Cleanup waits until ctld's successful node redirect synchronization excludes
// the exact slot incarnation.
func (c *RuntimeSlotNetworkClient) Cleanup(
	ctx context.Context,
	request NodeCleanupControlRequest,
) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate ctld network cleanup request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	payload, err := c.exchange(ctx, http.MethodPut, RuntimeSlotNetworkCleanupPath, request)
	if err != nil {
		return err
	}
	var result RuntimeSlotNetworkCleanupResponse
	if err := decodeRuntimeSlotNetworkResponse(payload, &result); err != nil {
		return err
	}
	if !result.NetworkPolicyAbsent {
		return fmt.Errorf("ctld did not prove runtime slot network policy absence: %w", errdefs.ErrUnavailable)
	}
	return nil
}

// Ping checks that the elected ctld primary owns the secure local endpoint.
func (c *RuntimeSlotNetworkClient) Ping(ctx context.Context) error {
	_, err := c.exchange(ctx, http.MethodGet, RuntimeSlotNetworkHealthPath, nil)
	return err
}

func (c *RuntimeSlotNetworkClient) exchange(ctx context.Context, method, path string, body any) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("ctld runtime slot network client is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := validateRuntimeSlotNetworkSocket(c.socketPath, c.ownerUID); err != nil {
		return nil, err
	}
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode ctld runtime slot network request: %w: %w", err, errdefs.ErrInvalidArgument)
		}
	}
	if len(encoded) > maxRuntimeSlotNetworkBytes {
		return nil, fmt.Errorf("ctld runtime slot network request exceeds 1 MiB: %w", errdefs.ErrInvalidArgument)
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return dialSecureNodeSocket(dialCtx, c.socketPath, c.ownerUID)
		},
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   c.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("ctld runtime slot network redirects are forbidden")
		},
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("create ctld runtime slot network request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("call ctld runtime slot network socket: %w: %w", err, errdefs.ErrUnavailable)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxRuntimeSlotNetworkBytes+1))
	if err != nil || len(payload) > maxRuntimeSlotNetworkBytes {
		return nil, fmt.Errorf("read ctld runtime slot network response: response exceeds 1 MiB: %w", errdefs.ErrUnavailable)
	}
	if response.StatusCode != http.StatusOK {
		return nil, nodeControlResponseError(response.StatusCode, payload)
	}
	return payload, nil
}

func validateRuntimeSlotNetworkSocket(path string, ownerUID uint32) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve ctld runtime slot network socket: %w: %w", err, errdefs.ErrUnavailable)
	}
	if resolved != path {
		return fmt.Errorf("ctld runtime slot network socket must not traverse symlinks: %w", errdefs.ErrPermissionDenied)
	}
	if err := validateSecureNodeSocket(path, ownerUID); err != nil {
		return err
	}
	return nil
}

func decodeRuntimeSlotNetworkResponse(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode ctld runtime slot network response: %w: %w", err, errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("ctld runtime slot network response contains trailing data: %w", errdefs.ErrUnavailable)
	}
	return nil
}
