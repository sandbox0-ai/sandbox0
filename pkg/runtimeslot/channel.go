package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const (
	// NodeChannelPath is the mutually authenticated, node-initiated command
	// channel. The channel never persists claim messages because they contain a
	// one-time raw writer grant.
	NodeChannelPath        = "/internal/v1/runtime-slot-node-channel"
	NodeChannelSubprotocol = "sandbox0.runtime-slot.node.v2"
	NodeChannelVersion     = 2
	NodeChannelMaxBytes    = 2 << 20
	NodeChannelMaxError    = 4 << 10
)

// NodeChannelCommandKind identifies one root-owned node operation.
type NodeChannelCommandKind string

const (
	NodeChannelCommandNetworkPrepare NodeChannelCommandKind = "network_prepare"
	NodeChannelCommandClaim          NodeChannelCommandKind = "claim"
	NodeChannelCommandCommandReady   NodeChannelCommandKind = "command_ready"
	NodeChannelCommandCleanup        NodeChannelCommandKind = "cleanup"
)

// NodeChannelErrorClass is a bounded retry classification returned by a node.
type NodeChannelErrorClass string

const (
	NodeChannelErrorInvalidArgument    NodeChannelErrorClass = "invalid_argument"
	NodeChannelErrorNotFound           NodeChannelErrorClass = "not_found"
	NodeChannelErrorAlreadyExists      NodeChannelErrorClass = "already_exists"
	NodeChannelErrorFailedPrecondition NodeChannelErrorClass = "failed_precondition"
	NodeChannelErrorPermissionDenied   NodeChannelErrorClass = "permission_denied"
	NodeChannelErrorResourceExhausted  NodeChannelErrorClass = "resource_exhausted"
	NodeChannelErrorUnavailable        NodeChannelErrorClass = "unavailable"
	NodeChannelErrorInternal           NodeChannelErrorClass = "internal"
)

// NodeChannelHello binds one authenticated stream to an exact node boot. The
// server must derive ClusterID, NodeID, and NodeUID from transport
// authentication and compare them with these values before registration.
type NodeChannelHello struct {
	Version         int                      `json:"version"`
	AgentInstanceID string                   `json:"agent_instance_id"`
	ClusterID       string                   `json:"cluster_id"`
	NodeID          string                   `json:"node_id"`
	NodeUID         string                   `json:"node_uid"`
	NodeBootID      string                   `json:"node_boot_id"`
	Capabilities    []NodeChannelCommandKind `json:"capabilities"`
}

// Validate rejects ambiguous stream identity and capability negotiation.
func (h NodeChannelHello) Validate() error {
	if h.Version != NodeChannelVersion {
		return fmt.Errorf("unsupported node channel version %d", h.Version)
	}
	fields := []struct{ name, value string }{
		{name: "agent_instance_id", value: h.AgentInstanceID},
		{name: "cluster_id", value: h.ClusterID},
		{name: "node_id", value: h.NodeID},
		{name: "node_uid", value: h.NodeUID},
		{name: "node_boot_id", value: h.NodeBootID},
	}
	for _, field := range fields {
		if err := validateRequiredID(field.name, field.value); err != nil {
			return err
		}
	}
	want := []NodeChannelCommandKind{
		NodeChannelCommandClaim,
		NodeChannelCommandCommandReady,
		NodeChannelCommandCleanup,
	}
	if len(h.Capabilities) == len(want)+1 {
		if h.Capabilities[0] != NodeChannelCommandNetworkPrepare {
			return fmt.Errorf("node channel network_prepare must be the first capability")
		}
		h.Capabilities = h.Capabilities[1:]
	}
	if len(h.Capabilities) != len(want) {
		return fmt.Errorf("node channel capabilities are incomplete")
	}
	for index := range want {
		if h.Capabilities[index] != want[index] {
			return fmt.Errorf("node channel capabilities must use the canonical order")
		}
	}
	return nil
}

// Supports reports whether an already validated hello supports one command.
func (h NodeChannelHello) Supports(kind NodeChannelCommandKind) bool {
	for _, capability := range h.Capabilities {
		if capability == kind {
			return true
		}
	}
	return false
}

// NodeChannelTarget is immutable regional routing identity. ControlEndpoint
// is meaningful only for claim and command-ready operations and remains a
// node-local Unix endpoint.
type NodeChannelTarget struct {
	SlotID          string `json:"slot_id"`
	ClusterID       string `json:"cluster_id"`
	AllocationID    string `json:"allocation_id"`
	NodeID          string `json:"node_id"`
	NodeUID         string `json:"node_uid"`
	NodeBootID      string `json:"node_boot_id"`
	ControlEndpoint string `json:"control_endpoint,omitempty"`
}

func (t NodeChannelTarget) validate(withControl bool) error {
	fields := []struct{ name, value string }{
		{name: "slot_id", value: t.SlotID},
		{name: "cluster_id", value: t.ClusterID},
		{name: "allocation_id", value: t.AllocationID},
		{name: "node_id", value: t.NodeID},
		{name: "node_uid", value: t.NodeUID},
		{name: "node_boot_id", value: t.NodeBootID},
	}
	for _, field := range fields {
		if err := validateRequiredID(field.name, field.value); err != nil {
			return err
		}
	}
	if withControl {
		if err := validateControlEndpoint(t.ControlEndpoint); err != nil {
			return err
		}
		parsed, err := url.Parse(t.ControlEndpoint)
		if err != nil || parsed.Scheme != "unix" || parsed.Host != "" || parsed.Opaque != "" ||
			parsed.RawPath != "" || parsed.String() != t.ControlEndpoint {
			return fmt.Errorf("node channel control endpoint must be a canonical local Unix URL")
		}
	} else if t.ControlEndpoint != "" {
		return fmt.Errorf("cleanup target must not contain a control endpoint")
	}
	return nil
}

// NodeChannelCommand contains exactly one operation. RequestID is the
// canonical digest of every target and request byte, so a delayed response
// cannot satisfy another command.
type NodeChannelCommand struct {
	Version        int                               `json:"version"`
	RequestID      string                            `json:"request_id"`
	Kind           NodeChannelCommandKind            `json:"kind"`
	Target         NodeChannelTarget                 `json:"target"`
	NetworkPrepare *NodeNetworkPrepareControlRequest `json:"network_prepare,omitempty"`
	Claim          *NodeClaimControlRequest          `json:"claim,omitempty"`
	CommandReady   *CommandReadyControlRequest       `json:"command_ready,omitempty"`
	Cleanup        *NodeCleanupControlRequest        `json:"cleanup,omitempty"`
}

// NodeNetworkPrepareControlRequest binds one exact ctld-owned network policy
// application before a writer grant is issued.
type NodeNetworkPrepareControlRequest struct {
	OperationID   string `json:"operation_id"`
	ClaimID       string `json:"claim_id"`
	SlotID        string `json:"slot_id"`
	ClusterID     string `json:"cluster_id"`
	AllocationID  string `json:"allocation_id"`
	NodeID        string `json:"node_id"`
	NodeUID       string `json:"node_uid"`
	NodeBootID    string `json:"node_boot_id"`
	NetNSIdentity string `json:"netns_identity"`
	NetworkPolicy string `json:"network_policy"`
	PolicyDigest  string `json:"policy_digest"`
}

// Validate rejects network preparation detached from its physical slot or
// raw policy bytes.
func (r NodeNetworkPrepareControlRequest) Validate() error {
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID, "slot_id": r.SlotID,
		"cluster_id": r.ClusterID, "allocation_id": r.AllocationID, "node_id": r.NodeID,
		"node_uid": r.NodeUID, "node_boot_id": r.NodeBootID, "netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if len(r.NetworkPolicy) > MaxNetworkPolicyBytes {
		return fmt.Errorf("network policy exceeds 64 KiB")
	}
	if r.PolicyDigest != NetworkPolicyDigest(r.NetworkPolicy) {
		return fmt.Errorf("network policy digest does not match raw policy")
	}
	return nil
}

// RuntimeSlotNetworkIncarnationID derives the byte-stable physical network
// identity independently checked by the region, node channel, and ctld.
func RuntimeSlotNetworkIncarnationID(request NodeNetworkPrepareControlRequest) string {
	return runtimeSlotNetworkIncarnationID(
		request.ClusterID,
		request.SlotID,
		request.AllocationID,
		request.NodeID,
		request.NodeUID,
		request.NodeBootID,
		request.NetNSIdentity,
	)
}

func runtimeSlotNetworkIncarnationID(clusterID, slotID, allocationID, nodeID, nodeUID, nodeBootID, netnsIdentity string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		clusterID,
		slotID,
		allocationID,
		nodeID,
		nodeUID,
		nodeBootID,
		netnsIdentity,
	}, "\x00")))
	return "nomad-net-v1:" + hex.EncodeToString(digest[:])
}

// NewNodeChannelNetworkPrepareCommand builds an exact ctld-owned network
// policy application command.
func NewNodeChannelNetworkPrepareCommand(
	target NodeChannelTarget,
	request NodeNetworkPrepareControlRequest,
) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandNetworkPrepare,
		Target: target, NetworkPrepare: &request,
	}
	return sealNodeChannelCommand(command)
}

// NewNodeChannelClaimCommand builds an exact node-local claim command.
func NewNodeChannelClaimCommand(target NodeChannelTarget, request NodeClaimControlRequest) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandClaim,
		Target: target, Claim: &request,
	}
	return sealNodeChannelCommand(command)
}

// NewNodeChannelCommandReadyCommand builds an exact command-ready commit.
func NewNodeChannelCommandReadyCommand(target NodeChannelTarget, request CommandReadyControlRequest) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandCommandReady,
		Target: target, CommandReady: &request,
	}
	return sealNodeChannelCommand(command)
}

// NewNodeChannelCleanupCommand builds an exact plugin-independent cleanup.
func NewNodeChannelCleanupCommand(target NodeChannelTarget, request NodeCleanupControlRequest) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandCleanup,
		Target: target, Cleanup: &request,
	}
	return sealNodeChannelCommand(command)
}

func sealNodeChannelCommand(command NodeChannelCommand) (NodeChannelCommand, error) {
	requestID, err := command.digest()
	if err != nil {
		return NodeChannelCommand{}, err
	}
	command.RequestID = requestID
	if err := command.Validate(); err != nil {
		return NodeChannelCommand{}, err
	}
	return command, nil
}

// Validate checks the command shape, physical target, and canonical digest.
func (c NodeChannelCommand) Validate() error {
	if c.Version != NodeChannelVersion {
		return fmt.Errorf("unsupported node channel command version %d", c.Version)
	}
	if len(c.RequestID) != sha256.Size*2 {
		return fmt.Errorf("node channel request_id must be a canonical SHA-256 digest")
	}
	requestID, err := c.digest()
	if err != nil {
		return err
	}
	if c.RequestID != requestID {
		return fmt.Errorf("node channel request_id does not match the command")
	}
	switch c.Kind {
	case NodeChannelCommandNetworkPrepare:
		if c.NetworkPrepare == nil || c.Claim != nil || c.CommandReady != nil || c.Cleanup != nil {
			return fmt.Errorf("network-prepare command must contain only a network request")
		}
		if err := c.Target.validate(false); err != nil {
			return err
		}
		if err := c.NetworkPrepare.Validate(); err != nil {
			return fmt.Errorf("network-prepare request: %w", err)
		}
		request := c.NetworkPrepare
		if request.SlotID != c.Target.SlotID || request.ClusterID != c.Target.ClusterID ||
			request.AllocationID != c.Target.AllocationID || request.NodeID != c.Target.NodeID ||
			request.NodeUID != c.Target.NodeUID || request.NodeBootID != c.Target.NodeBootID {
			return fmt.Errorf("network-prepare request does not match the node channel target")
		}
	case NodeChannelCommandClaim:
		if c.Claim == nil || c.NetworkPrepare != nil || c.CommandReady != nil || c.Cleanup != nil {
			return fmt.Errorf("claim command must contain only a claim request")
		}
		if err := c.Target.validate(true); err != nil {
			return err
		}
		if err := c.Claim.ValidateRegional(); err != nil {
			return fmt.Errorf("claim request: %w", err)
		}
		identity := c.Claim.Stage.Identity
		if identity.SlotNonce != c.Target.SlotID || identity.PodUID != c.Target.AllocationID ||
			identity.NodeUID != c.Target.NodeUID || identity.BootID != c.Target.NodeBootID {
			return fmt.Errorf("claim request does not match the node channel target")
		}
	case NodeChannelCommandCommandReady:
		if c.CommandReady == nil || c.NetworkPrepare != nil || c.Claim != nil || c.Cleanup != nil {
			return fmt.Errorf("command-ready command must contain only a command-ready request")
		}
		if err := c.Target.validate(true); err != nil {
			return err
		}
		if err := c.CommandReady.Proof.Validate(); err != nil {
			return fmt.Errorf("command-ready request: %w", err)
		}
		if c.CommandReady.Proof.SlotID != c.Target.SlotID {
			return fmt.Errorf("command-ready request does not match the node channel target")
		}
	case NodeChannelCommandCleanup:
		if c.Cleanup == nil || c.NetworkPrepare != nil || c.Claim != nil || c.CommandReady != nil {
			return fmt.Errorf("cleanup command must contain only a cleanup request")
		}
		if err := c.Target.validate(false); err != nil {
			return err
		}
		if err := c.Cleanup.Validate(); err != nil {
			return fmt.Errorf("cleanup request: %w", err)
		}
		request := c.Cleanup
		if request.SlotID != c.Target.SlotID || request.ClusterID != c.Target.ClusterID ||
			request.AllocationID != c.Target.AllocationID || request.NodeID != c.Target.NodeID ||
			request.NodeUID != c.Target.NodeUID || request.NodeBootID != c.Target.NodeBootID {
			return fmt.Errorf("cleanup request does not match the node channel target")
		}
	default:
		return fmt.Errorf("unsupported node channel command kind %q", c.Kind)
	}
	return nil
}

func (c NodeChannelCommand) digest() (string, error) {
	c.RequestID = ""
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode node channel command: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// NodeChannelResult contains either one exact success payload or one bounded
// classified error for the matching command.
type NodeChannelResult struct {
	Version            int                               `json:"version"`
	RequestID          string                            `json:"request_id"`
	Kind               NodeChannelCommandKind            `json:"kind"`
	NetworkPolicyToken *rootfshandoff.NetworkPolicyToken `json:"network_policy_token,omitempty"`
	ControlResponse    *NodeControlResponse              `json:"control_response,omitempty"`
	CleanupProof       *NodeCleanupControlProof          `json:"cleanup_proof,omitempty"`
	Error              string                            `json:"error,omitempty"`
	ErrorClass         NodeChannelErrorClass             `json:"error_class,omitempty"`
}

// ValidateFor rejects a response for any command other than the exact request.
func (r NodeChannelResult) ValidateFor(command NodeChannelCommand) error {
	if err := command.Validate(); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	if r.Version != NodeChannelVersion || r.RequestID != command.RequestID || r.Kind != command.Kind {
		return fmt.Errorf("node channel result belongs to another command")
	}
	if r.Error != "" || r.ErrorClass != "" {
		if strings.TrimSpace(r.Error) != r.Error || r.Error == "" || len(r.Error) > NodeChannelMaxError ||
			!r.ErrorClass.valid() || r.NetworkPolicyToken != nil || r.ControlResponse != nil || r.CleanupProof != nil {
			return fmt.Errorf("node channel error result is invalid")
		}
		return nil
	}
	switch command.Kind {
	case NodeChannelCommandNetworkPrepare:
		if r.NetworkPolicyToken == nil || r.ControlResponse != nil || r.CleanupProof != nil {
			return fmt.Errorf("node channel network policy result is incomplete")
		}
		if err := r.NetworkPolicyToken.Validate(); err != nil {
			return err
		}
		request := command.NetworkPrepare
		if r.NetworkPolicyToken.PodUID != request.AllocationID ||
			r.NetworkPolicyToken.ClaimID != request.ClaimID ||
			r.NetworkPolicyToken.PolicyDigest != request.PolicyDigest ||
			r.NetworkPolicyToken.NetNSIdentity != request.NetNSIdentity ||
			r.NetworkPolicyToken.PodSandboxID != RuntimeSlotNetworkIncarnationID(*request) {
			return fmt.Errorf("node channel network policy token belongs to another request")
		}
		return nil
	case NodeChannelCommandClaim, NodeChannelCommandCommandReady:
		if r.ControlResponse == nil || r.NetworkPolicyToken != nil || r.CleanupProof != nil {
			return fmt.Errorf("node channel control result is incomplete")
		}
		return r.ControlResponse.Validate()
	case NodeChannelCommandCleanup:
		if r.CleanupProof == nil || r.NetworkPolicyToken != nil || r.ControlResponse != nil {
			return fmt.Errorf("node channel cleanup result is incomplete")
		}
		if err := r.CleanupProof.Validate(); err != nil {
			return err
		}
		if r.CleanupProof.Request() != *command.Cleanup {
			return fmt.Errorf("node channel cleanup proof belongs to another request")
		}
		return nil
	default:
		return fmt.Errorf("unsupported node channel result kind %q", command.Kind)
	}
}

func (c NodeChannelErrorClass) valid() bool {
	switch c {
	case NodeChannelErrorInvalidArgument, NodeChannelErrorNotFound,
		NodeChannelErrorAlreadyExists, NodeChannelErrorFailedPrecondition,
		NodeChannelErrorPermissionDenied, NodeChannelErrorResourceExhausted,
		NodeChannelErrorUnavailable,
		NodeChannelErrorInternal:
		return true
	default:
		return false
	}
}
