package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
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
	NodeChannelCommandRunningFork    NodeChannelCommandKind = "running_fork"
	NodeChannelCommandPausedRebase   NodeChannelCommandKind = "paused_rebase"
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
	capabilities := append([]NodeChannelCommandKind(nil), h.Capabilities...)
	if len(capabilities) > 0 && capabilities[0] == NodeChannelCommandNetworkPrepare {
		capabilities = capabilities[1:]
	}
	if len(capabilities) < 3 || len(capabilities) > 5 ||
		capabilities[0] != NodeChannelCommandClaim || capabilities[1] != NodeChannelCommandCommandReady ||
		capabilities[len(capabilities)-1] != NodeChannelCommandCleanup {
		return fmt.Errorf("node channel capabilities are incomplete")
	}
	optional := capabilities[2 : len(capabilities)-1]
	if len(optional) == 1 && optional[0] != NodeChannelCommandRunningFork && optional[0] != NodeChannelCommandPausedRebase {
		return fmt.Errorf("node channel capabilities must use the canonical order")
	}
	if len(optional) == 2 && (optional[0] != NodeChannelCommandRunningFork || optional[1] != NodeChannelCommandPausedRebase) {
		return fmt.Errorf("node channel capabilities must use the canonical order")
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
		return fmt.Errorf("node channel target must not contain an unused control endpoint")
	}
	return nil
}

func (t NodeChannelTarget) validateNodeOnly() error {
	for name, value := range map[string]string{
		"cluster_id": t.ClusterID, "node_id": t.NodeID,
		"node_uid": t.NodeUID, "node_boot_id": t.NodeBootID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if t.SlotID != "" || t.AllocationID != "" || t.ControlEndpoint != "" {
		return fmt.Errorf("node-only channel target must not contain runtime slot identity")
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
	RunningFork    *NodeRunningForkControlRequest    `json:"running_fork,omitempty"`
	PausedRebase   *NodePausedRebaseControlRequest   `json:"paused_rebase,omitempty"`
	Cleanup        *NodeCleanupControlRequest        `json:"cleanup,omitempty"`
}

// NodePausedRebaseControlRequest carries exact PostgreSQL pre-operation
// authority to one root-owned offline worker. It has no runtime-slot binding.
type NodePausedRebaseControlRequest struct {
	Worker                 rootfsrebase.WorkerRequest `json:"worker"`
	Reject                 bool                       `json:"reject,omitempty"`
	AcknowledgeProofDigest string                     `json:"acknowledge_proof_digest,omitempty"`
}

// Validate rejects an incomplete offline worker command.
func (r NodePausedRebaseControlRequest) Validate() error {
	if err := r.Worker.Validate(); err != nil {
		return err
	}
	if r.Reject && r.AcknowledgeProofDigest != "" {
		return fmt.Errorf("paused-rebase rejection and acknowledgement are mutually exclusive")
	}
	if r.AcknowledgeProofDigest == "" {
		return nil
	}
	parsed, err := digest.Parse(r.AcknowledgeProofDigest)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != r.AcknowledgeProofDigest {
		return fmt.Errorf("acknowledge_proof_digest must be a canonical sha256 digest")
	}
	return nil
}

// NodeRunningForkControlRequest binds a manager-triggered live checkpoint to
// the exact source writer and a pre-created paused target.
type NodeRunningForkControlRequest struct {
	Fork                       rootfshandoff.RunningForkCheckpointRequest `json:"fork"`
	SourceFilesystemID         string                                     `json:"source_filesystem_id"`
	SourceWriterGrantID        string                                     `json:"source_writer_grant_id"`
	SourceWriterEpoch          int64                                      `json:"source_writer_epoch"`
	BindingVersion             int                                        `json:"binding_version"`
	BindingDigest              string                                     `json:"binding_digest"`
	ExpectedSourceGenerationID string                                     `json:"expected_source_generation_id"`
}

// Validate rejects a fork detached from its durable source writer binding.
func (r NodeRunningForkControlRequest) Validate() error {
	if err := r.Fork.Validate(); err != nil {
		return fmt.Errorf("fork: %w", err)
	}
	for name, value := range map[string]string{
		"source_filesystem_id": r.SourceFilesystemID, "source_writer_grant_id": r.SourceWriterGrantID,
		"expected_source_generation_id": r.ExpectedSourceGenerationID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if r.SourceWriterEpoch <= 0 || r.BindingVersion != rootfshandoff.WriterBindingVersion {
		return fmt.Errorf("source writer epoch or binding version is invalid")
	}
	binding, err := hex.DecodeString(r.BindingDigest)
	if err != nil || len(binding) != sha256.Size || hex.EncodeToString(binding) != r.BindingDigest {
		return fmt.Errorf("binding_digest must be canonical 32-byte hexadecimal")
	}
	return nil
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

// NewNodeChannelRunningForkCommand builds an exact live RootFS checkpoint
// command for an authenticated source node boot.
func NewNodeChannelRunningForkCommand(
	target NodeChannelTarget,
	request NodeRunningForkControlRequest,
) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandRunningFork,
		Target: target, RunningFork: &request,
	}
	return sealNodeChannelCommand(command)
}

// NewNodeChannelPausedRebaseCommand builds an exact offline RootFS rebase
// command for an authenticated node boot.
func NewNodeChannelPausedRebaseCommand(
	target NodeChannelTarget,
	request NodePausedRebaseControlRequest,
) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandPausedRebase,
		Target: target, PausedRebase: &request,
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
		if c.NetworkPrepare == nil || c.payloadCount() != 1 {
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
		if c.Claim == nil || c.payloadCount() != 1 {
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
		if c.CommandReady == nil || c.payloadCount() != 1 {
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
	case NodeChannelCommandRunningFork:
		if c.RunningFork == nil || c.payloadCount() != 1 {
			return fmt.Errorf("running-fork command must contain only a running-fork request")
		}
		if err := c.Target.validate(false); err != nil {
			return err
		}
		if err := c.RunningFork.Validate(); err != nil {
			return fmt.Errorf("running-fork request: %w", err)
		}
	case NodeChannelCommandPausedRebase:
		if c.PausedRebase == nil || c.payloadCount() != 1 {
			return fmt.Errorf("paused-rebase command must contain only a rebase request")
		}
		if err := c.Target.validateNodeOnly(); err != nil {
			return err
		}
		if err := c.PausedRebase.Validate(); err != nil {
			return fmt.Errorf("paused-rebase request: %w", err)
		}
	case NodeChannelCommandCleanup:
		if c.Cleanup == nil || c.payloadCount() != 1 {
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

func (c NodeChannelCommand) payloadCount() int {
	count := 0
	for _, present := range []bool{
		c.NetworkPrepare != nil, c.Claim != nil, c.CommandReady != nil,
		c.RunningFork != nil, c.PausedRebase != nil, c.Cleanup != nil,
	} {
		if present {
			count++
		}
	}
	return count
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
	Version            int                                        `json:"version"`
	RequestID          string                                     `json:"request_id"`
	Kind               NodeChannelCommandKind                     `json:"kind"`
	NetworkPolicyToken *rootfshandoff.NetworkPolicyToken          `json:"network_policy_token,omitempty"`
	ControlResponse    *NodeControlResponse                       `json:"control_response,omitempty"`
	RunningFork        *rootfshandoff.RunningForkCheckpointResult `json:"running_fork,omitempty"`
	PausedRebase       *rootfsrebase.WorkerResult                 `json:"paused_rebase,omitempty"`
	PausedRebaseReject *rootfsrebase.WorkerRejection              `json:"paused_rebase_rejection,omitempty"`
	PausedRebaseAck    *rootfsrebase.WorkerAcknowledgement        `json:"paused_rebase_ack,omitempty"`
	CleanupProof       *NodeCleanupControlProof                   `json:"cleanup_proof,omitempty"`
	Error              string                                     `json:"error,omitempty"`
	ErrorClass         NodeChannelErrorClass                      `json:"error_class,omitempty"`
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
			!r.ErrorClass.valid() || r.payloadCount() != 0 {
			return fmt.Errorf("node channel error result is invalid")
		}
		return nil
	}
	switch command.Kind {
	case NodeChannelCommandNetworkPrepare:
		if r.NetworkPolicyToken == nil || r.payloadCount() != 1 {
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
		if r.ControlResponse == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel control result is incomplete")
		}
		return r.ControlResponse.Validate()
	case NodeChannelCommandRunningFork:
		if r.RunningFork == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel running-fork result is incomplete")
		}
		if err := r.RunningFork.Validate(); err != nil {
			return err
		}
		request := command.RunningFork
		proof := r.RunningFork.Proof
		if proof.OperationID != request.Fork.OperationID || proof.SourceSandboxID != request.Fork.SourceSandboxID ||
			proof.TargetSandboxID != request.Fork.TargetSandboxID ||
			proof.CheckpointGenerationID != request.Fork.TargetGenerationID ||
			proof.SourceFilesystemID != request.SourceFilesystemID ||
			proof.SourceWriterGrantID != request.SourceWriterGrantID ||
			proof.SourceWriterEpoch != request.SourceWriterEpoch || proof.BindingVersion != request.BindingVersion ||
			proof.BindingDigest != request.BindingDigest ||
			proof.ExpectedSourceGenerationID != request.ExpectedSourceGenerationID {
			return fmt.Errorf("node channel running-fork result belongs to another writer or target")
		}
		return nil
	case NodeChannelCommandPausedRebase:
		if command.PausedRebase.Reject {
			if r.PausedRebaseReject == nil || r.payloadCount() != 1 {
				return fmt.Errorf("node channel paused-rebase rejection is incomplete")
			}
			return r.PausedRebaseReject.ValidateFor(command.PausedRebase.Worker)
		}
		if command.PausedRebase.AcknowledgeProofDigest == "" {
			if r.PausedRebase == nil || r.payloadCount() != 1 {
				return fmt.Errorf("node channel paused-rebase result is incomplete")
			}
			if err := r.PausedRebase.ValidateFor(command.PausedRebase.Worker); err != nil {
				return fmt.Errorf("node channel paused-rebase result is invalid: %w", err)
			}
			return nil
		}
		if r.PausedRebaseAck == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel paused-rebase acknowledgement is incomplete")
		}
		return r.PausedRebaseAck.ValidateFor(
			command.PausedRebase.Worker, command.PausedRebase.AcknowledgeProofDigest,
		)
	case NodeChannelCommandCleanup:
		if r.CleanupProof == nil || r.payloadCount() != 1 {
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

func (r NodeChannelResult) payloadCount() int {
	count := 0
	for _, present := range []bool{
		r.NetworkPolicyToken != nil, r.ControlResponse != nil, r.RunningFork != nil,
		r.PausedRebase != nil, r.PausedRebaseReject != nil,
		r.PausedRebaseAck != nil, r.CleanupProof != nil,
	} {
		if present {
			count++
		}
	}
	return count
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
