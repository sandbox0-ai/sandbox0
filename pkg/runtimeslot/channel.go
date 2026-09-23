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
	NodeChannelPath                           = "/internal/v1/runtime-slot-node-channel"
	NodeChannelSubprotocol                    = "sandbox0.runtime-slot.node.v3"
	NodeChannelVersion                        = 3
	NodeChannelMaxBytes                       = 2 << 20
	NodeChannelMaxError                       = 4 << 10
	DefaultNodeChannelCapacityTTLMilliseconds = 90_000
)

// NodeChannelCommandKind identifies one root-owned node operation.
type NodeChannelCommandKind string

const (
	NodeChannelCommandNetworkPrepare                  NodeChannelCommandKind = "network_prepare"
	NodeChannelCommandClaim                           NodeChannelCommandKind = "claim"
	NodeChannelCommandCommandReady                    NodeChannelCommandKind = "command_ready"
	NodeChannelCommandPlannedRetire                   NodeChannelCommandKind = "planned_retire"
	NodeChannelCommandRunningFork                     NodeChannelCommandKind = "running_fork"
	NodeChannelCommandPausedRebase                    NodeChannelCommandKind = "paused_rebase"
	NodeChannelCommandCleanup                         NodeChannelCommandKind = "cleanup"
	NodeChannelCommandMigrationCapture                NodeChannelCommandKind = "migration_capture"
	NodeChannelCommandMigrationCPUPreflight           NodeChannelCommandKind = "migration_cpu_preflight"
	NodeChannelCommandMigrationRecover                NodeChannelCommandKind = "migration_recover"
	NodeChannelCommandMigrationStagingReserve         NodeChannelCommandKind = "migration_staging_reserve"
	NodeChannelCommandMigrationStagingRelease         NodeChannelCommandKind = "migration_staging_release"
	NodeChannelCommandMigrationPublish                NodeChannelCommandKind = "migration_publish"
	NodeChannelCommandMigrationPublicationPlan        NodeChannelCommandKind = "migration_publication_plan"
	NodeChannelCommandMigrationFinalize               NodeChannelCommandKind = "migration_finalize"
	NodeChannelCommandMigrationSourceGC               NodeChannelCommandKind = "migration_source_gc"
	NodeChannelCommandMigrationFence                  NodeChannelCommandKind = "migration_fence"
	NodeChannelCommandMigrationFailureStop            NodeChannelCommandKind = "migration_failure_stop"
	NodeChannelCommandMigrationFailureCleanup         NodeChannelCommandKind = "migration_failure_cleanup"
	NodeChannelCommandMigrationFailureFinalize        NodeChannelCommandKind = "migration_failure_finalize"
	NodeChannelCommandCheckpointImageCancel           NodeChannelCommandKind = "checkpoint_image_cancel"
	NodeChannelCommandMigrationCaptureFailureCleanup  NodeChannelCommandKind = "migration_capture_failure_cleanup"
	NodeChannelCommandMigrationCaptureFailureFinalize NodeChannelCommandKind = "migration_capture_failure_finalize"
	NodeChannelCommandMigrationImagePrepare           NodeChannelCommandKind = "migration_image_prepare"
	NodeChannelCommandMigrationImagePrefetch          NodeChannelCommandKind = "migration_image_prefetch"
	NodeChannelCommandMigrationCapturePeer            NodeChannelCommandKind = "migration_capture_peer"
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
	Capacity        NodeChannelCapacity      `json:"capacity"`
}

// NodeChannelCapacity is the ctld-owned allocatable resource boundary for one
// dedicated node boot. It is refreshed by bounded channel reconnects.
type NodeChannelCapacity struct {
	CPUMillicores int64 `json:"cpu_millicores"`
	MemoryBytes   int64 `json:"memory_bytes"`
	// Admission budgets bound the sum of sandbox limits, independently of
	// the physical cgroup boundary. Zero preserves non-overcommitted admission.
	AdmissionCPUMillicores int64  `json:"admission_cpu_millicores,omitempty"`
	AdmissionMemoryBytes   int64  `json:"admission_memory_bytes,omitempty"`
	CPUSetCPUs             string `json:"cpuset_cpus"`
	CPUSetMems             string `json:"cpuset_mems"`
	TTLMilliseconds        int64  `json:"ttl_milliseconds"`
}

func (c NodeChannelCapacity) Validate() error {
	physicalCPUs, err := ValidateCPUSet(c.CPUSetCPUs)
	if err != nil {
		return fmt.Errorf("node capacity cpuset_cpus: %w", err)
	}
	if _, err := ValidateCPUSet(c.CPUSetMems); err != nil {
		return fmt.Errorf("node capacity cpuset_mems: %w", err)
	}
	if c.CPUMillicores < MinRuntimeCPUMillicores || c.CPUMillicores > int64(physicalCPUs)*1_000 ||
		c.CPUMillicores > MaxRuntimeCPUMillicores {
		return fmt.Errorf("node capacity cpu_millicores must fit its dedicated CPU set and supported range")
	}
	if c.MemoryBytes < 1 || c.MemoryBytes > MaxRuntimeMemoryBytes {
		return fmt.Errorf("node capacity memory is outside the supported range")
	}
	cpu, memory := c.AdmissionLimits()
	if cpu < c.CPUMillicores || cpu > c.CPUMillicores*16 {
		return fmt.Errorf("node CPU admission budget must be between physical capacity and 16 times that capacity")
	}
	if memory < c.MemoryBytes || memory > c.MemoryBytes*2 {
		return fmt.Errorf("node memory admission budget must be between physical capacity and twice that capacity")
	}
	if c.TTLMilliseconds < 1_000 || c.TTLMilliseconds > 600_000 {
		return fmt.Errorf("node capacity TTL must be between 1000 and 600000 milliseconds")
	}
	return nil
}

// AdmissionLimits returns the budget consumed by sandbox CPU/memory limits.
// Physical capacity and per-sandbox cgroup limits are never inflated by it.
func (c NodeChannelCapacity) AdmissionLimits() (int64, int64) {
	cpu, memory := c.AdmissionCPUMillicores, c.AdmissionMemoryBytes
	if cpu == 0 {
		cpu = c.CPUMillicores
	}
	if memory == 0 {
		memory = c.MemoryBytes
	}
	return cpu, memory
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
	if err := h.Capacity.Validate(); err != nil {
		return err
	}
	capabilities := append([]NodeChannelCommandKind(nil), h.Capabilities...)
	if len(capabilities) > 0 && capabilities[0] == NodeChannelCommandNetworkPrepare {
		capabilities = capabilities[1:]
	}
	canonical := []NodeChannelCommandKind{
		NodeChannelCommandPlannedRetire, NodeChannelCommandRunningFork, NodeChannelCommandPausedRebase,
		NodeChannelCommandMigrationCapture, NodeChannelCommandMigrationPublish, NodeChannelCommandMigrationPublicationPlan, NodeChannelCommandMigrationFence, NodeChannelCommandMigrationImagePrepare, NodeChannelCommandMigrationImagePrefetch, NodeChannelCommandMigrationCapturePeer, NodeChannelCommandMigrationFinalize, NodeChannelCommandMigrationSourceGC,
		NodeChannelCommandMigrationCPUPreflight, NodeChannelCommandMigrationRecover,
		NodeChannelCommandMigrationStagingReserve, NodeChannelCommandMigrationStagingRelease, NodeChannelCommandMigrationFailureStop, NodeChannelCommandMigrationFailureCleanup, NodeChannelCommandCheckpointImageCancel, NodeChannelCommandMigrationFailureFinalize,
		NodeChannelCommandMigrationCaptureFailureCleanup, NodeChannelCommandMigrationCaptureFailureFinalize,
	}
	if len(capabilities) < 3 || len(capabilities) > 3+len(canonical) ||
		capabilities[0] != NodeChannelCommandClaim || capabilities[1] != NodeChannelCommandCommandReady ||
		capabilities[len(capabilities)-1] != NodeChannelCommandCleanup {
		return fmt.Errorf("node channel capabilities are incomplete")
	}
	optional := capabilities[2 : len(capabilities)-1]

	canonicalIndex := 0
	for _, capability := range optional {
		for canonicalIndex < len(canonical) && canonical[canonicalIndex] != capability {
			canonicalIndex++
		}
		if canonicalIndex == len(canonical) {
			return fmt.Errorf("node channel capabilities must use the canonical order")
		}
		canonicalIndex++
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
	MigrationStaging                *MigrationStagingRequest                `json:"migration_staging,omitempty"`
	MigrationCPUPreflight           *MigrationCPUPreflightRequest           `json:"migration_cpu_preflight,omitempty"`
	MigrationSourceGC               *MigrationSourceGCRequest               `json:"migration_source_gc,omitempty"`
	MigrationFinalize               *MigrationSourceFinalizeRequest         `json:"migration_finalize,omitempty"`
	MigrationImagePrepare           *MigrationImagePrepareRequest           `json:"migration_image_prepare,omitempty"`
	MigrationImagePrefetch          *MigrationImagePrefetchRequest          `json:"migration_image_prefetch,omitempty"`
	MigrationCapturePeer            *MigrationCapturePeerRequest            `json:"migration_capture_peer,omitempty"`
	MigrationFailureStop            *MigrationFailureRequest                `json:"migration_failure_stop,omitempty"`
	MigrationFailureCleanup         *MigrationFailureCleanupRequest         `json:"migration_failure_cleanup,omitempty"`
	CheckpointImageCancel           *CheckpointImageCancelRequest           `json:"checkpoint_image_cancel,omitempty"`
	MigrationFailureFinalize        *MigrationFailureFinalizeRequest        `json:"migration_failure_finalize,omitempty"`
	MigrationCaptureFailureCleanup  *MigrationCaptureFailureRequest         `json:"migration_capture_failure_cleanup,omitempty"`
	MigrationCaptureFailureFinalize *MigrationCaptureFailureFinalizeRequest `json:"migration_capture_failure_finalize,omitempty"`
	MigrationFence                  *MigrationSourceFenceRequest            `json:"migration_fence,omitempty"`
	MigrationPublish                *MigrationPublicationRequest            `json:"migration_publish,omitempty"`
	MigrationPublicationPlan        *MigrationPublicationRequest            `json:"migration_publication_plan,omitempty"`
	MigrationCapture                *MigrationCaptureRequest                `json:"migration_capture,omitempty"`
	Version                         int                                     `json:"version"`
	RequestID                       string                                  `json:"request_id"`
	Kind                            NodeChannelCommandKind                  `json:"kind"`
	Target                          NodeChannelTarget                       `json:"target"`
	NetworkPrepare                  *NodeNetworkPrepareControlRequest       `json:"network_prepare,omitempty"`
	Claim                           *NodeClaimControlRequest                `json:"claim,omitempty"`
	CommandReady                    *CommandReadyControlRequest             `json:"command_ready,omitempty"`
	PlannedRetire                   *NodePlannedRetireControlRequest        `json:"planned_retire,omitempty"`
	RunningFork                     *NodeRunningForkControlRequest          `json:"running_fork,omitempty"`
	PausedRebase                    *NodePausedRebaseControlRequest         `json:"paused_rebase,omitempty"`
	Cleanup                         *NodeCleanupControlRequest              `json:"cleanup,omitempty"`
}

const NodePlannedRetireProofVersion = 1

// NodePlannedRetireControlRequest binds a durable local RootFS retirement
// marker to the exact regional pause transaction and writer incarnation. The
// node persists this marker before the manager quiesces or stops the allocation.
type NodePlannedRetireControlRequest struct {
	OperationID    string `json:"operation_id"`
	ClaimID        string `json:"claim_id"`
	SlotID         string `json:"slot_id"`
	AllocationID   string `json:"allocation_id"`
	WriterGrantID  string `json:"writer_grant_id"`
	WriterEpoch    int64  `json:"writer_epoch"`
	BindingVersion int    `json:"binding_version"`
	BindingDigest  string `json:"binding_digest"`
}

// Validate rejects a retirement plan detached from its exact slot and writer.
func (r NodePlannedRetireControlRequest) Validate() error {
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID, "slot_id": r.SlotID,
		"allocation_id": r.AllocationID, "writer_grant_id": r.WriterGrantID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if r.WriterEpoch <= 0 || r.BindingVersion != rootfshandoff.WriterBindingVersion {
		return fmt.Errorf("writer epoch or binding version is invalid")
	}
	binding, err := hex.DecodeString(r.BindingDigest)
	if err != nil || len(binding) != sha256.Size || hex.EncodeToString(binding) != r.BindingDigest {
		return fmt.Errorf("binding_digest must be canonical 32-byte hexadecimal")
	}
	return nil
}

// NodePlannedRetireControlProof is byte-stable evidence that the exact request
// was persisted in the node RootFS session journal.
type NodePlannedRetireControlProof struct {
	Version       int    `json:"version"`
	OperationID   string `json:"operation_id"`
	SlotID        string `json:"slot_id"`
	AllocationID  string `json:"allocation_id"`
	WriterGrantID string `json:"writer_grant_id"`
	ProofDigest   string `json:"proof_digest"`
}

// NewNodePlannedRetireControlProof builds the deterministic acknowledgement
// returned only after the local retirement marker is durable.
func NewNodePlannedRetireControlProof(request NodePlannedRetireControlRequest) (NodePlannedRetireControlProof, error) {
	if err := request.Validate(); err != nil {
		return NodePlannedRetireControlProof{}, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return NodePlannedRetireControlProof{}, err
	}
	digest := sha256.Sum256(payload)
	return NodePlannedRetireControlProof{
		Version: NodePlannedRetireProofVersion, OperationID: request.OperationID,
		SlotID: request.SlotID, AllocationID: request.AllocationID,
		WriterGrantID: request.WriterGrantID, ProofDigest: hex.EncodeToString(digest[:]),
	}, nil
}

// ValidateFor proves that an acknowledgement belongs to the exact plan.
func (p NodePlannedRetireControlProof) ValidateFor(request NodePlannedRetireControlRequest) error {
	expected, err := NewNodePlannedRetireControlProof(request)
	if err != nil {
		return err
	}
	if p != expected {
		return fmt.Errorf("planned-retire proof belongs to another request")
	}
	return nil
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
// application for an initial claim or a fenced active-policy mutation.
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
	// PolicyRevision is the PostgreSQL slot revision captured by the mutation.
	// Zero is reserved for initial claim; later revisions fence delayed retries,
	// including ABA changes back to previously applied policy bytes.
	PolicyRevision       int64  `json:"policy_revision,omitempty"`
	ExpectedPolicyDigest string `json:"expected_policy_digest,omitempty"`
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
	if r.PolicyRevision < 0 || (r.PolicyRevision == 0) != (r.ExpectedPolicyDigest == "") {
		return fmt.Errorf("network mutation requires both a positive slot revision and expected policy digest")
	}
	if r.ExpectedPolicyDigest != "" {
		if err := digest.Digest(r.ExpectedPolicyDigest).Validate(); err != nil {
			return fmt.Errorf("invalid expected network policy digest: %w", err)
		}
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

// NewNodeChannelPlannedRetireCommand builds an exact durable RootFS retirement
// planning command for an authenticated source node boot.
func NewNodeChannelPlannedRetireCommand(
	target NodeChannelTarget,
	request NodePlannedRetireControlRequest,
) (NodeChannelCommand, error) {
	command := NodeChannelCommand{
		Version: NodeChannelVersion, Kind: NodeChannelCommandPlannedRetire,
		Target: target, PlannedRetire: &request,
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
	case NodeChannelCommandMigrationSourceGC:
		if c.MigrationSourceGC == nil || c.payloadCount() != 1 || c.MigrationSourceGC.Target != c.Target {
			return fmt.Errorf("migration source GC command changed target")
		}
		if err := c.MigrationSourceGC.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationFinalize:
		if c.MigrationFinalize == nil || c.payloadCount() != 1 || c.MigrationFinalize.Fence.PublicationRequest.Capture.Request.Target != c.Target {
			return fmt.Errorf("migration finalization command changed source target")
		}
		if err := c.MigrationFinalize.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationCapturePeer:
		if c.MigrationCapturePeer == nil || c.payloadCount() != 1 || c.MigrationCapturePeer.Staging.Target != c.Target {
			return fmt.Errorf("migration capture peer changed the exact destination")
		}
		if err := c.MigrationCapturePeer.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationImagePrefetch:
		if c.MigrationImagePrefetch == nil || c.payloadCount() != 1 || c.MigrationImagePrefetch.Staging.Target != c.Target {
			return fmt.Errorf("migration prefetch changed the exact destination")
		}
		if err := c.MigrationImagePrefetch.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationImagePrepare:
		if c.MigrationImagePrepare == nil || c.payloadCount() != 1 || c.MigrationImagePrepare.Target != c.Target {
			return fmt.Errorf("migration image preparation changed its destination")
		}
		if err := c.MigrationImagePrepare.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationCaptureFailureCleanup:
		if c.MigrationCaptureFailureCleanup == nil || c.payloadCount() != 1 || c.MigrationCaptureFailureCleanup.Capture.Request.Target != c.Target {
			return fmt.Errorf("failed capture command changed source")
		}
		if _, err := c.MigrationCaptureFailureCleanup.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationCaptureFailureFinalize:
		if c.MigrationCaptureFailureFinalize == nil || c.payloadCount() != 1 || c.MigrationCaptureFailureFinalize.Request.Capture.Request.Target != c.Target {
			return fmt.Errorf("failed capture command changed source")
		}
		if _, err := c.MigrationCaptureFailureFinalize.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandCheckpointImageCancel:
		if c.CheckpointImageCancel == nil || c.payloadCount() != 1 || c.CheckpointImageCancel.Image.Target != c.Target {
			return fmt.Errorf("checkpoint image cancellation changed destination")
		}
		if _, err := c.CheckpointImageCancel.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationFailureFinalize:
		if c.MigrationFailureFinalize == nil || c.payloadCount() != 1 || c.MigrationFailureFinalize.Request.Failure.Request.Restore.Image.Target != c.Target {
			return fmt.Errorf("migration failure cleanup changed its destination")
		}
		if _, err := c.MigrationFailureFinalize.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationFailureCleanup:
		if c.MigrationFailureCleanup == nil || c.payloadCount() != 1 || c.MigrationFailureCleanup.Failure.Request.Restore.Image.Target != c.Target {
			return fmt.Errorf("migration failure cleanup changed its destination")
		}
		if _, err := c.MigrationFailureCleanup.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationFailureStop:
		if c.MigrationFailureStop == nil || c.payloadCount() != 1 || c.MigrationFailureStop.Restore.Image.Target != c.Target {
			return fmt.Errorf("migration failure stop changed its destination")
		}
		if _, err := c.MigrationFailureStop.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationFence:
		if c.MigrationFence == nil || c.payloadCount() != 1 || c.MigrationFence.PublicationRequest.Capture.Request.Target != c.Target {
			return fmt.Errorf("migration fence changed the exact source")
		}
		if _, err := c.MigrationFence.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationPublicationPlan:
		if c.MigrationPublicationPlan == nil || c.payloadCount() != 1 || c.MigrationPublicationPlan.Capture.Request.Target != c.Target {
			return fmt.Errorf("planned publication changed the exact source target")
		}
		if _, err := c.MigrationPublicationPlan.Digest(); err != nil {
			return err
		}
		if c.MigrationPublicationPlan.DestinationPeerCertificateSHA256 == "" {
			return fmt.Errorf("planned publication requires a reserved destination peer")
		}
	case NodeChannelCommandMigrationPublish:
		if c.MigrationPublish == nil || c.payloadCount() != 1 || c.MigrationPublish.Capture.Request.Target != c.Target {
			return fmt.Errorf("migration publication changed the exact source target")
		}
		if _, err := c.MigrationPublish.Digest(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationStagingReserve, NodeChannelCommandMigrationStagingRelease:
		if c.MigrationStaging == nil || c.payloadCount() != 1 || c.MigrationStaging.Target != c.Target {
			return fmt.Errorf("staging command changed request or target")
		}
		if err := c.MigrationStaging.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationCPUPreflight:
		if c.MigrationCPUPreflight == nil || c.payloadCount() != 1 || c.MigrationCPUPreflight.Target != c.Target {
			return fmt.Errorf("CPU preflight changed exact target")
		}
		if err := c.MigrationCPUPreflight.Validate(); err != nil {
			return err
		}
	case NodeChannelCommandMigrationCapture, NodeChannelCommandMigrationRecover:
		if c.MigrationCapture == nil || c.payloadCount() != 1 || c.MigrationCapture.Target != c.Target {
			return fmt.Errorf("migration capture command changed the exact source target")
		}
		if err := c.MigrationCapture.Validate(); err != nil {
			return err
		}
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
		if c.Claim.MigrationRestore != nil && c.Claim.MigrationRestore.Image.Target != c.Target {
			return fmt.Errorf("restore claim changed the exact destination")
		}
		identity := c.Claim.Stage.Identity
		if identity.SlotNonce != c.Target.SlotID || identity.AllocationID != c.Target.AllocationID ||
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
	case NodeChannelCommandPlannedRetire:
		if c.PlannedRetire == nil || c.payloadCount() != 1 {
			return fmt.Errorf("planned-retire command must contain only a retirement request")
		}
		if err := c.Target.validate(false); err != nil {
			return err
		}
		if err := c.PlannedRetire.Validate(); err != nil {
			return fmt.Errorf("planned-retire request: %w", err)
		}
		if c.PlannedRetire.SlotID != c.Target.SlotID ||
			c.PlannedRetire.AllocationID != c.Target.AllocationID {
			return fmt.Errorf("planned-retire request does not match the node channel target")
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
		c.MigrationCapture != nil,
		c.MigrationCPUPreflight != nil,
		c.MigrationStaging != nil,
		c.MigrationPublish != nil,
		c.MigrationPublicationPlan != nil,
		c.MigrationFinalize != nil,
		c.MigrationSourceGC != nil,
		c.MigrationFence != nil,
		c.MigrationFailureStop != nil,
		c.MigrationFailureCleanup != nil,
		c.MigrationFailureFinalize != nil,
		c.CheckpointImageCancel != nil,
		c.MigrationCaptureFailureCleanup != nil,
		c.MigrationCaptureFailureFinalize != nil,
		c.MigrationImagePrepare != nil,
		c.MigrationImagePrefetch != nil,
		c.MigrationCapturePeer != nil,
		c.NetworkPrepare != nil, c.Claim != nil, c.CommandReady != nil,
		c.PlannedRetire != nil, c.RunningFork != nil, c.PausedRebase != nil, c.Cleanup != nil,
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
	MigrationStagingReserved        *MigrationStagingReserved                  `json:"migration_staging_reserved,omitempty"`
	MigrationStagingReleased        *MigrationStagingReleased                  `json:"migration_staging_released,omitempty"`
	MigrationCPUPreflight           *MigrationCPUPreflight                     `json:"migration_cpu_preflight,omitempty"`
	MigrationSourceGC               *MigrationSourceGCAcknowledgement          `json:"migration_source_gc,omitempty"`
	MigrationFinalize               *MigrationSourceFinalizeProof              `json:"migration_finalize,omitempty"`
	MigrationImagePrepare           *MigrationImagePrepared                    `json:"migration_image_prepare,omitempty"`
	MigrationImagePrefetch          *MigrationImagePrefetched                  `json:"migration_image_prefetch,omitempty"`
	MigrationCapturePeer            *MigrationCapturePeerPrepared              `json:"migration_capture_peer,omitempty"`
	MigrationFailureStop            *MigrationFailureStopProof                 `json:"migration_failure_stop,omitempty"`
	MigrationFailureCleanup         *MigrationFailureCleanupProof              `json:"migration_failure_cleanup,omitempty"`
	CheckpointImageCancel           *CheckpointImageCancelProof                `json:"checkpoint_image_cancel,omitempty"`
	MigrationFailureFinalize        *MigrationFailureFinalizeProof             `json:"migration_failure_finalize,omitempty"`
	MigrationCaptureFailureCleanup  *MigrationCaptureFailureProof              `json:"migration_capture_failure_cleanup,omitempty"`
	MigrationCaptureFailureFinalize *MigrationCaptureFailureFinalizeProof      `json:"migration_capture_failure_finalize,omitempty"`
	MigrationFence                  *MigrationSourceFenceProof                 `json:"migration_fence,omitempty"`
	MigrationPublish                *MigrationPublication                      `json:"migration_publish,omitempty"`
	MigrationPublicationPlan        *MigrationPublicationPlan                  `json:"migration_publication_plan,omitempty"`
	MigrationCapture                *MigrationCapture                          `json:"migration_capture,omitempty"`
	Version                         int                                        `json:"version"`
	RequestID                       string                                     `json:"request_id"`
	Kind                            NodeChannelCommandKind                     `json:"kind"`
	NetworkPolicyToken              *rootfshandoff.NetworkPolicyToken          `json:"network_policy_token,omitempty"`
	ControlResponse                 *NodeControlResponse                       `json:"control_response,omitempty"`
	PlannedRetireProof              *NodePlannedRetireControlProof             `json:"planned_retire_proof,omitempty"`
	RunningFork                     *rootfshandoff.RunningForkCheckpointResult `json:"running_fork,omitempty"`
	PausedRebase                    *rootfsrebase.WorkerResult                 `json:"paused_rebase,omitempty"`
	PausedRebaseReject              *rootfsrebase.WorkerRejection              `json:"paused_rebase_rejection,omitempty"`
	PausedRebaseAck                 *rootfsrebase.WorkerAcknowledgement        `json:"paused_rebase_ack,omitempty"`
	CleanupProof                    *NodeCleanupControlProof                   `json:"cleanup_proof,omitempty"`
	Error                           string                                     `json:"error,omitempty"`
	ErrorClass                      NodeChannelErrorClass                      `json:"error_class,omitempty"`
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
	case NodeChannelCommandMigrationSourceGC:
		if r.MigrationSourceGC == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration source GC acknowledgement is absent")
		}
		return r.MigrationSourceGC.ValidateFor(*command.MigrationSourceGC)
	case NodeChannelCommandMigrationFinalize:
		if r.MigrationFinalize == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration finalization result is incomplete")
		}
		return r.MigrationFinalize.ValidateFor(*command.MigrationFinalize)
	case NodeChannelCommandMigrationCapturePeer:
		if r.MigrationCapturePeer == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration capture peer receipt is missing")
		}
		return r.MigrationCapturePeer.ValidateFor(*command.MigrationCapturePeer)
	case NodeChannelCommandMigrationImagePrefetch:
		if r.MigrationImagePrefetch == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration prefetch receipt is missing")
		}
		return r.MigrationImagePrefetch.ValidateFor(*command.MigrationImagePrefetch)
	case NodeChannelCommandMigrationImagePrepare:
		if r.MigrationImagePrepare == nil || r.payloadCount() != 1 {
			return fmt.Errorf("prepared migration image receipt is missing")
		}
		return r.MigrationImagePrepare.ValidateFor(*command.MigrationImagePrepare)
	case NodeChannelCommandMigrationCaptureFailureCleanup:
		if r.MigrationCaptureFailureCleanup == nil || r.payloadCount() != 1 {
			return fmt.Errorf("failed capture proof is missing")
		}
		return r.MigrationCaptureFailureCleanup.ValidateFor(*command.MigrationCaptureFailureCleanup)
	case NodeChannelCommandMigrationCaptureFailureFinalize:
		if r.MigrationCaptureFailureFinalize == nil || r.payloadCount() != 1 {
			return fmt.Errorf("failed capture proof is missing")
		}
		return r.MigrationCaptureFailureFinalize.ValidateFor(*command.MigrationCaptureFailureFinalize)
	case NodeChannelCommandCheckpointImageCancel:
		if r.CheckpointImageCancel == nil || r.payloadCount() != 1 {
			return fmt.Errorf("checkpoint image cancellation proof is missing")
		}
		return r.CheckpointImageCancel.ValidateFor(*command.CheckpointImageCancel)
	case NodeChannelCommandMigrationFailureFinalize:
		if r.MigrationFailureFinalize == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration failure cleanup proof is missing")
		}
		return r.MigrationFailureFinalize.ValidateFor(*command.MigrationFailureFinalize)
	case NodeChannelCommandMigrationFailureCleanup:
		if r.MigrationFailureCleanup == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration failure cleanup proof is missing")
		}
		return r.MigrationFailureCleanup.ValidateFor(*command.MigrationFailureCleanup)
	case NodeChannelCommandMigrationFailureStop:
		if r.MigrationFailureStop == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration failure stop proof is missing")
		}
		return r.MigrationFailureStop.ValidateFor(*command.MigrationFailureStop)
	case NodeChannelCommandMigrationFence:
		if r.MigrationFence == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration fence proof is missing")
		}
		return r.MigrationFence.ValidateFor(*command.MigrationFence)
	case NodeChannelCommandMigrationPublicationPlan:
		if r.MigrationPublicationPlan == nil || r.payloadCount() != 1 {
			return fmt.Errorf("planned publication result is missing")
		}
		return r.MigrationPublicationPlan.ValidateFor(*command.MigrationPublicationPlan)
	case NodeChannelCommandMigrationPublish:
		if r.MigrationPublish == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration publication result is missing")
		}
		if err := r.MigrationPublish.ValidateFor(*command.MigrationPublish); err != nil {
			return err
		}
		return nil
	case NodeChannelCommandMigrationStagingReserve:
		if r.MigrationStagingReserved == nil || r.payloadCount() != 1 {
			return fmt.Errorf("staging reservation needs its exact receipt")
		}
		return r.MigrationStagingReserved.ValidateFor(*command.MigrationStaging)
	case NodeChannelCommandMigrationStagingRelease:
		if r.MigrationStagingReleased == nil || r.payloadCount() != 1 {
			return fmt.Errorf("staging release needs its exact receipt")
		}
		return r.MigrationStagingReleased.ValidateFor(*command.MigrationStaging)
	case NodeChannelCommandMigrationCPUPreflight:
		if r.MigrationCPUPreflight == nil || r.payloadCount() != 1 {
			return fmt.Errorf("CPU preflight result is incomplete")
		}
		return r.MigrationCPUPreflight.ValidateFor(*command.MigrationCPUPreflight)
	case NodeChannelCommandMigrationCapture, NodeChannelCommandMigrationRecover:
		if r.MigrationCapture == nil || r.payloadCount() != 1 {
			return fmt.Errorf("migration capture result is incomplete")
		}
		if err := r.MigrationCapture.Validate(); err != nil {
			return err
		}
		digest, err := command.MigrationCapture.Digest()
		if err != nil || r.MigrationCapture.RequestDigest != digest {
			return fmt.Errorf("migration capture result belongs to another source")
		}
		return nil
	case NodeChannelCommandNetworkPrepare:
		if r.NetworkPolicyToken == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel network policy result is incomplete")
		}
		if err := r.NetworkPolicyToken.Validate(); err != nil {
			return err
		}
		request := command.NetworkPrepare
		if r.NetworkPolicyToken.AllocationID != request.AllocationID ||
			r.NetworkPolicyToken.ClaimID != request.ClaimID ||
			r.NetworkPolicyToken.PolicyDigest != request.PolicyDigest ||
			r.NetworkPolicyToken.NetNSIdentity != request.NetNSIdentity ||
			r.NetworkPolicyToken.NetworkIncarnationID != RuntimeSlotNetworkIncarnationID(*request) {
			return fmt.Errorf("node channel network policy token belongs to another request")
		}
		return nil
	case NodeChannelCommandClaim, NodeChannelCommandCommandReady:
		if r.ControlResponse == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel control result is incomplete")
		}
		if command.Kind == NodeChannelCommandClaim {
			return r.ControlResponse.ValidateClaimResult(*command.Claim)
		}
		if a := r.ControlResponse.MigrationAdoption; a != nil && a.Request.Target != command.Target {
			return fmt.Errorf("adoption receipt changed target node")
		}
		return r.ControlResponse.ValidateCommandReadyResult(*command.CommandReady)
	case NodeChannelCommandPlannedRetire:
		if r.PlannedRetireProof == nil || r.payloadCount() != 1 {
			return fmt.Errorf("node channel planned-retire result is incomplete")
		}
		return r.PlannedRetireProof.ValidateFor(*command.PlannedRetire)
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
		r.MigrationCapture != nil,
		r.MigrationCPUPreflight != nil,
		r.MigrationStagingReserved != nil,
		r.MigrationStagingReleased != nil,
		r.MigrationPublish != nil,
		r.MigrationPublicationPlan != nil,
		r.MigrationFinalize != nil,
		r.MigrationSourceGC != nil,
		r.MigrationFence != nil,
		r.MigrationFailureStop != nil,
		r.MigrationFailureCleanup != nil,
		r.MigrationFailureFinalize != nil,
		r.CheckpointImageCancel != nil,
		r.MigrationCaptureFailureCleanup != nil,
		r.MigrationCaptureFailureFinalize != nil,
		r.MigrationImagePrepare != nil,
		r.MigrationImagePrefetch != nil,
		r.MigrationCapturePeer != nil,
		r.NetworkPolicyToken != nil, r.ControlResponse != nil, r.PlannedRetireProof != nil, r.RunningFork != nil,
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
