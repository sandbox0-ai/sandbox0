// Package runtimeslot defines the versioned node-to-region protocol for
// generic warm runtime allocations.
package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const (
	PathPrefix                      = "/internal/v1/runtime-slots/"
	ProcdCommandReadyProbePath      = "/api/v1/runtime/command-ready-probe"
	NodeClaimControlPath            = "/claim"
	NodeCommandReadyControlPath     = "/command-ready"
	NodeCleanupControlPath          = "/v1/runtime-slots/cleanup"
	CommandReadyProofVersion        = 1
	legacyNodeCleanupProofVersion   = 2
	NodeCleanupProofVersion         = 3
	NomadProcdPort                  = 49983
	NomadProcdPortLabel             = "procd"
	NomadTaskName                   = "slot"
	RuntimeAssignmentRevisionLabel  = "sandbox0.ai/runtime-assignment-revision"
	RuntimeResourceLeaseDigestLabel = "sandbox0.ai/runtime-resource-lease-digest"
	MaxRuntimeAssignmentBytes       = 64 << 10
	MaxNetworkPolicyBytes           = 64 << 10

	readyPathSuffix        = "/ready"
	heartbeatPathSuffix    = "/heartbeat"
	startingPathSuffix     = "/starting"
	commandReadyPathSuffix = "/command-ready"
)

const (
	WriterRetireKindCanceled       = "canceled"
	WriterRetireKindCrashAbandon   = "crash_abandon"
	WriterRetireKindPlannedPublish = "planned_publish"
	WriterRetireKindPrelaunchAbort = "prelaunch_abort"
)

// NodeCleanupControlRequest identifies one plugin-independent terminal cleanup
// against the root-owned ctld Nomad runtime. WriterAuthorityDigest is exact
// evidence from the regional authority; it is never accepted as a liveness
// assertion without node-local absence checks.
type NodeCleanupControlRequest struct {
	OperationID           string               `json:"operation_id"`
	WriterOperationID     string               `json:"writer_operation_id,omitempty"`
	WriterRetireKind      string               `json:"writer_retire_kind,omitempty"`
	SlotID                string               `json:"slot_id"`
	ClusterID             string               `json:"cluster_id"`
	AllocationID          string               `json:"allocation_id"`
	NodeID                string               `json:"node_id"`
	NodeUID               string               `json:"node_uid"`
	NodeBootID            string               `json:"node_boot_id"`
	NetNSIdentity         string               `json:"netns_identity"`
	RunscContainerID      string               `json:"runsc_container_id,omitempty"`
	WriterGrantID         string               `json:"writer_grant_id,omitempty"`
	WriterAuthorityDigest string               `json:"writer_authority_digest,omitempty"`
	Resources             RuntimeResourceLease `json:"resources"`
	ResourceLeaseDigest   string               `json:"resource_lease_digest,omitempty"`
}

// Validate rejects incomplete or non-canonical node cleanup identities.
func (r NodeCleanupControlRequest) Validate() error {
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "slot_id": r.SlotID, "cluster_id": r.ClusterID,
		"allocation_id": r.AllocationID, "node_id": r.NodeID, "node_uid": r.NodeUID,
		"node_boot_id": r.NodeBootID, "netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{
		"runsc_container_id": r.RunscContainerID, "writer_grant_id": r.WriterGrantID,
		"writer_operation_id": r.WriterOperationID, "writer_retire_kind": r.WriterRetireKind,
	} {
		if value != "" {
			if err := validateRequiredID(name, value); err != nil {
				return err
			}
		}
	}
	if r.Resources.IsZero() {
		if r.ResourceLeaseDigest != "" {
			return fmt.Errorf("resource lease digest requires a resource lease")
		}
	} else {
		if err := r.Resources.Validate(); err != nil {
			return fmt.Errorf("resource lease: %w", err)
		}
		if r.Resources.SlotID != r.SlotID || r.Resources.ClusterID != r.ClusterID ||
			r.Resources.NodeID != r.NodeID || r.Resources.NodeUID != r.NodeUID ||
			r.Resources.NodeBootID != r.NodeBootID {
			return fmt.Errorf("resource lease does not match cleanup incarnation")
		}
		digest, err := r.Resources.Digest()
		if err != nil {
			return fmt.Errorf("digest resource lease: %w", err)
		}
		expectedDigest := strings.TrimPrefix(digest, "sha256:")
		if _, err := DecodeProof("resource_lease_digest", r.ResourceLeaseDigest); err != nil {
			return err
		}
		if r.ResourceLeaseDigest != expectedDigest {
			return fmt.Errorf("resource lease digest does not match cleanup lease")
		}
	}
	if r.WriterGrantID == "" {
		if r.WriterOperationID != "" || r.WriterRetireKind != "" || r.WriterAuthorityDigest != "" {
			return fmt.Errorf("writer operation, retirement kind, and authority proof require a writer grant")
		}
		return nil
	}
	if r.WriterOperationID == "" {
		return fmt.Errorf("writer_operation_id is required for a writer grant")
	}
	if !validWriterRetireKind(r.WriterRetireKind) {
		return fmt.Errorf("writer_retire_kind is invalid")
	}
	_, err := DecodeProof("writer_authority_digest", r.WriterAuthorityDigest)
	return err
}

// NodeCleanupControlProof is stable evidence that the exact node incarnation
// no longer owns runsc, RootFS, or network state.
type NodeCleanupControlProof struct {
	Version               int                  `json:"version"`
	OperationID           string               `json:"operation_id"`
	WriterOperationID     string               `json:"writer_operation_id,omitempty"`
	SlotID                string               `json:"slot_id"`
	ClusterID             string               `json:"cluster_id"`
	AllocationID          string               `json:"allocation_id"`
	NodeID                string               `json:"node_id"`
	NodeUID               string               `json:"node_uid"`
	NodeBootID            string               `json:"node_boot_id"`
	NetNSIdentity         string               `json:"netns_identity"`
	RunscContainerID      string               `json:"runsc_container_id,omitempty"`
	WriterGrantID         string               `json:"writer_grant_id,omitempty"`
	WriterRetireKind      string               `json:"writer_retire_kind,omitempty"`
	WriterAuthorityDigest string               `json:"writer_authority_digest,omitempty"`
	RootFSOperationID     string               `json:"rootfs_operation_id,omitempty"`
	RootFSProofDigest     string               `json:"rootfs_proof_digest,omitempty"`
	Resources             RuntimeResourceLease `json:"resources"`
	ResourceLeaseID       string               `json:"resource_lease_id,omitempty"`
	ResourceLeaseDigest   string               `json:"resource_lease_digest,omitempty"`
	RunscAbsent           bool                 `json:"runsc_absent"`
	StableMountAbsent     bool                 `json:"stable_mount_absent"`
	RootFSWriterAbsent    bool                 `json:"rootfs_writer_absent"`
	NetworkPolicyAbsent   bool                 `json:"network_policy_absent"`
	ResourceCgroupAbsent  bool                 `json:"resource_cgroup_absent"`
	ProofDigest           string               `json:"proof_digest"`
}

// legacyNodeCleanupControlProof preserves the exact version-2 JSON shape so a
// ctld restart can finish a cleanup proof durably written before resource
// leasing was deployed. Version 2 is accepted only when every resource field
// is absent.
type legacyNodeCleanupControlProof struct {
	Version               int    `json:"version"`
	OperationID           string `json:"operation_id"`
	WriterOperationID     string `json:"writer_operation_id,omitempty"`
	SlotID                string `json:"slot_id"`
	ClusterID             string `json:"cluster_id"`
	AllocationID          string `json:"allocation_id"`
	NodeID                string `json:"node_id"`
	NodeUID               string `json:"node_uid"`
	NodeBootID            string `json:"node_boot_id"`
	NetNSIdentity         string `json:"netns_identity"`
	RunscContainerID      string `json:"runsc_container_id,omitempty"`
	WriterGrantID         string `json:"writer_grant_id,omitempty"`
	WriterRetireKind      string `json:"writer_retire_kind,omitempty"`
	WriterAuthorityDigest string `json:"writer_authority_digest,omitempty"`
	RootFSOperationID     string `json:"rootfs_operation_id,omitempty"`
	RootFSProofDigest     string `json:"rootfs_proof_digest,omitempty"`
	RunscAbsent           bool   `json:"runsc_absent"`
	StableMountAbsent     bool   `json:"stable_mount_absent"`
	RootFSWriterAbsent    bool   `json:"rootfs_writer_absent"`
	NetworkPolicyAbsent   bool   `json:"network_policy_absent"`
	ProofDigest           string `json:"proof_digest"`
}

// Validate checks every terminal fact and the canonical proof digest.
func (p NodeCleanupControlProof) Validate() error {
	request := p.Request()
	if err := request.Validate(); err != nil {
		return err
	}
	if p.Version != legacyNodeCleanupProofVersion && p.Version != NodeCleanupProofVersion {
		return fmt.Errorf("unsupported node cleanup proof version %d", p.Version)
	}
	if !p.RunscAbsent || !p.StableMountAbsent || !p.RootFSWriterAbsent || !p.NetworkPolicyAbsent {
		return fmt.Errorf("node cleanup proof does not establish physical absence")
	}
	if p.Version == legacyNodeCleanupProofVersion &&
		(!p.Resources.IsZero() || p.ResourceLeaseID != "" || p.ResourceLeaseDigest != "" || p.ResourceCgroupAbsent) {
		return fmt.Errorf("legacy cleanup proof contains resource lease facts")
	}
	if p.Resources.IsZero() {
		if p.ResourceLeaseID != "" || p.ResourceLeaseDigest != "" || p.ResourceCgroupAbsent {
			return fmt.Errorf("legacy cleanup proof contains resource lease facts")
		}
	} else if p.ResourceLeaseID != p.Resources.LeaseID || !p.ResourceCgroupAbsent {
		return fmt.Errorf("node cleanup proof does not establish resource cgroup absence")
	}
	if p.WriterGrantID == "" {
		if p.RootFSOperationID != "" || p.RootFSProofDigest != "" {
			return fmt.Errorf("RootFS absence proof requires a writer grant")
		}
	} else {
		if err := validateRequiredID("rootfs_operation_id", p.RootFSOperationID); err != nil {
			return err
		}
		if p.RootFSOperationID != p.WriterOperationID {
			return fmt.Errorf("RootFS absence proof does not match the writer operation")
		}
		if _, err := DecodeProof("rootfs_proof_digest", p.RootFSProofDigest); err != nil {
			return err
		}
	}
	digest, err := p.Digest()
	if err != nil {
		return err
	}
	if p.ProofDigest != digest {
		return fmt.Errorf("node cleanup proof digest does not match its facts")
	}
	return nil
}

// Request reconstructs the immutable cleanup request bound by this proof.
func (p NodeCleanupControlProof) Request() NodeCleanupControlRequest {
	return NodeCleanupControlRequest{
		OperationID: p.OperationID, WriterOperationID: p.WriterOperationID,
		WriterRetireKind: p.WriterRetireKind,
		SlotID:           p.SlotID, ClusterID: p.ClusterID, AllocationID: p.AllocationID,
		NodeID: p.NodeID, NodeUID: p.NodeUID, NodeBootID: p.NodeBootID,
		NetNSIdentity: p.NetNSIdentity, RunscContainerID: p.RunscContainerID,
		WriterGrantID: p.WriterGrantID, WriterAuthorityDigest: p.WriterAuthorityDigest,
		Resources: p.Resources, ResourceLeaseDigest: p.ResourceLeaseDigest,
	}
}

func validWriterRetireKind(kind string) bool {
	switch kind {
	case WriterRetireKindCanceled, WriterRetireKindCrashAbandon,
		WriterRetireKindPlannedPublish, WriterRetireKindPrelaunchAbort:
		return true
	default:
		return false
	}
}

// Digest hashes the cleanup facts without the self-referential ProofDigest.
func (p NodeCleanupControlProof) Digest() (string, error) {
	var value any
	switch p.Version {
	case legacyNodeCleanupProofVersion:
		if !p.Resources.IsZero() || p.ResourceLeaseID != "" || p.ResourceLeaseDigest != "" || p.ResourceCgroupAbsent {
			return "", fmt.Errorf("legacy cleanup proof contains resource lease facts")
		}
		value = legacyNodeCleanupControlProof{
			Version: p.Version, OperationID: p.OperationID, WriterOperationID: p.WriterOperationID,
			SlotID: p.SlotID, ClusterID: p.ClusterID, AllocationID: p.AllocationID,
			NodeID: p.NodeID, NodeUID: p.NodeUID, NodeBootID: p.NodeBootID,
			NetNSIdentity: p.NetNSIdentity, RunscContainerID: p.RunscContainerID,
			WriterGrantID: p.WriterGrantID, WriterRetireKind: p.WriterRetireKind,
			WriterAuthorityDigest: p.WriterAuthorityDigest, RootFSOperationID: p.RootFSOperationID,
			RootFSProofDigest: p.RootFSProofDigest, RunscAbsent: p.RunscAbsent,
			StableMountAbsent: p.StableMountAbsent, RootFSWriterAbsent: p.RootFSWriterAbsent,
			NetworkPolicyAbsent: p.NetworkPolicyAbsent,
		}
	case NodeCleanupProofVersion:
		p.ProofDigest = ""
		value = p
	default:
		return "", fmt.Errorf("unsupported node cleanup proof version %d", p.Version)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode node cleanup proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// NodeClaimControlRequest is the one-shot region-to-driver claim contract.
// The raw writer token is present only in this online request and must never
// be persisted by an intermediate node proxy.
type NodeClaimControlRequest struct {
	OperationID   string                      `json:"operation_id,omitempty"`
	ClaimID       string                      `json:"claim_id,omitempty"`
	RootfsPath    string                      `json:"rootfs_path"`
	PolicyToken   string                      `json:"policy_token"`
	WriterEpoch   string                      `json:"writer_epoch"`
	Stage         *rootfshandoff.StageRequest `json:"stage,omitempty"`
	NetworkPolicy string                      `json:"network_policy,omitempty"`
	Runtime       *runtimecontrol.Assignment  `json:"runtime,omitempty"`
	Resources     RuntimeResourceLease        `json:"resources"`
}

// ValidateRegional rejects development-only claims before they reach the
// root-owned node control socket.
func (r NodeClaimControlRequest) ValidateRegional() error {
	if err := validateRequiredID("operation_id", r.OperationID); err != nil {
		return err
	}
	if err := validateRequiredID("claim_id", r.ClaimID); err != nil {
		return err
	}
	if r.RootfsPath != "" {
		return fmt.Errorf("rootfs_path is forbidden for regional claims")
	}
	if r.Stage == nil {
		return fmt.Errorf("stage is required for regional claims")
	}
	if err := r.Stage.Validate(); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if r.ClaimID != r.Stage.Identity.ClaimID {
		return fmt.Errorf("claim_id does not match stage")
	}
	if r.PolicyToken != r.Stage.Identity.WriterGrantToken {
		return fmt.Errorf("policy_token does not match stage writer grant")
	}
	if r.WriterEpoch != strconv.FormatInt(r.Stage.Identity.WriterEpoch, 10) {
		return fmt.Errorf("writer_epoch does not match stage")
	}
	if err := r.Resources.Validate(); err != nil {
		return fmt.Errorf("resource lease: %w", err)
	}
	if r.Resources.OperationID != r.OperationID || r.Resources.ClaimID != r.ClaimID ||
		r.Resources.SlotID != r.Stage.Identity.SlotNonce ||
		r.Resources.NodeUID != r.Stage.Identity.NodeUID || r.Resources.NodeBootID != r.Stage.Identity.BootID {
		return fmt.Errorf("resource lease does not match the regional claim")
	}
	resourceDigest, err := r.Resources.Digest()
	if err != nil {
		return fmt.Errorf("resource lease digest: %w", err)
	}
	if r.Stage.Labels[RuntimeResourceLeaseDigestLabel] != resourceDigest {
		return fmt.Errorf("resource lease digest does not match stage")
	}
	if len(r.NetworkPolicy) > MaxNetworkPolicyBytes {
		return fmt.Errorf("network policy exceeds 64 KiB")
	}
	if NetworkPolicyDigest(r.NetworkPolicy) != r.Stage.ExpectedPolicyToken.PolicyDigest {
		return fmt.Errorf("network_policy does not match stage policy token")
	}
	if r.Runtime == nil {
		return fmt.Errorf("runtime assignment is required for regional claims")
	}
	if err := r.Runtime.Validate(); err != nil {
		return fmt.Errorf("runtime assignment: %w", err)
	}
	if r.Runtime.EnvVars[runtimecontrol.EnvSandboxID] != r.Runtime.SandboxID {
		return fmt.Errorf("runtime assignment sandbox environment does not match sandbox_id")
	}
	payload, err := json.Marshal(r.Runtime)
	if err != nil {
		return fmt.Errorf("encode runtime assignment: %w", err)
	}
	if len(payload) > MaxRuntimeAssignmentBytes {
		return fmt.Errorf("runtime assignment exceeds 64 KiB")
	}
	revision, err := r.Runtime.Revision()
	if err != nil {
		return fmt.Errorf("runtime assignment revision: %w", err)
	}
	if r.Stage.Labels[RuntimeAssignmentRevisionLabel] != revision {
		return fmt.Errorf("runtime assignment revision does not match stage")
	}
	return nil
}

// NetworkPolicyDigest is the canonical digest bound into a RootFS network
// incarnation token and independently recomputed by the task driver.
func NetworkPolicyDigest(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(digest[:])
}

// NodeControlResponse is returned only after the driver has durably reached
// the requested local phase.
type NodeControlResponse struct {
	Phase string `json:"phase"`
}

func (r NodeControlResponse) Validate() error {
	if r.Phase != string(StateActive) {
		return fmt.Errorf("node control phase must be active")
	}
	return nil
}

// CommandReadyProof is the canonical evidence produced after a manager has
// completed an authenticated, runtime-gated command against one procd process.
// It is submitted over the root-only node control socket; the driver hashes it
// before advancing the regional slot.
type CommandReadyProof struct {
	Version            int    `json:"version"`
	SlotID             string `json:"slot_id"`
	OperationID        string `json:"operation_id"`
	ClaimID            string `json:"claim_id"`
	LaunchAttempt      string `json:"launch_attempt"`
	RunscContainerID   string `json:"runsc_container_id"`
	ProcdInstanceID    string `json:"procd_instance_id"`
	ProcdAddress       string `json:"procd_address"`
	RequestMethod      string `json:"request_method"`
	RequestPath        string `json:"request_path"`
	ResponseStatus     int    `json:"response_status"`
	ResponseBodyDigest string `json:"response_body_digest"`
}

// CommandReadyControlRequest is sent by manager over the root-only driver
// control socket after the procd probe succeeds.
type CommandReadyControlRequest struct {
	Proof CommandReadyProof `json:"proof"`
}

func (p CommandReadyProof) Validate() error {
	if p.Version != CommandReadyProofVersion {
		return fmt.Errorf("unsupported command-ready proof version %d", p.Version)
	}
	for name, value := range map[string]string{
		"slot_id": p.SlotID, "operation_id": p.OperationID, "claim_id": p.ClaimID,
		"launch_attempt": p.LaunchAttempt, "runsc_container_id": p.RunscContainerID,
		"procd_instance_id": p.ProcdInstanceID, "procd_address": p.ProcdAddress,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if p.RequestMethod != "PUT" || p.RequestPath != ProcdCommandReadyProbePath || p.ResponseStatus != 200 {
		return fmt.Errorf("command-ready proof does not describe the canonical procd probe")
	}
	if err := ValidateNomadProcdAddress(p.ProcdAddress); err != nil {
		return err
	}
	if _, err := DecodeProof("response_body_digest", p.ResponseBodyDigest); err != nil {
		return err
	}
	return nil
}

// NomadProcdAddress returns the canonical allocation-local procd origin.
func NomadProcdAddress(ip string) (string, error) {
	if ip != strings.TrimSpace(ip) {
		return "", fmt.Errorf("procd IP must be canonical")
	}
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.String() != ip {
		return "", fmt.Errorf("procd IP must be canonical")
	}
	return "http://" + net.JoinHostPort(parsed.String(), strconv.Itoa(NomadProcdPort)), nil
}

// NomadRunscContainerID derives the task driver's stable runsc identity from
// the region-authoritative slot ID.
func NomadRunscContainerID(slotID string) string {
	digest := sha256.Sum256([]byte(slotID))
	return "s0-" + hex.EncodeToString(digest[:16])
}

// NomadNetworkChainName derives the stable historical chain identity recorded
// with a Nomad runtime slot. Ctld owns policy application; the name remains in
// cleanup proofs so node incarnations cannot be confused.
func NomadNetworkChainName(containerID string) string {
	value := strings.TrimPrefix(containerID, "s0-")
	if len(value) > 12 {
		value = value[:12]
	}
	return "S0-NET-" + value
}

// ValidateNomadProcdAddress requires the canonical allocation-local procd
// origin persisted by the regional runtime-slot registry.
func ValidateNomadProcdAddress(address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" {
		return fmt.Errorf("procd_address must be a canonical HTTP origin")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || port != strconv.Itoa(NomadProcdPort) {
		return fmt.Errorf("procd_address must use the Nomad procd port")
	}
	want, err := NomadProcdAddress(host)
	if err != nil || want != address {
		return fmt.Errorf("procd_address must be canonical")
	}
	return nil
}

// Digest returns the canonical regional command-ready digest.
func (p CommandReadyProof) Digest() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode command-ready proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// State is the region-authoritative lifecycle of one physical warm slot.
type State string

const (
	StateRegistered    State = "registered"
	StateFastpathReady State = "fastpath_ready"
	StateClaiming      State = "claiming"
	StateStarting      State = "starting"
	StateActive        State = "active"
	StateQuiescing     State = "quiescing"
	StateOrphaned      State = "orphaned"
	StateTerminal      State = "terminal"
)

func (s State) Valid() bool {
	switch s {
	case StateRegistered, StateFastpathReady, StateClaiming, StateStarting,
		StateActive, StateQuiescing, StateOrphaned, StateTerminal:
		return true
	default:
		return false
	}
}

// RegistrationRequest contains physical placement identity supplied by the
// trusted node agent. NodeUID is deliberately derived from authentication.
type RegistrationRequest struct {
	ClusterID            string `json:"cluster_id"`
	AllocationID         string `json:"allocation_id"`
	AllocationNamespace  string `json:"allocation_namespace"`
	NodeID               string `json:"node_id"`
	NodeBootID           string `json:"node_boot_id"`
	NetNSIdentity        string `json:"netns_identity"`
	ControlEndpoint      string `json:"control_endpoint"`
	RuntimeCompatibility string `json:"runtime_compatibility"`
}

func (r RegistrationRequest) Validate() error {
	for name, value := range map[string]string{
		"cluster_id": r.ClusterID, "allocation_id": r.AllocationID,
		"allocation_namespace": r.AllocationNamespace, "node_id": r.NodeID,
		"node_boot_id": r.NodeBootID, "netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if err := validateControlEndpoint(r.ControlEndpoint); err != nil {
		return err
	}
	return validateCompatibility(r.RuntimeCompatibility)
}

// ReadinessRequest proves that all claim-independent fast-path resources are
// ready for the exact allocation and node-boot incarnation.
type ReadinessRequest struct {
	AllocationID       string `json:"allocation_id"`
	NodeBootID         string `json:"node_boot_id"`
	RuntimeReadyDigest string `json:"runtime_ready_digest"`
	NetworkReadyDigest string `json:"network_ready_digest"`
	StorageReadyDigest string `json:"storage_ready_digest"`
}

func (r ReadinessRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"runtime_ready_digest": r.RuntimeReadyDigest,
		"network_ready_digest": r.NetworkReadyDigest,
		"storage_ready_digest": r.StorageReadyDigest,
	} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	return nil
}

// HeartbeatRequest extends liveness without changing readiness or claim state.
type HeartbeatRequest struct {
	AllocationID string `json:"allocation_id"`
	NodeBootID   string `json:"node_boot_id"`
}

func (r HeartbeatRequest) Validate() error {
	return validateCaller(r.AllocationID, r.NodeBootID)
}

// StartingRequest records the exact runsc launch and post-claim RootFS/network
// bindings. The regional writer grant must already be consumed.
type StartingRequest struct {
	AllocationID        string `json:"allocation_id"`
	NodeBootID          string `json:"node_boot_id"`
	OperationID         string `json:"operation_id"`
	ClaimID             string `json:"claim_id"`
	LaunchAttempt       string `json:"launch_attempt"`
	RunscContainerID    string `json:"runsc_container_id"`
	RootFSBindingDigest string `json:"rootfs_binding_digest"`
	ClaimNetworkDigest  string `json:"claim_network_digest"`
	ResourceLeaseID     string `json:"resource_lease_id"`
	ResourceLeaseDigest string `json:"resource_lease_digest"`
}

func (r StartingRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID,
		"launch_attempt": r.LaunchAttempt, "runsc_container_id": r.RunscContainerID,
		"resource_lease_id": r.ResourceLeaseID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if _, err := DecodeProof("rootfs_binding_digest", r.RootFSBindingDigest); err != nil {
		return err
	}
	if _, err := DecodeProof("claim_network_digest", r.ClaimNetworkDigest); err != nil {
		return err
	}
	_, err := DecodeProof("resource_lease_digest", r.ResourceLeaseDigest)
	return err
}

// CommandReadyRequest proves that the launched procd instance accepts commands.
type CommandReadyRequest struct {
	AllocationID       string `json:"allocation_id"`
	NodeBootID         string `json:"node_boot_id"`
	OperationID        string `json:"operation_id"`
	ClaimID            string `json:"claim_id"`
	ProcdInstanceID    string `json:"procd_instance_id"`
	ProcdAddress       string `json:"procd_address"`
	CommandReadyDigest string `json:"command_ready_digest"`
}

func (r CommandReadyRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID,
		"procd_instance_id": r.ProcdInstanceID, "procd_address": r.ProcdAddress,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if err := ValidateNomadProcdAddress(r.ProcdAddress); err != nil {
		return err
	}
	_, err := DecodeProof("command_ready_digest", r.CommandReadyDigest)
	return err
}

// Observation is the bounded node-visible projection of one durable slot.
type Observation struct {
	SlotID              string     `json:"slot_id"`
	State               State      `json:"state"`
	Revision            int64      `json:"revision"`
	ServerTime          time.Time  `json:"server_time"`
	HeartbeatExpiresAt  time.Time  `json:"heartbeat_expires_at"`
	ClaimOperationID    string     `json:"claim_operation_id,omitempty"`
	ClaimID             string     `json:"claim_id,omitempty"`
	ClaimLeaseExpiresAt *time.Time `json:"claim_lease_expires_at,omitempty"`
}

const (
	ErrorInvalidArgument    = "invalid_argument"
	ErrorUnauthenticated    = "unauthenticated"
	ErrorPermissionDenied   = "permission_denied"
	ErrorNotFound           = "not_found"
	ErrorConflict           = "conflict"
	ErrorFailedPrecondition = "failed_precondition"
	ErrorUnavailable        = "unavailable"
)

// ErrorResponse provides a stable machine-readable failure class.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e ErrorResponse) Validate() error {
	switch e.Code {
	case ErrorInvalidArgument, ErrorUnauthenticated, ErrorPermissionDenied,
		ErrorNotFound, ErrorConflict, ErrorFailedPrecondition, ErrorUnavailable:
	default:
		return fmt.Errorf("error code is invalid")
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Errorf("error message is required")
	}
	return nil
}

func (o Observation) Validate() error {
	if err := validateRequiredID("slot_id", o.SlotID); err != nil {
		return err
	}
	if !o.State.Valid() {
		return fmt.Errorf("state is invalid")
	}
	if o.Revision <= 0 {
		return fmt.Errorf("revision must be positive")
	}
	if o.ServerTime.IsZero() || o.HeartbeatExpiresAt.IsZero() {
		return fmt.Errorf("server_time and heartbeat_expires_at are required")
	}
	claimed := strings.TrimSpace(o.ClaimOperationID) != "" || strings.TrimSpace(o.ClaimID) != ""
	if claimed {
		if err := validateRequiredID("claim_operation_id", o.ClaimOperationID); err != nil {
			return err
		}
		if err := validateRequiredID("claim_id", o.ClaimID); err != nil {
			return err
		}
		if o.ClaimLeaseExpiresAt == nil || o.ClaimLeaseExpiresAt.IsZero() {
			return fmt.Errorf("claim_lease_expires_at is required for a claimed slot")
		}
	} else if o.ClaimLeaseExpiresAt != nil {
		return fmt.Errorf("claim_lease_expires_at requires a claim binding")
	}
	return nil
}

func SlotPath(slotID string) string {
	return PathPrefix + url.PathEscape(slotID)
}

func ValidateSlotID(slotID string) error {
	return validateRequiredID("slot_id", slotID)
}

func ReadyPath(slotID string) string        { return SlotPath(slotID) + readyPathSuffix }
func HeartbeatPath(slotID string) string    { return SlotPath(slotID) + heartbeatPathSuffix }
func StartingPath(slotID string) string     { return SlotPath(slotID) + startingPathSuffix }
func CommandReadyPath(slotID string) string { return SlotPath(slotID) + commandReadyPathSuffix }

// DecodeProof validates and decodes a canonical lowercase SHA-256 proof.
func DecodeProof(name, value string) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	decoded, err := hex.DecodeString(trimmed)
	if value != trimmed || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != trimmed {
		return nil, fmt.Errorf("%s must be a canonical 32-byte lowercase hexadecimal digest", name)
	}
	return decoded, nil
}

func validateCaller(allocationID, nodeBootID string) error {
	if err := validateRequiredID("allocation_id", allocationID); err != nil {
		return err
	}
	return validateRequiredID("node_boot_id", nodeBootID)
}

func validateRequiredID(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || trimmed == "" || len(value) > 512 {
		return fmt.Errorf("%s is required and must not exceed 512 bytes", name)
	}
	return nil
}

func validateCompatibility(value string) error {
	trimmed := strings.TrimSpace(value)
	parsed, err := digest.Parse(trimmed)
	if value != trimmed || err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return fmt.Errorf("runtime_compatibility must be a canonical sha256 digest")
	}
	return nil
}

func validateControlEndpoint(value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || value == "" || len(value) > 2_048 {
		return fmt.Errorf("control_endpoint is required and must not exceed 2048 bytes")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("control_endpoint is invalid")
	}
	switch parsed.Scheme {
	case "http", "https":
		if parsed.Host == "" {
			return fmt.Errorf("control_endpoint HTTP origin requires a host")
		}
	case "unix":
		if parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") {
			return fmt.Errorf("control_endpoint unix origin requires an absolute path")
		}
	default:
		return fmt.Errorf("control_endpoint scheme must be http, https, or unix")
	}
	return nil
}
