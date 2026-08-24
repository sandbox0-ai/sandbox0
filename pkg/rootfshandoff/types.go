package rootfshandoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const (
	WriterBindingVersion         = 1
	GenerationDescriptorVersion  = 1
	GenerationDescriptorMaxBytes = rootfsblock.MaxDescriptorBytes
	CrashFenceProofVersion       = 1
	RunningForkCheckpointVersion = 1
	CrashFenceHeadKeepInitial    = "keep_initial_generation"

	StateConsuming  = "consuming"
	StateStaged     = "staged"
	StateReserved   = "reserved"
	StateCapturing  = "capturing"
	StateReady      = "ready"
	StatePrepared   = "prepared"
	StateHandedOff  = "handed_off"
	StateRemoving   = "removing"
	StatePoisoned   = "poisoned"
	StateTombstoned = "tombstoned"
)

// Identity binds a one-shot synthetic parent to one runtime
// incarnation. The parent is an authorization capability only when every
// field matches the node-local registry and the regional writer epoch.
type Identity struct {
	NodeUID              string `json:"node_uid"`
	BootID               string `json:"boot_id"`
	RuntimeGeneration    string `json:"runtime_generation"`
	AllocationID         string `json:"allocation_id"`
	NetworkIncarnationID string `json:"network_incarnation_id"`
	TaskName             string `json:"task_name"`
	SourceOCIDigest      string `json:"source_oci_digest"`
	RootFSDriver         string `json:"rootfs_driver"`
	RuntimeClass         string `json:"runtime_class"`
	SlotNonce            string `json:"slot_nonce"`
	ClaimID              string `json:"claim_id"`
	LaunchAttempt        string `json:"launch_attempt"`
	RootFSID             string `json:"rootfs_id"`
	WriterEpoch          int64  `json:"writer_epoch"`
	WriterGrantID        string `json:"writer_grant_id"`
	// WriterGrantTokenDigest is a non-secret issuance nonce. It survives in
	// node-local durable state and prevents a deleted grant row from being
	// confused with a later grant that accidentally reuses semantic IDs.
	WriterGrantTokenDigest string `json:"writer_grant_token_digest"`
	WriterGrantToken       string `json:"writer_grant_token,omitempty"`
}

// StageRequest authorizes one committed runtime slot before the node runtime
// prepares storage. InitialGeneration is immutable once the request succeeds.
type StageRequest struct {
	BindingVersion      int                   `json:"binding_version"`
	Parent              string                `json:"parent"`
	InitialGeneration   string                `json:"initial_generation"`
	Generation          *GenerationDescriptor `json:"generation,omitempty"`
	ExpectedPolicyToken NetworkPolicyToken    `json:"expected_policy_token"`
	Identity            Identity              `json:"identity"`
	Labels              map[string]string     `json:"labels,omitempty"`
}

// GenerationDescriptor is the bounded immutable control-plane description
// needed by the node storage runtime to attach one durable block-map head. It
// contains locators and checksums, never regional S3 or PostgreSQL credentials.
type GenerationDescriptor struct {
	Version            int    `json:"version"`
	GenerationID       string `json:"generation_id"`
	FilesystemID       string `json:"filesystem_id"`
	SourceOCIDigest    string `json:"source_oci_digest"`
	BaseArtifactDigest string `json:"base_artifact_digest"`
	BaseBlockRoot      string `json:"base_block_root"`
	CurrentBlockHead   string `json:"current_block_head"`
	WriterEpoch        int64  `json:"writer_epoch"`
	FormatGeneration   int    `json:"format_generation"`
	DurabilityState    string `json:"durability_state"`
	LocatorVersion     int64  `json:"locator_version"`
	Descriptor         []byte `json:"descriptor"`
}

func (d GenerationDescriptor) Validate() error {
	if d.Version != GenerationDescriptorVersion {
		return fmt.Errorf("unsupported generation descriptor version %d", d.Version)
	}
	for name, value := range map[string]string{
		"generation_id": d.GenerationID, "filesystem_id": d.FilesystemID,
		"source_oci_digest": d.SourceOCIDigest, "base_artifact_digest": d.BaseArtifactDigest,
		"base_block_root": d.BaseBlockRoot, "current_block_head": d.CurrentBlockHead,
		"durability_state": d.DurabilityState,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"source_oci_digest": d.SourceOCIDigest, "base_artifact_digest": d.BaseArtifactDigest,
		"base_block_root": d.BaseBlockRoot, "current_block_head": d.CurrentBlockHead,
	} {
		if err := validateSHA256Digest(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if d.WriterEpoch < 0 || d.FormatGeneration <= 0 || d.LocatorVersion <= 0 {
		return fmt.Errorf("writer_epoch, format_generation, and locator_version are invalid")
	}
	if d.DurabilityState != "composite_durable" && d.DurabilityState != "s3_materialized" {
		return fmt.Errorf("generation is not region durable")
	}
	if len(d.Descriptor) == 0 || len(d.Descriptor) > GenerationDescriptorMaxBytes {
		return fmt.Errorf("descriptor must contain 1..%d bytes", GenerationDescriptorMaxBytes)
	}
	blockDescriptor, err := rootfsblock.DecodeDescriptor(d.Descriptor)
	if err != nil {
		return fmt.Errorf("block descriptor: %w", err)
	}
	if blockDescriptor.MappingRoot.RootDigest != d.CurrentBlockHead {
		return fmt.Errorf("block descriptor mapping root does not match current_block_head")
	}
	if d.DurabilityState == "s3_materialized" && blockDescriptor.CompositeTail != nil ||
		d.DurabilityState == "composite_durable" && blockDescriptor.CompositeTail == nil {
		return fmt.Errorf("block descriptor tail does not match durability_state")
	}
	return nil
}

// NetworkPolicyToken identifies the exact network incarnation for which ctld
// has installed policy. A policy digest alone is vulnerable to ABA after a
// claim, network incarnation, source IP, or ctld generation changes.
type NetworkPolicyToken struct {
	AllocationID         string `json:"allocation_id"`
	NetworkIncarnationID string `json:"network_incarnation_id"`
	ClaimID              string `json:"claim_id"`
	NetworkEpoch         int64  `json:"network_epoch"`
	PolicyDigest         string `json:"policy_digest"`
	SourceIP             string `json:"source_ip"`
	CtldGeneration       string `json:"ctld_generation"`
	NetNSIdentity        string `json:"netns_identity"`
}

// ReadyRequest resolves the staged logical generation to a protected host
// mount. Prepare may wait for this transition, but it never returns a
// placeholder root.
type ReadyRequest struct {
	Parent             string             `json:"parent"`
	Source             string             `json:"source"`
	Type               string             `json:"type,omitempty"`
	Options            []string           `json:"options,omitempty"`
	AppliedPolicyToken NetworkPolicyToken `json:"applied_policy_token"`
}

// ConsumerRequest asks the rootfs runtime to bind a prepared backend key to the
// exact runtime consumer record after the runtime has committed it. Some
// runtime paths consume Prepare mounts without a later backend Mounts call,
// so lifecycle code must not depend on that callback to record the handoff.
type ConsumerRequest struct {
	Parent string `json:"parent"`
}

func (r ConsumerRequest) Validate() error {
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	return nil
}

type ParentStatus struct {
	StageRequest
	State                     string                     `json:"state"`
	ActiveKey                 string                     `json:"active_key,omitempty"`
	Mount                     *Mount                     `json:"mount,omitempty"`
	WriterLease               *protocol.LeaseObservation `json:"writer_lease,omitempty"`
	WriterAuthorityDegradedAt string                     `json:"writer_authority_degraded_at,omitempty"`
	WriterAuthorityLastError  string                     `json:"writer_authority_last_error,omitempty"`
	CreatedAt                 string                     `json:"created_at"`
	UpdatedAt                 string                     `json:"updated_at"`
}

// RetireRequest identifies one planned writer retirement. OperationID is
// stable across Manager, ctld, the rootfs runtime, and PostgreSQL retries.
type RetireRequest struct {
	Parent      string `json:"parent"`
	OperationID string `json:"operation_id"`
}

// PlannedRetireOperationID returns the stable regional lifecycle identity for
// one exact writer incarnation. Both the control plane and node runtime use
// this value so a pause intent can exist before Nomad asks the driver to stop.
func PlannedRetireOperationID(parent, writerGrantID string, writerEpoch int64) string {
	payload := fmt.Sprintf("%s\x00%s\x00%d", parent, writerGrantID, writerEpoch)
	sum := sha256.Sum256([]byte(payload))
	return "nomad-retire-" + hex.EncodeToString(sum[:16])
}

func (r RetireRequest) Validate() error {
	if strings.TrimSpace(r.Parent) == "" || strings.TrimSpace(r.OperationID) == "" {
		return fmt.Errorf("parent and operation_id are required")
	}
	return nil
}

// RetireResult is returned only after the Task-facing mount, physical device,
// and writable branch session have been revoked and the branch is sealed.
type RetireResult struct {
	Parent           string `json:"parent"`
	RootFSID         string `json:"rootfs_id"`
	WriterEpoch      int64  `json:"writer_epoch"`
	OperationID      string `json:"operation_id"`
	CurrentBlockHead string `json:"current_block_head"`
	DurabilityState  string `json:"durability_state"`
	Descriptor       []byte `json:"descriptor"`
	DetachProof      string `json:"detach_proof"`
}

// CrashFenceRequest identifies one non-cooperative writer revocation. The
// operation is allocated regionally before this request reaches the node and
// must remain stable across every retry.
type CrashFenceRequest struct {
	Parent      string `json:"parent"`
	OperationID string `json:"operation_id"`
}

func (r CrashFenceRequest) Validate() error {
	if strings.TrimSpace(r.Parent) == "" || strings.TrimSpace(r.OperationID) == "" {
		return fmt.Errorf("parent and operation_id are required")
	}
	return nil
}

// CrashFenceSessionObservation is produced by the physical session owner
// after it has durably tombstoned the exact writer and re-inspected host mount
// and NBD state. Boolean absence fields are intentionally explicit so a zero
// value can never be mistaken for a successful observation.
type CrashFenceSessionObservation struct {
	Parent            string   `json:"parent"`
	RootFSID          string   `json:"rootfs_id"`
	WriterEpoch       int64    `json:"writer_epoch"`
	OperationID       string   `json:"operation_id"`
	BindingDigest     string   `json:"binding_digest"`
	SessionState      string   `json:"session_state"`
	BranchPath        string   `json:"branch_path"`
	DeviceBound       bool     `json:"device_bound"`
	DevicePath        string   `json:"device_path,omitempty"`
	NBDPoolAbsent     bool     `json:"nbd_pool_absent,omitempty"`
	NBDPID            int      `json:"nbd_pid"`
	NBDHolders        []string `json:"nbd_holders,omitempty"`
	LiveSessionAbsent bool     `json:"live_session_absent"`
	MergedMountAbsent bool     `json:"merged_mount_absent"`
	XFSMountAbsent    bool     `json:"xfs_mount_absent"`
	ObservedAt        string   `json:"observed_at"`
}

func (o CrashFenceSessionObservation) Validate() error {
	for name, value := range map[string]string{
		"parent": o.Parent, "rootfs_id": o.RootFSID, "operation_id": o.OperationID,
		"binding_digest": o.BindingDigest, "session_state": o.SessionState,
		"branch_path": o.BranchPath, "observed_at": o.ObservedAt,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if o.WriterEpoch <= 0 {
		return fmt.Errorf("writer_epoch must be positive")
	}
	binding, err := hex.DecodeString(o.BindingDigest)
	if err != nil || len(binding) != sha256.Size || hex.EncodeToString(binding) != o.BindingDigest {
		return fmt.Errorf("binding_digest must be a canonical 32-byte hexadecimal digest")
	}
	if o.SessionState != StateTombstoned || !o.LiveSessionAbsent || !o.MergedMountAbsent || !o.XFSMountAbsent {
		return fmt.Errorf("physical writer is not fully detached")
	}
	if o.DeviceBound && strings.TrimSpace(o.DevicePath) == "" {
		return fmt.Errorf("device_path is required for a bound device")
	}
	if !o.DeviceBound && (strings.TrimSpace(o.DevicePath) != "" || !o.NBDPoolAbsent) {
		return fmt.Errorf("unbound device proof must establish an empty NBD pool")
	}
	if o.NBDPID != 0 || len(o.NBDHolders) != 0 {
		return fmt.Errorf("NBD device is still owned")
	}
	return nil
}

// CrashFenceProof is the canonical node proof for abandoning one unsealed
// writer without advancing its durable generation. It contains no caller-
// supplied liveness facts.
type CrashFenceProof struct {
	Version                int                          `json:"version"`
	OperationID            string                       `json:"operation_id"`
	Parent                 string                       `json:"parent"`
	ClaimID                string                       `json:"claim_id"`
	WriterGrantID          string                       `json:"writer_grant_id"`
	WriterEpoch            int64                        `json:"writer_epoch"`
	BindingVersion         int                          `json:"binding_version"`
	BindingDigest          string                       `json:"binding_digest"`
	RootFSID               string                       `json:"rootfs_id"`
	InitialGeneration      string                       `json:"initial_generation"`
	InitialBlockHead       string                       `json:"initial_block_head"`
	HeadAction             string                       `json:"head_action"`
	NodeUID                string                       `json:"node_uid"`
	BootID                 string                       `json:"boot_id"`
	RuntimeGeneration      string                       `json:"runtime_generation"`
	HostMountNamespaceID   string                       `json:"host_mount_namespace_id"`
	AllocationID           string                       `json:"allocation_id"`
	NetworkIncarnationID   string                       `json:"network_incarnation_id"`
	TaskName               string                       `json:"task_name"`
	SlotNonce              string                       `json:"slot_nonce"`
	ActiveKey              string                       `json:"active_key"`
	ConsumerBound          bool                         `json:"consumer_bound"`
	ContainerID            string                       `json:"container_id,omitempty"`
	ContainerAbsent        bool                         `json:"container_absent"`
	TaskAbsent             bool                         `json:"task_absent"`
	FrontendSnapshotAbsent bool                         `json:"frontend_snapshot_absent"`
	StableMountAbsent      bool                         `json:"stable_mount_absent"`
	RootFSState            string                       `json:"rootfs_state"`
	Session                CrashFenceSessionObservation `json:"session"`
	ObservedAt             string                       `json:"observed_at"`
}

func (p CrashFenceProof) Validate() error {
	if p.Version != CrashFenceProofVersion {
		return fmt.Errorf("unsupported crash fence proof version %d", p.Version)
	}
	for name, value := range map[string]string{
		"operation_id": p.OperationID, "parent": p.Parent, "claim_id": p.ClaimID,
		"writer_grant_id": p.WriterGrantID, "binding_digest": p.BindingDigest,
		"rootfs_id": p.RootFSID, "initial_generation": p.InitialGeneration,
		"initial_block_head": p.InitialBlockHead, "node_uid": p.NodeUID, "boot_id": p.BootID,
		"runtime_generation": p.RuntimeGeneration, "host_mount_namespace_id": p.HostMountNamespaceID,
		"allocation_id": p.AllocationID, "network_incarnation_id": p.NetworkIncarnationID, "task_name": p.TaskName,
		"slot_nonce": p.SlotNonce, "active_key": p.ActiveKey,
		"rootfs_state": p.RootFSState, "observed_at": p.ObservedAt,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if p.ConsumerBound && strings.TrimSpace(p.ContainerID) == "" {
		return fmt.Errorf("container_id is required for a bound consumer")
	}
	if !p.ConsumerBound && strings.TrimSpace(p.ContainerID) != "" {
		return fmt.Errorf("container_id must be empty for an unbound consumer")
	}
	if p.WriterEpoch <= 0 || p.BindingVersion != WriterBindingVersion {
		return fmt.Errorf("writer_epoch and binding_version are invalid")
	}
	binding, err := hex.DecodeString(p.BindingDigest)
	if err != nil || len(binding) != sha256.Size || hex.EncodeToString(binding) != p.BindingDigest {
		return fmt.Errorf("binding_digest must be a canonical 32-byte hexadecimal digest")
	}
	if err := validateSHA256Digest(p.InitialBlockHead); err != nil {
		return fmt.Errorf("initial_block_head: %w", err)
	}
	if p.HeadAction != CrashFenceHeadKeepInitial || p.RootFSState != StateTombstoned ||
		!p.ContainerAbsent || !p.TaskAbsent || !p.FrontendSnapshotAbsent || !p.StableMountAbsent {
		return fmt.Errorf("crash fence proof does not establish terminal writer absence")
	}
	if err := p.Session.Validate(); err != nil {
		return fmt.Errorf("session: %w", err)
	}
	if p.Session.Parent != p.Parent || p.Session.RootFSID != p.RootFSID ||
		p.Session.WriterEpoch != p.WriterEpoch || p.Session.OperationID != p.OperationID ||
		p.Session.BindingDigest != p.BindingDigest {
		return fmt.Errorf("session observation does not match the writer binding")
	}
	return nil
}

func (p CrashFenceProof) Digest() ([sha256.Size]byte, error) {
	if err := p.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

type CrashFenceResult struct {
	Proof       CrashFenceProof `json:"proof"`
	ProofDigest string          `json:"proof_digest"`
}

// RunningForkCheckpointProof binds one briefly frozen branch boundary to the
// regional target generation. The descriptor digest remains stable evidence
// after an asynchronous materializer advances the generation locator.
type RunningForkCheckpointProof struct {
	Version                    int    `json:"version"`
	OperationID                string `json:"operation_id"`
	SourceSandboxID            string `json:"source_sandbox_id"`
	SourceFilesystemID         string `json:"source_filesystem_id"`
	TargetSandboxID            string `json:"target_sandbox_id"`
	SourceWriterGrantID        string `json:"source_writer_grant_id"`
	SourceWriterEpoch          int64  `json:"source_writer_epoch"`
	BindingVersion             int    `json:"binding_version"`
	BindingDigest              string `json:"binding_digest"`
	ExpectedSourceGenerationID string `json:"expected_source_generation_id"`
	CheckpointGenerationID     string `json:"checkpoint_generation_id"`
	CheckpointSequence         uint64 `json:"checkpoint_sequence"`
	CheckpointDescriptorDigest string `json:"checkpoint_descriptor_digest"`
}

// RunningForkCheckpointRequest names the regional target before the node
// freezes the source filesystem. IDs are stable across retries.
type RunningForkCheckpointRequest struct {
	OperationID        string `json:"operation_id"`
	SourceSandboxID    string `json:"source_sandbox_id"`
	TargetSandboxID    string `json:"target_sandbox_id"`
	TargetGenerationID string `json:"target_generation_id"`
}

func (r RunningForkCheckpointRequest) Validate() error {
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "source_sandbox_id": r.SourceSandboxID,
		"target_sandbox_id": r.TargetSandboxID, "target_generation_id": r.TargetGenerationID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
		if strings.TrimSpace(value) != value {
			return fmt.Errorf("%s must use canonical whitespace-free encoding", name)
		}
	}
	if r.SourceSandboxID == r.TargetSandboxID {
		return fmt.Errorf("source and target sandboxes must differ")
	}
	return nil
}

// RunningForkCheckpointResult contains bounded regional control metadata. Its
// block payload is either a PostgreSQL composite tail or immutable objects.
type RunningForkCheckpointResult struct {
	Generation  GenerationDescriptor       `json:"generation"`
	Proof       RunningForkCheckpointProof `json:"proof"`
	ProofDigest string                     `json:"proof_digest"`
}

func (r RunningForkCheckpointResult) Validate() error {
	if err := r.Generation.Validate(); err != nil {
		return fmt.Errorf("generation: %w", err)
	}
	proofDigest, err := r.Proof.Digest()
	if err != nil {
		return fmt.Errorf("proof: %w", err)
	}
	if r.ProofDigest != hex.EncodeToString(proofDigest[:]) {
		return fmt.Errorf("proof_digest does not match proof")
	}
	if r.Proof.CheckpointGenerationID != r.Generation.GenerationID ||
		r.Proof.TargetSandboxID != r.Generation.FilesystemID ||
		r.Proof.SourceWriterEpoch != r.Generation.WriterEpoch ||
		r.Proof.CheckpointDescriptorDigest != digest.FromBytes(r.Generation.Descriptor).String() {
		return fmt.Errorf("proof does not match checkpoint generation")
	}
	return nil
}

func (p RunningForkCheckpointProof) Validate() error {
	if p.Version != RunningForkCheckpointVersion {
		return fmt.Errorf("unsupported running fork checkpoint proof version %d", p.Version)
	}
	for name, value := range map[string]string{
		"operation_id": p.OperationID, "source_sandbox_id": p.SourceSandboxID,
		"source_filesystem_id": p.SourceFilesystemID, "target_sandbox_id": p.TargetSandboxID,
		"source_writer_grant_id":        p.SourceWriterGrantID,
		"expected_source_generation_id": p.ExpectedSourceGenerationID,
		"checkpoint_generation_id":      p.CheckpointGenerationID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if p.SourceSandboxID == p.TargetSandboxID {
		return fmt.Errorf("source and target sandboxes must differ")
	}
	if p.SourceWriterEpoch <= 0 || p.BindingVersion != WriterBindingVersion || p.CheckpointSequence > math.MaxInt64 {
		return fmt.Errorf("writer epoch, binding version, or checkpoint sequence is invalid")
	}
	binding, err := hex.DecodeString(p.BindingDigest)
	if err != nil || len(binding) != sha256.Size || hex.EncodeToString(binding) != p.BindingDigest {
		return fmt.Errorf("binding_digest must be a canonical 32-byte hexadecimal digest")
	}
	if err := validateSHA256Digest(p.CheckpointDescriptorDigest); err != nil {
		return fmt.Errorf("checkpoint_descriptor_digest: %w", err)
	}
	return nil
}

func (p RunningForkCheckpointProof) Digest() ([sha256.Size]byte, error) {
	if err := p.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

func (r CrashFenceResult) Validate() error {
	digest, err := r.Proof.Digest()
	if err != nil {
		return err
	}
	if r.ProofDigest != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("proof_digest does not match proof")
	}
	return nil
}

func (r RetireResult) Validate() error {
	if strings.TrimSpace(r.Parent) == "" || strings.TrimSpace(r.RootFSID) == "" ||
		r.WriterEpoch <= 0 || strings.TrimSpace(r.OperationID) == "" ||
		strings.TrimSpace(r.CurrentBlockHead) == "" || strings.TrimSpace(r.DurabilityState) == "" ||
		len(r.Descriptor) == 0 {
		return fmt.Errorf("retire result fields are incomplete")
	}
	proof, err := hex.DecodeString(strings.TrimSpace(r.DetachProof))
	if err != nil || len(proof) != sha256.Size || hex.EncodeToString(proof) != strings.TrimSpace(r.DetachProof) {
		return fmt.Errorf("detach_proof must be a canonical 32-byte hexadecimal digest")
	}
	descriptor, err := rootfsblock.DecodeDescriptor(r.Descriptor)
	if err != nil {
		return fmt.Errorf("descriptor: %w", err)
	}
	if descriptor.MappingRoot.RootDigest != r.CurrentBlockHead {
		return fmt.Errorf("current_block_head does not match descriptor mapping root")
	}
	switch r.DurabilityState {
	case rootfsblock.DurabilityS3:
		if descriptor.CompositeTail != nil {
			return fmt.Errorf("s3_materialized result cannot contain a composite tail")
		}
	case rootfsblock.DurabilityComposite:
		if descriptor.CompositeTail == nil {
			return fmt.Errorf("composite_durable result requires a composite tail")
		}
	default:
		return fmt.Errorf("unsupported durability_state %q", r.DurabilityState)
	}
	return nil
}

type GateRequest struct {
	SlotNonce string `json:"slot_nonce"`
}

type GateImage struct {
	SlotNonce       string `json:"slot_nonce"`
	SourceOCIDigest string `json:"source_oci_digest"`
	Manifest        string `json:"manifest_digest"`
	Config          string `json:"config_digest"`
	Layer           string `json:"layer_digest"`
	DiffID          string `json:"diff_id"`
	Parent          string `json:"parent_chain_id"`
	LeaseID         string `json:"lease_id"`
}

// RuntimeIncarnation is the host rootfs runtime's admitted node/runtime identity.
// Ctld obtains this fact over the root-only local socket rather than trusting
// a Manager request or duplicating node bootstrap configuration.
type RuntimeIncarnation struct {
	NodeUID              string `json:"node_uid"`
	BootID               string `json:"boot_id"`
	RuntimeGeneration    string `json:"runtime_generation"`
	HostMountNamespaceID string `json:"host_mount_namespace_id"`
	AdmissionReady       bool   `json:"admission_ready"`
	WriterRenewalReady   bool   `json:"writer_renewal_ready"`
}

func (i RuntimeIncarnation) Validate() error {
	for name, value := range map[string]string{
		"node_uid": i.NodeUID, "boot_id": i.BootID, "runtime_generation": i.RuntimeGeneration,
		"host_mount_namespace_id": i.HostMountNamespaceID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

type WriterGrantBatchRenewRequest struct {
	Items []StageRequest `json:"items"`
}

type Mount struct {
	Type       string   `json:"type"`
	Source     string   `json:"source"`
	Options    []string `json:"options,omitempty"`
	Device     uint64   `json:"device"`
	Inode      uint64   `json:"inode"`
	MountID    uint64   `json:"mount_id"`
	Generation string   `json:"generation"`
}

func (r StageRequest) Validate() error {
	return r.validate(true)
}

// ValidateDurableBinding validates the immutable Stage binding after the raw
// writer token has been deliberately removed from node-local durable state.
func (r StageRequest) ValidateDurableBinding() error {
	return r.validate(false)
}

func (r StageRequest) validate(requireWriterToken bool) error {
	if r.BindingVersion != WriterBindingVersion {
		return fmt.Errorf("unsupported binding_version %d", r.BindingVersion)
	}
	required := map[string]string{
		"parent":                    r.Parent,
		"initial_generation":        r.InitialGeneration,
		"node_uid":                  r.Identity.NodeUID,
		"boot_id":                   r.Identity.BootID,
		"runtime_generation":        r.Identity.RuntimeGeneration,
		"allocation_id":             r.Identity.AllocationID,
		"network_incarnation_id":    r.Identity.NetworkIncarnationID,
		"task_name":                 r.Identity.TaskName,
		"source_oci_digest":         r.Identity.SourceOCIDigest,
		"rootfs_driver":             r.Identity.RootFSDriver,
		"runtime_class":             r.Identity.RuntimeClass,
		"slot_nonce":                r.Identity.SlotNonce,
		"claim_id":                  r.Identity.ClaimID,
		"launch_attempt":            r.Identity.LaunchAttempt,
		"rootfs_id":                 r.Identity.RootFSID,
		"writer_grant_id":           r.Identity.WriterGrantID,
		"writer_grant_token_digest": r.Identity.WriterGrantTokenDigest,
	}
	if requireWriterToken {
		required["writer_grant_token"] = r.Identity.WriterGrantToken
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if r.Identity.WriterEpoch <= 0 {
		return fmt.Errorf("writer_epoch must be positive")
	}
	if r.Generation != nil {
		if err := r.Generation.Validate(); err != nil {
			return fmt.Errorf("generation: %w", err)
		}
		if r.Generation.GenerationID != r.InitialGeneration ||
			r.Generation.FilesystemID != r.Identity.RootFSID ||
			r.Generation.WriterEpoch >= r.Identity.WriterEpoch {
			return fmt.Errorf("generation does not match RootFS identity and writer epoch")
		}
	}
	digestBytes, err := hex.DecodeString(r.Identity.WriterGrantTokenDigest)
	if err != nil || len(digestBytes) != sha256.Size {
		return fmt.Errorf("writer_grant_token_digest must be a 32-byte lowercase hexadecimal SHA-256 digest")
	}
	if hex.EncodeToString(digestBytes) != r.Identity.WriterGrantTokenDigest {
		return fmt.Errorf("writer_grant_token_digest must use canonical lowercase hexadecimal encoding")
	}
	if requireWriterToken && WriterGrantTokenDigest(r.Identity.WriterGrantToken) != r.Identity.WriterGrantTokenDigest {
		return fmt.Errorf("writer_grant_token_digest does not match writer_grant_token")
	}
	if err := r.ExpectedPolicyToken.Validate(); err != nil {
		return fmt.Errorf("expected_policy_token: %w", err)
	}
	if r.ExpectedPolicyToken.AllocationID != r.Identity.AllocationID ||
		r.ExpectedPolicyToken.ClaimID != r.Identity.ClaimID ||
		r.ExpectedPolicyToken.NetworkIncarnationID != "" && r.ExpectedPolicyToken.NetworkIncarnationID != r.Identity.NetworkIncarnationID {
		return fmt.Errorf("expected_policy_token does not match RootFS identity")
	}
	if err := validateSHA256Digest(r.Parent); err != nil {
		return fmt.Errorf("parent must be a valid chain ID: %w", err)
	}
	return nil
}

func validateSHA256Digest(value string) error {
	parsed, err := digest.Parse(value)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return fmt.Errorf("must be a canonical sha256 digest")
	}
	return nil
}

// WriterGrantTokenDigest derives the non-secret issuance nonce stored with a
// durable handoff. The raw token remains an online Consume-only credential.
func WriterGrantTokenDigest(rawToken string) string {
	sum := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(sum[:])
}

// BindingDigest returns the canonical digest consumed by the regional writer
// authority. The bearer token is deliberately excluded so the same immutable
// binding can be retried without persisting the token in node-local state.
func (r StageRequest) BindingDigest() ([sha256.Size]byte, error) {
	r.Identity.WriterGrantToken = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode writer binding: %w", err)
	}
	return sha256.Sum256(payload), nil
}

// WithoutWriterGrantToken returns the durable form of a Stage request.
func (r StageRequest) WithoutWriterGrantToken() StageRequest {
	r.Identity.WriterGrantToken = ""
	return r
}

func (r ReadyRequest) Normalize() (ReadyRequest, error) {
	r.Parent = strings.TrimSpace(r.Parent)
	r.Source = strings.TrimSpace(r.Source)
	r.Type = strings.TrimSpace(r.Type)
	if r.Parent == "" {
		return ReadyRequest{}, fmt.Errorf("parent is required")
	}
	if err := r.AppliedPolicyToken.Validate(); err != nil {
		return ReadyRequest{}, fmt.Errorf("applied_policy_token: %w", err)
	}
	if r.Source == "" || !filepath.IsAbs(r.Source) {
		return ReadyRequest{}, fmt.Errorf("source must be an absolute path")
	}
	if r.Type == "" {
		r.Type = "bind"
	}
	if r.Type != "bind" {
		return ReadyRequest{}, fmt.Errorf("only bind mounts are supported")
	}
	if len(r.Options) == 0 {
		r.Options = []string{"rbind", "rw", "nosuid", "nodev"}
	}
	if !slices.Contains(r.Options, "bind") && !slices.Contains(r.Options, "rbind") {
		return ReadyRequest{}, fmt.Errorf("bind or rbind option is required")
	}
	allowed := map[string]struct{}{
		"bind": {}, "rbind": {}, "rw": {}, "nosuid": {}, "nodev": {}, "noatime": {},
	}
	for _, option := range r.Options {
		if _, ok := allowed[option]; !ok {
			return ReadyRequest{}, fmt.Errorf("mount option %q is not allowed", option)
		}
	}
	if !slices.Contains(r.Options, "rw") {
		return ReadyRequest{}, fmt.Errorf("rw option is required")
	}
	if !slices.Contains(r.Options, "nosuid") || !slices.Contains(r.Options, "nodev") {
		return ReadyRequest{}, fmt.Errorf("nosuid and nodev options are required")
	}
	return r, nil
}

func (t NetworkPolicyToken) Validate() error {
	required := map[string]string{
		"allocation_id": t.AllocationID, "claim_id": t.ClaimID, "policy_digest": t.PolicyDigest,
		"network_incarnation_id": t.NetworkIncarnationID, "source_ip": t.SourceIP,
		"ctld_generation": t.CtldGeneration, "netns_identity": t.NetNSIdentity,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if t.NetworkEpoch <= 0 {
		return fmt.Errorf("network_epoch must be positive")
	}
	return nil
}
