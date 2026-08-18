// Package rootfsclaim defines the authenticated Manager-to-ctld protocol for
// the stock Kubernetes running-marker RootFS handoff.
package rootfsclaim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const APIPathPrefix = "/internal/v1/rootfs-claims/"

// PodIdentity identifies one immutable warm-slot Pod incarnation. NodeUID is
// part of the request so ctld can reject a stale Manager routing decision.
type PodIdentity struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	NodeUID   string `json:"node_uid"`
	SlotNonce string `json:"slot_nonce"`
}

func (p PodIdentity) Validate() error {
	required := map[string]string{
		"namespace":  p.Namespace,
		"name":       p.Name,
		"uid":        p.UID,
		"node_uid":   p.NodeUID,
		"slot_nonce": p.SlotNonce,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

// RuntimeIncarnation is observed by ctld and the host Snapshotter. Manager
// never supplies these values as authorization facts.
type RuntimeIncarnation struct {
	NodeUID           string `json:"node_uid"`
	BootID            string `json:"boot_id"`
	RuntimeGeneration string `json:"runtime_generation"`
	CtldGeneration    string `json:"ctld_generation"`
}

func (i RuntimeIncarnation) Validate() error {
	for name, value := range map[string]string{
		"node_uid": i.NodeUID, "boot_id": i.BootID,
		"runtime_generation": i.RuntimeGeneration, "ctld_generation": i.CtldGeneration,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

type CreateGateRequest struct {
	Pod PodIdentity `json:"pod"`
}

func (r CreateGateRequest) Validate() error { return r.Pod.Validate() }

// DeleteGateRequest removes an unconsumed one-shot gate while its warm-slot
// Pod identity is still live and locally verifiable.
type DeleteGateRequest struct {
	Pod PodIdentity `json:"pod"`
}

func (r DeleteGateRequest) Validate() error { return r.Pod.Validate() }

type CreateGateResponse struct {
	Gate        rootfshandoff.GateImage `json:"gate"`
	Incarnation RuntimeIncarnation      `json:"incarnation"`
}

func (r CreateGateResponse) Validate() error {
	if err := r.Incarnation.Validate(); err != nil {
		return fmt.Errorf("incarnation: %w", err)
	}
	if strings.TrimSpace(r.Gate.SlotNonce) == "" || strings.TrimSpace(r.Gate.Parent) == "" ||
		strings.TrimSpace(r.Gate.Image) == "" || strings.TrimSpace(r.Gate.Config) == "" {
		return fmt.Errorf("complete gate image identity is required")
	}
	return nil
}

// PreflightRequest asks ctld to derive the current CRI and network identity.
// ExpectedImage is the slot-specific synthetic B image already prepared for
// this Pod; it is compared with the durable gate registry.
type PreflightRequest struct {
	Pod               PodIdentity `json:"pod"`
	ClaimID           string      `json:"claim_id"`
	LaunchAttempt     string      `json:"launch_attempt"`
	ContainerName     string      `json:"container_name"`
	ExpectedAImage    string      `json:"expected_a_image"`
	ExpectedBImage    string      `json:"expected_b_image"`
	PolicyDigest      string      `json:"policy_digest"`
	WriterEpoch       int64       `json:"writer_epoch"`
	RootFSID          string      `json:"rootfs_id"`
	InitialGeneration string      `json:"initial_generation"`
}

func (r PreflightRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	for name, value := range map[string]string{
		"claim_id": r.ClaimID, "launch_attempt": r.LaunchAttempt, "container_name": r.ContainerName,
		"expected_a_image": r.ExpectedAImage, "expected_b_image": r.ExpectedBImage,
		"policy_digest": r.PolicyDigest, "rootfs_id": r.RootFSID,
		"initial_generation": r.InitialGeneration,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if r.WriterEpoch <= 0 {
		return fmt.Errorf("writer_epoch must be positive")
	}
	return nil
}

// PreflightResponse contains only node-observed facts. The writer grant fields
// are added by Manager after the regional Issue transaction succeeds.
type PreflightResponse struct {
	Parent              string                           `json:"parent"`
	Identity            rootfshandoff.Identity           `json:"identity"`
	ExpectedPolicyToken rootfshandoff.NetworkPolicyToken `json:"expected_policy_token"`
	Incarnation         RuntimeIncarnation               `json:"incarnation"`
}

func (r PreflightResponse) Validate() error {
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	if err := r.ExpectedPolicyToken.Validate(); err != nil {
		return fmt.Errorf("expected_policy_token: %w", err)
	}
	if err := r.Incarnation.Validate(); err != nil {
		return fmt.Errorf("incarnation: %w", err)
	}
	return nil
}

// StageRequest carries the one-time regional grant to ctld. Ctld revalidates
// the Pod, gate and runtime incarnation before forwarding it to the host-local
// Snapshotter. This is the only request that transports the raw writer token.
type StageRequest struct {
	Pod     PodIdentity                `json:"pod"`
	Handoff rootfshandoff.StageRequest `json:"handoff"`
}

func (r StageRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	if err := r.Handoff.Validate(); err != nil {
		return fmt.Errorf("handoff: %w", err)
	}
	if r.Pod.UID != r.Handoff.Identity.PodUID || r.Pod.NodeUID != r.Handoff.Identity.NodeUID ||
		r.Pod.SlotNonce != r.Handoff.Identity.SlotNonce {
		return fmt.Errorf("pod identity does not match handoff identity")
	}
	return nil
}

// ReadyRequest asks ctld to reconcile local storage and policy state. It
// intentionally contains neither a host Source path nor an applied policy
// token; both facts must be obtained by ctld from local owners.
type ReadyRequest struct {
	Pod    PodIdentity `json:"pod"`
	Parent string      `json:"parent"`
}

func (r ReadyRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	return nil
}

type StatusResponse struct {
	Parent rootfshandoff.ParentStatus `json:"parent"`
}

// VerifyConsumerRequest closes the durable D_ROOT_HANDED_OFF fact after stock
// containerd has committed the exact B container. It carries no runtime key or
// mount path; ctld and the host Snapshotter derive those facts locally.
type VerifyConsumerRequest struct {
	Pod     PodIdentity `json:"pod"`
	Parent  string      `json:"parent"`
	ClaimID string      `json:"claim_id"`
}

func (r VerifyConsumerRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	if strings.TrimSpace(r.ClaimID) == "" {
		return fmt.Errorf("claim_id is required")
	}
	return nil
}

// PlannedRetireRequest identifies one cooperative retirement of an accepted B
// container. Manager supplies only stable logical identity; ctld and the host
// Snapshotter derive all runtime, mount, device, and seal facts locally.
type PlannedRetireRequest struct {
	Pod         PodIdentity `json:"pod"`
	Parent      string      `json:"parent"`
	ClaimID     string      `json:"claim_id"`
	OperationID string      `json:"operation_id"`
}

func (r PlannedRetireRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	for name, value := range map[string]string{
		"parent": r.Parent, "claim_id": r.ClaimID, "operation_id": r.OperationID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

// PlannedRetireResponse is produced only after Kubernetes has removed the B
// Task and the host Snapshotter has revoked and sealed the physical writer.
type PlannedRetireResponse struct {
	Result rootfshandoff.RetireResult `json:"result"`
}

func (r PlannedRetireResponse) Validate() error {
	return r.Result.Validate()
}

// CrashFenceRequest asks the node to attest an already-regionally-selected
// crash-abandon operation. Ctld derives all container, mount, and device facts
// locally; callers cannot supply absence observations.
type CrashFenceRequest struct {
	Pod         PodIdentity `json:"pod"`
	Parent      string      `json:"parent"`
	ClaimID     string      `json:"claim_id"`
	OperationID string      `json:"operation_id"`
}

func (r CrashFenceRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	for name, value := range map[string]string{
		"parent": r.Parent, "claim_id": r.ClaimID, "operation_id": r.OperationID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

type CrashFenceResponse struct {
	Result rootfshandoff.CrashFenceResult `json:"result"`
}

func (r CrashFenceResponse) Validate() error { return r.Result.Validate() }

// PrelaunchAbortRequest asks ctld to revoke a consumed handoff before the
// atomic A-to-B Pod transition has committed. It intentionally carries no
// caller-supplied cleanup or runtime facts.
type PrelaunchAbortRequest struct {
	Pod            PodIdentity `json:"pod"`
	Parent         string      `json:"parent"`
	ClaimID        string      `json:"claim_id"`
	ExpectedAImage string      `json:"expected_a_image"`
}

func (r PrelaunchAbortRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	if strings.TrimSpace(r.ClaimID) == "" {
		return fmt.Errorf("claim_id is required")
	}
	if strings.TrimSpace(r.ExpectedAImage) == "" {
		return fmt.Errorf("expected_a_image is required")
	}
	return nil
}

// PrelaunchAbortProof is the deterministic node proof bound to a consumed
// writer grant. Manager persists only its digest in the regional terminal
// transition; the proof is not a fencing credential by itself.
type PrelaunchAbortProof struct {
	Version           int    `json:"version"`
	Parent            string `json:"parent"`
	ClaimID           string `json:"claim_id"`
	WriterGrantID     string `json:"writer_grant_id"`
	WriterEpoch       int64  `json:"writer_epoch"`
	BindingVersion    int    `json:"binding_version"`
	BindingDigest     string `json:"binding_digest"`
	NodeUID           string `json:"node_uid"`
	BootID            string `json:"boot_id"`
	RuntimeGeneration string `json:"runtime_generation"`
	PodUID            string `json:"pod_uid"`
	PodSandboxID      string `json:"pod_sandbox_id"`
	SlotNonce         string `json:"slot_nonce"`
	RootFSID          string `json:"rootfs_id"`
	SnapshotterState  string `json:"snapshotter_state"`
}

func (p PrelaunchAbortProof) Validate() error {
	if p.Version != 1 {
		return fmt.Errorf("unsupported proof version %d", p.Version)
	}
	for name, value := range map[string]string{
		"parent": p.Parent, "claim_id": p.ClaimID, "writer_grant_id": p.WriterGrantID,
		"binding_digest": p.BindingDigest, "node_uid": p.NodeUID, "boot_id": p.BootID,
		"runtime_generation": p.RuntimeGeneration, "pod_uid": p.PodUID,
		"pod_sandbox_id": p.PodSandboxID, "slot_nonce": p.SlotNonce,
		"rootfs_id": p.RootFSID, "snapshotter_state": p.SnapshotterState,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if p.WriterEpoch <= 0 || p.BindingVersion <= 0 {
		return fmt.Errorf("positive writer_epoch and binding_version are required")
	}
	digest, err := hex.DecodeString(p.BindingDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != p.BindingDigest {
		return fmt.Errorf("binding_digest must be a canonical 32-byte lowercase hexadecimal digest")
	}
	if p.SnapshotterState != rootfshandoff.StateTombstoned {
		return fmt.Errorf("snapshotter_state must be tombstoned")
	}
	return nil
}

func (p PrelaunchAbortProof) Digest() ([sha256.Size]byte, error) {
	if err := p.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

type PrelaunchAbortResponse struct {
	Proof       PrelaunchAbortProof `json:"proof"`
	ProofDigest string              `json:"proof_digest"`
}

func (r PrelaunchAbortResponse) Validate() error {
	digest, err := r.Proof.Digest()
	if err != nil {
		return fmt.Errorf("proof: %w", err)
	}
	if r.ProofDigest != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("proof_digest does not match proof")
	}
	return nil
}

// FinalizeRequest removes the already-retired one-shot gate and local
// tombstone. The host Snapshotter independently verifies regional retirement
// before dropping its identity fence.
type FinalizeRequest struct {
	Pod     PodIdentity `json:"pod"`
	Parent  string      `json:"parent"`
	ClaimID string      `json:"claim_id"`
}

func (r FinalizeRequest) Validate() error {
	if err := r.Pod.Validate(); err != nil {
		return fmt.Errorf("pod: %w", err)
	}
	if strings.TrimSpace(r.Parent) == "" {
		return fmt.Errorf("parent is required")
	}
	if strings.TrimSpace(r.ClaimID) == "" {
		return fmt.Errorf("claim_id is required")
	}
	return nil
}

func ClaimPath(claimID string) string {
	return APIPathPrefix + url.PathEscape(claimID)
}

func PreflightPath(claimID string) string      { return ClaimPath(claimID) + "/preflight" }
func StagePath(claimID string) string          { return ClaimPath(claimID) + "/stage" }
func ReadyPath(claimID string) string          { return ClaimPath(claimID) + "/ready" }
func VerifyConsumerPath(claimID string) string { return ClaimPath(claimID) + "/consumer" }
func PlannedRetirePath(claimID string) string  { return ClaimPath(claimID) + "/retire" }
func RetireResultPath(claimID string) string   { return ClaimPath(claimID) + "/retire-result" }
func CrashFencePath(claimID string) string     { return ClaimPath(claimID) + "/crash-fence" }
func PrelaunchAbortPath(claimID string) string { return ClaimPath(claimID) + "/prelaunch-abort" }
func FinalizePath(claimID string) string       { return ClaimPath(claimID) + "/finalize" }
func GatePath(slotNonce string) string {
	return "/internal/v1/rootfs-slots/" + url.PathEscape(slotNonce) + "/gate"
}
