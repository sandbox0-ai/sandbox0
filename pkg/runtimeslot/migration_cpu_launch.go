package runtimeslot

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const MigrationCPULaunchVersion = 1

// MigrationCPULaunch binds launch-time CPU evidence to one physical runtime
// claim. It is not a node attestation by itself: only a trusted launch recorder
// may persist it, before advertising readiness. A later host observation must
// never be used to manufacture this historical record for an existing guest.
// Resources is the immutable launch lease snapshot, validated against its
// digest; it does not grant capacity. No writer bearer tokens, workload
// environment or filesystem descriptors are copied into this evidence.
type MigrationCPULaunch struct {
	// ExecutableDigest identifies the qualified runtime artifact. New split-runtime
	// releases bind the whole bundle; historical main-file digests remain readable.
	ExecutableDigest    string                      `json:"executable_digest"`
	Version             int                         `json:"version"`
	Target              NodeChannelTarget           `json:"target"`
	SandboxID           string                      `json:"sandbox_id"`
	RuntimeGeneration   int64                       `json:"runtime_generation"`
	LaunchAttempt       string                      `json:"launch_attempt"`
	BindingDigest       string                      `json:"binding_digest"`
	ResourceLeaseDigest string                      `json:"resource_lease_digest"`
	Resources           RuntimeResourceLease        `json:"resources"`
	AssignmentRevision  string                      `json:"assignment_revision"`
	Observation         MigrationCPUObservation     `json:"observation"`
	Restored            *MigrationCPURestoreLineage `json:"restored,omitempty"`
}

func (l MigrationCPULaunch) Validate() error {
	if l.Version != MigrationCPULaunchVersion || l.RuntimeGeneration <= 0 {
		return fmt.Errorf("invalid CPU launch version or generation")
	}
	if err := l.Target.validate(true); err != nil {
		return err
	}
	for name, value := range map[string]string{"sandbox_id": l.SandboxID, "launch_attempt": l.LaunchAttempt} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"binding_digest": l.BindingDigest, "resource_lease_digest": l.ResourceLeaseDigest, "assignment_revision": l.AssignmentRevision} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	if err := digest.Digest(l.ExecutableDigest).Validate(); err != nil || !strings.HasPrefix(l.ExecutableDigest, "sha256:") {
		return fmt.Errorf("CPU launch lacks exact runsc executable digest")
	}
	lease, err := l.Resources.Digest()
	if err != nil || strings.TrimPrefix(lease, "sha256:") != l.ResourceLeaseDigest ||
		l.Resources.SlotID != l.Target.SlotID || l.Resources.ClusterID != l.Target.ClusterID ||
		l.Resources.NodeID != l.Target.NodeID || l.Resources.NodeUID != l.Target.NodeUID || l.Resources.NodeBootID != l.Target.NodeBootID {
		return fmt.Errorf("CPU launch resource snapshot changed its binding")
	}
	if err := l.Observation.Covers(l.Resources.CPUSetCPUs); err != nil {
		return err
	}
	if l.Restored != nil {
		if l.RuntimeGeneration < 2 && l.Restored.CheckpointKind != runtimecontrol.CheckpointFork {
			return fmt.Errorf("restored CPU history requires an advanced generation")
		}
		if err := l.Restored.Validate(); err != nil {
			return err
		}
		return CheckMigrationCPUProfiles(l.Restored.GuestProfile, l.Observation.Profile)
	}
	return nil
}

func (l MigrationCPULaunch) Digest() (string, error) {
	if err := l.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(l)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// BindMigrationCPULaunch checks the exact original ordinary claim. The caller
// must already have measured CPU exposure during that launch; this function
// only binds evidence, and performs no observation or execution. Restored guests
// inherit CPU state from their source image and cannot become ordinary launches.
func BindMigrationCPULaunch(target NodeChannelTarget, claim NodeClaimControlRequest, observation MigrationCPUObservation, executableDigest string) (*MigrationCPULaunch, error) {
	if err := target.validate(true); err != nil {
		return nil, err
	}
	if err := claim.ValidateRegional(); err != nil {
		return nil, err
	}
	if claim.MigrationRestore != nil {
		return nil, fmt.Errorf("restored CPU exposure requires source image evidence")
	}
	identity, resources := claim.Stage.Identity, claim.Resources
	if target.SlotID != identity.SlotNonce || target.AllocationID != identity.AllocationID || target.NodeUID != identity.NodeUID ||
		target.NodeBootID != identity.BootID || target.ClusterID != resources.ClusterID || target.NodeID != resources.NodeID ||
		identity.RuntimeGeneration != strconv.FormatInt(claim.Runtime.RuntimeGeneration, 10) || identity.TaskName != NomadTaskName ||
		identity.RootFSDriver != "nomad-driver" || identity.RuntimeClass != "sandbox0-gvisor" {
		return nil, fmt.Errorf("CPU launch changed runtime placement")
	}
	if err := observation.Covers(resources.CPUSetCPUs); err != nil {
		return nil, err
	}
	binding, err := claim.Stage.BindingDigest()
	if err != nil {
		return nil, err
	}
	lease, _ := resources.Digest()
	revision, _ := claim.Runtime.Revision()
	// Own the feature slice: caller mutation after binding cannot rewrite the
	// launch's historical evidence through a shared backing array.
	observation.Profile.Features = append([]string(nil), observation.Profile.Features...)
	launch := &MigrationCPULaunch{ExecutableDigest: executableDigest, Version: MigrationCPULaunchVersion, Target: target, SandboxID: claim.Runtime.SandboxID,
		RuntimeGeneration: claim.Runtime.RuntimeGeneration, LaunchAttempt: identity.LaunchAttempt,
		BindingDigest: hex.EncodeToString(binding[:]), ResourceLeaseDigest: strings.TrimPrefix(lease, "sha256:"), Resources: resources, AssignmentRevision: revision, Observation: observation}
	return launch, launch.Validate()
}

// ValidateCapture rejects evidence from another boot, allocation, launch or
// resource contract before any current host measurement can be considered.
// The expected launch attempt comes from the source's durable Stage identity.
func (l MigrationCPULaunch) ValidateCapture(capture MigrationCaptureRequest, launchAttempt string, resources RuntimeResourceLease) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if err := capture.Validate(); err != nil {
		return err
	}
	resourceDigest, err := resources.Digest()
	if err != nil {
		return err
	}
	if l.Target != capture.Target || l.SandboxID != capture.SandboxID || l.RuntimeGeneration != capture.SourceGeneration ||
		l.LaunchAttempt != launchAttempt || l.BindingDigest != capture.BindingDigest ||
		l.AssignmentRevision != capture.AssignmentRevision || l.ResourceLeaseDigest != capture.ResourceLeaseDigest || l.ResourceLeaseDigest != strings.TrimPrefix(resourceDigest, "sha256:") ||
		resources.SlotID != l.Target.SlotID || resources.ClusterID != l.Target.ClusterID || resources.NodeID != l.Target.NodeID ||
		resources.NodeUID != l.Target.NodeUID || resources.NodeBootID != l.Target.NodeBootID {
		return fmt.Errorf("CPU launch evidence changed captured runtime")
	}
	return l.Observation.Covers(resources.CPUSetCPUs)
}

// CheckCurrentSource requires an unchanged source profile and coverage of the
// current eligible CPUs. Even a source feature superset changes historical
// assumptions; only the destination may provide a feature superset.
func (l MigrationCPULaunch) CheckCurrentSource(current MigrationCPUObservation, cpuSet string) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if err := l.Observation.Covers(cpuSet); err != nil {
		return err
	}
	if err := current.Covers(cpuSet); err != nil {
		return err
	}
	before, _ := l.Observation.Profile.Digest()
	after, err := current.Profile.Digest()
	if err != nil || before != after {
		return fmt.Errorf("source CPU profile changed since launch")
	}
	return nil
}
