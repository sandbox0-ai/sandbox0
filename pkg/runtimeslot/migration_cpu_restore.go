package runtimeslot

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"
)

// MigrationCPURestoreLineage retains the guest exposure across migrations.
// Only the immediate predecessor's digest is carried: history must not grow
// recursively with the number of moves. The regional restore command retains
// that predecessor and is the authority used to verify this link.
type MigrationCPURestoreLineage struct {
	SourceLaunchDigest   string              `json:"source_launch_digest"`
	RestoreRequestDigest string              `json:"restore_request_digest"`
	GuestProfile         MigrationCPUProfile `json:"guest_profile"`
}

func (l MigrationCPURestoreLineage) Validate() error {
	if err := digest.Digest(l.SourceLaunchDigest).Validate(); err != nil || !strings.HasPrefix(l.SourceLaunchDigest, "sha256:") {
		return fmt.Errorf("restored CPU history lacks its source launch digest")
	}
	if _, err := DecodeProof("restore_request_digest", l.RestoreRequestDigest); err != nil {
		return err
	}
	return l.GuestProfile.Validate()
}

// GuestCPUProfile is the CPU state inherited by the process. Observation is
// separately the current host's launch/restore capability, which may be wider.
func (l MigrationCPULaunch) GuestCPUProfile() MigrationCPUProfile {
	profile := l.Observation.Profile
	if l.Restored != nil {
		profile = l.Restored.GuestProfile
	}
	profile.Features = append([]string(nil), profile.Features...)
	return profile
}

func (l *MigrationCPULaunch) Clone() *MigrationCPULaunch {
	if l == nil {
		return nil
	}
	copy := *l
	copy.Observation.Profile.Features = append([]string(nil), l.Observation.Profile.Features...)
	if l.Restored != nil {
		lineage := *l.Restored
		lineage.GuestProfile = l.GuestCPUProfile()
		copy.Restored = &lineage
	}
	return &copy
}

// BindMigrationCPURestore is called only after actual successful restore and
// unchanged observations on both sides of that execution. It preserves the
// source guest profile, even when the destination supports additional features.
// Like ordinary launch binding, it validates evidence, not physical execution.
func BindMigrationCPURestore(request MigrationRestoreRequest, before, after MigrationCPUObservation) (*MigrationCPULaunch, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	source := request.Image.Publication.CPULaunch
	if source == nil {
		return nil, fmt.Errorf("restored CPU history requires its source launch")
	}
	resources := request.Image.Resources
	if err := before.Covers(resources.CPUSetCPUs); err != nil {
		return nil, err
	}
	if err := after.Covers(resources.CPUSetCPUs); err != nil {
		return nil, err
	}
	first, _ := before.Digest()
	last, _ := after.Digest()
	if first != last {
		return nil, fmt.Errorf("destination CPU profile or coverage changed during restore")
	}
	guest := source.GuestCPUProfile()
	if err := CheckMigrationCPUProfiles(guest, after.Profile); err != nil {
		return nil, err
	}
	restoreDigest, _ := request.Digest()
	sourceDigest, _ := source.Digest()
	binding, _ := request.Stage.BindingDigest()
	lease, _ := resources.Digest()
	assignment := request.Image.Publication.Assignment.Target
	revision, _ := assignment.Revision()
	after.Profile.Features = append([]string(nil), after.Profile.Features...)
	launch := &MigrationCPULaunch{Version: MigrationCPULaunchVersion, ExecutableDigest: source.ExecutableDigest,
		Target: request.Image.Target, SandboxID: assignment.SandboxID, RuntimeGeneration: assignment.RuntimeGeneration,
		LaunchAttempt: request.Stage.Identity.LaunchAttempt, BindingDigest: hex.EncodeToString(binding[:]),
		ResourceLeaseDigest: strings.TrimPrefix(lease, "sha256:"), Resources: resources, AssignmentRevision: revision,
		Observation: after, Restored: &MigrationCPURestoreLineage{SourceLaunchDigest: sourceDigest, RestoreRequestDigest: restoreDigest, GuestProfile: guest}}
	return launch, launch.Validate()
}

// ValidateRestore rejects invented exposure, dropped lineage, or reuse of a
// different restore's history. The caller must obtain request from durable
// regional/node authority, never from a source choosing its own predecessor.
func (l MigrationCPULaunch) ValidateRestore(request MigrationRestoreRequest) error {
	if err := l.Validate(); err != nil {
		return err
	}
	expected, err := BindMigrationCPURestore(request, l.Observation, l.Observation)
	if err != nil {
		return err
	}
	want, _ := expected.Digest()
	actual, _ := l.Digest()
	if want != actual {
		return fmt.Errorf("CPU lineage changed its restored runtime or guest exposure")
	}
	return nil
}
