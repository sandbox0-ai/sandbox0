package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// MigrationPublicationRequest names a completed source cut. CPUFeaturesDigest
// and CPULaunch are immutable eligibility evidence supplied by the regional
// coordinator, not CPU measurements performed by this storage operation.
// CPULaunch may be absent on historical metadata, which cannot execute restore.
// Publication alone
// never authorizes target execution or releases source custody.
type MigrationPublicationRequest struct {
	Capture             MigrationCapture                   `json:"capture"`
	Assignment          runtimecontrol.MigrationAssignment `json:"assignment"`
	CompatibilityDigest string                             `json:"compatibility_digest"`
	CPUFeaturesDigest   string                             `json:"cpu_features_digest"`
	CPULaunch           *MigrationCPULaunch                `json:"cpu_launch,omitempty"`
}

func (r MigrationPublicationRequest) Binding() (runtimecheckpoint.Binding, error) {
	var zero runtimecheckpoint.Binding
	if err := r.Capture.Validate(); err != nil {
		return zero, err
	}
	if err := r.Assignment.Validate(); err != nil {
		return zero, err
	}
	c := r.Capture.Request
	if r.CPULaunch != nil {
		if err := r.CPULaunch.ValidateCapture(c, r.CPULaunch.LaunchAttempt, r.CPULaunch.Resources); err != nil {
			return zero, err
		}
		profile, _ := r.CPULaunch.GuestCPUProfile().Digest()
		if profile != r.CPUFeaturesDigest {
			return zero, fmt.Errorf("image publication changed its launch CPU profile")
		}
	}
	if r.Capture.State != MigrationCaptureComplete || r.Capture.RootFS == nil ||
		r.Assignment.OperationID != c.OperationID || r.Assignment.SourceGeneration != c.SourceGeneration ||
		r.Assignment.SourceRevision != c.AssignmentRevision || r.Assignment.Target.SandboxID != c.SandboxID {
		return zero, fmt.Errorf("image publication requires the exact completed source cut")
	}
	payload, err := json.Marshal(r.Capture.RootFS.Generation)
	if err != nil {
		return zero, err
	}
	binding := runtimecheckpoint.Binding{OperationID: c.OperationID, SandboxID: c.SandboxID,
		TeamID: r.Assignment.Target.TeamID, SourceBindingDigest: "sha256:" + c.BindingDigest,
		RuntimeCompatibilityDigest: r.CompatibilityDigest, AssignmentRevision: "sha256:" + c.AssignmentRevision,
		CPUFeaturesDigest: r.CPUFeaturesDigest, RootFSGenerationID: r.Capture.RootFS.Generation.GenerationID,
		RootFSDescriptorDigest: digest.FromBytes(payload).String()}
	return binding, binding.Validate()
}

func (r MigrationPublicationRequest) Digest() (string, error) {
	if _, err := r.Binding(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// MigrationPublication is an immutable object-store receipt. Its request
// includes the full filesystem descriptor, preventing independent memory and
// disk snapshots from being combined under the same operation identity.
type MigrationPublication struct {
	RequestDigest string                      `json:"request_digest"`
	Binding       runtimecheckpoint.Binding   `json:"binding"`
	Reference     runtimecheckpoint.Reference `json:"reference"`
}

func (p MigrationPublication) ValidateFor(request MigrationPublicationRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	binding, err := request.Binding()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.Binding != binding {
		return fmt.Errorf("published image changed its source binding")
	}
	return p.Reference.ValidateFor(binding)
}

func NewNodeChannelMigrationPublishCommand(request MigrationPublicationRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationPublish,
		Target: request.Capture.Request.Target, MigrationPublish: &request})
}
