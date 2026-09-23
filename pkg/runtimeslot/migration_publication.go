package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

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
	// CheckpointSource publishes a reusable memory image without selecting its
	// future runtime. Assignment must be zero in this mode. The source's CPU
	// launch evidence remains mandatory even though no destination exists yet.
	CheckpointSource                 *runtimecontrol.Assignment         `json:"checkpoint_source,omitempty"`
	DestinationPeerCertificateSHA256 string                             `json:"destination_peer_certificate_sha256,omitempty"`
	Capture                          MigrationCapture                   `json:"capture"`
	Assignment                       runtimecontrol.MigrationAssignment `json:"assignment"`
	CompatibilityDigest              string                             `json:"compatibility_digest"`
	CPUFeaturesDigest                string                             `json:"cpu_features_digest"`
	CPULaunch                        *MigrationCPULaunch                `json:"cpu_launch,omitempty"`
}

// SourceAssignment returns the captured configuration, never future execution
// authority. Both memory checkpoints and migrations bind the same image format.
func (r MigrationPublicationRequest) SourceAssignment() (runtimecontrol.Assignment, error) {
	var source runtimecontrol.Assignment
	if r.CheckpointSource != nil {
		if !reflect.ValueOf(r.Assignment).IsZero() || r.DestinationPeerCertificateSHA256 != "" || r.CPULaunch == nil {
			return source, fmt.Errorf("checkpoint publication requires source-only assignment and recorded CPU history")
		}
		source = *r.CheckpointSource
	} else {
		if err := r.Assignment.Validate(); err != nil {
			return source, err
		}
		if r.Assignment.OperationID != r.Capture.Request.OperationID {
			return source, fmt.Errorf("image publication changed source operation")
		}
		source = r.Assignment.Target
		source.RuntimeGeneration = r.Assignment.SourceGeneration
	}
	revision, err := source.Revision()
	if err != nil {
		return runtimecontrol.Assignment{}, err
	}
	c := r.Capture.Request
	if source.SandboxID != c.SandboxID || source.RuntimeGeneration != c.SourceGeneration || revision != c.AssignmentRevision {
		return runtimecontrol.Assignment{}, fmt.Errorf("image publication changed captured assignment")
	}
	return source, nil
}

func (r MigrationPublicationRequest) Binding() (runtimecheckpoint.Binding, error) {
	var zero runtimecheckpoint.Binding
	if r.DestinationPeerCertificateSHA256 != "" {
		if err := runtimecheckpoint.ValidatePeerCertificateDigest(r.DestinationPeerCertificateSHA256); err != nil {
			return zero, err
		}
	}
	if err := r.Capture.Validate(); err != nil {
		return zero, err
	}
	source, err := r.SourceAssignment()
	if err != nil {
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
	if r.Capture.State != MigrationCaptureComplete || r.Capture.RootFS == nil {
		return zero, fmt.Errorf("image publication requires the exact completed source cut")
	}
	payload, err := json.Marshal(r.Capture.RootFS.Generation)
	if err != nil {
		return zero, err
	}
	binding := runtimecheckpoint.Binding{OperationID: c.OperationID, SandboxID: c.SandboxID,
		TeamID: source.TeamID, SourceBindingDigest: "sha256:" + c.BindingDigest,
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
	Peer          runtimecheckpoint.PeerEndpoint `json:"peer,omitzero"`
	RequestDigest string                         `json:"request_digest"`
	Binding       runtimecheckpoint.Binding      `json:"binding"`
	Reference     runtimecheckpoint.Reference    `json:"reference"`
}

func (p MigrationPublication) ValidateFor(request MigrationPublicationRequest) error {
	if p.Peer != (runtimecheckpoint.PeerEndpoint{}) {
		if request.DestinationPeerCertificateSHA256 == "" {
			return fmt.Errorf("peer image requires the authorized destination certificate")
		}
		if err := p.Peer.Validate(); err != nil {
			return err
		}
	}
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
