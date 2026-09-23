package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const NodeMigrationCPUPreflightControlPath = "/migration/cpu-preflight"
const MigrationCPUPreflightTimeout = 2 * time.Minute

// MigrationCPUPreflightRequest describes one read-only observation of a
// reserved source/destination pair. Source is an identity, not authorization
// to invoke capture. The source reads its own durable launch history; only the
// destination accepts the source result carried over the authenticated region.
// A successful result never reserves resources, freezes or starts a workload.
type MigrationCPUPreflightRequest struct {
	// CaptureOnly observes a source without reserving a future destination.
	// Resume/fork must independently verify the eventual target before restore.
	CaptureOnly          bool                                        `json:"capture_only,omitempty"`
	Checkpoint           *runtimecontrol.CheckpointRestoreAssignment `json:"checkpoint,omitempty"`
	Target               NodeChannelTarget                           `json:"target"`
	Source               MigrationCaptureRequest                     `json:"source"`
	SourceResources      RuntimeResourceLease                        `json:"source_resources"`
	Destination          NodeChannelTarget                           `json:"destination"`
	DestinationResources RuntimeResourceLease                        `json:"destination_resources"`
	Launch               *MigrationCPULaunch                         `json:"launch,omitempty"`
}

func (r MigrationCPUPreflightRequest) IsSource() bool { return r.Target == r.Source.Target }

func (r MigrationCPUPreflightRequest) Validate() error {
	if err := r.Source.Validate(); err != nil {
		return err
	}
	placements := []struct {
		target    NodeChannelTarget
		resources RuntimeResourceLease
	}{
		{r.Source.Target, r.SourceResources},
	}
	if r.CaptureOnly {
		if !r.IsSource() || r.Destination != (NodeChannelTarget{}) || !r.DestinationResources.IsZero() || r.Launch != nil || r.Checkpoint != nil {
			return fmt.Errorf("capture-only CPU preflight requires only the source and its recorded launch")
		}
	} else {
		if err := r.Destination.validate(true); err != nil {
			return err
		}
		if r.Destination.ClusterID != r.Source.Target.ClusterID || r.Destination.SlotID == r.Source.Target.SlotID ||
			r.Destination.AllocationID == r.Source.Target.AllocationID {
			return fmt.Errorf("CPU preflight requires distinct carriers in one cluster")
		}
		operation := r.Source.OperationID
		if r.Checkpoint != nil {
			a := r.Checkpoint
			if a.Validate() != nil || r.IsSource() || a.Capture.OperationID != r.Source.OperationID ||
				a.Capture.SandboxID != r.Source.SandboxID || a.Capture.RuntimeGeneration != r.Source.SourceGeneration ||
				a.Capture.Revision != r.Source.AssignmentRevision || a.OperationID == operation {
				return fmt.Errorf("CPU preflight changed checkpoint restore authority")
			}
			operation = a.OperationID
		} else if r.Destination.NodeID == r.Source.Target.NodeID || r.Destination.NodeUID == r.Source.Target.NodeUID {
			return fmt.Errorf("migration CPU preflight requires distinct nodes")
		}
		placements = append(placements, struct {
			target    NodeChannelTarget
			resources RuntimeResourceLease
		}{r.Destination, r.DestinationResources})
		if r.DestinationResources.OperationID != operation {
			return fmt.Errorf("CPU preflight changed destination reservation")
		}
	}
	for _, pair := range placements {
		if err := pair.resources.Validate(); err != nil {
			return err
		}
		t, l := pair.target, pair.resources
		if l.SlotID != t.SlotID || l.ClusterID != t.ClusterID || l.NodeID != t.NodeID || l.NodeUID != t.NodeUID || l.NodeBootID != t.NodeBootID {
			return fmt.Errorf("CPU preflight changed resource placement")
		}
	}
	sourceDigest, _ := r.SourceResources.Digest()
	if strings.TrimPrefix(sourceDigest, "sha256:") != r.Source.ResourceLeaseDigest {
		return fmt.Errorf("CPU preflight changed source lease")
	}
	if r.IsSource() {
		if r.Launch != nil {
			return fmt.Errorf("source CPU history must come from its launch recorder")
		}
		return nil
	}
	if r.Target != r.Destination || r.Launch == nil {
		return fmt.Errorf("CPU preflight lacks exact target or source history")
	}
	return r.Launch.ValidateCapture(r.Source, r.Launch.LaunchAttempt, r.SourceResources)
}

func (r MigrationCPUPreflightRequest) Digest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// MigrationCPUPreflight is point-in-time evidence, not execution authority.
// The regional transaction separately owns request freshness and custody.
type MigrationCPUPreflight struct {
	RequestDigest string                  `json:"request_digest"`
	Launch        MigrationCPULaunch      `json:"launch"`
	Observation   MigrationCPUObservation `json:"observation"`
}

func (p MigrationCPUPreflight) ValidateFor(request MigrationCPUPreflightRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want {
		return fmt.Errorf("CPU preflight changed request")
	}
	if err := p.Launch.ValidateCapture(request.Source, p.Launch.LaunchAttempt, request.SourceResources); err != nil {
		return err
	}
	if request.IsSource() {
		return p.Launch.CheckCurrentSource(p.Observation, request.SourceResources.CPUSetCPUs)
	}
	expected, _ := request.Launch.Digest()
	actual, err := p.Launch.Digest()
	if err != nil || actual != expected {
		return fmt.Errorf("destination CPU preflight changed source history")
	}
	if err := p.Observation.Covers(request.DestinationResources.CPUSetCPUs); err != nil {
		return err
	}
	return CheckMigrationCPUProfiles(p.Launch.GuestCPUProfile(), p.Observation.Profile)
}

func NewNodeChannelMigrationCPUPreflightCommand(request MigrationCPUPreflightRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationCPUPreflight,
		Target: request.Target, MigrationCPUPreflight: &request})
}

func (r NodeControlResponse) validateCPUPreflightResult(request MigrationCPUPreflightRequest) error {
	if r.Phase != "cpu_preflight" || r.MigrationCPUPreflight == nil || r.ClaimTiming != nil || r.Migration != nil || r.MigrationRestore != nil || r.MigrationAdoption != nil {
		return fmt.Errorf("CPU preflight cannot carry execution authority")
	}
	return r.MigrationCPUPreflight.ValidateFor(request)
}
