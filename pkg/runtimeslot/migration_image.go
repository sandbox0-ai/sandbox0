package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// MigrationImagePrepareRequest grants custody of one verified execution image
// to the reserved destination. It carries no bearer token, filesystem writer,
// host path or permission to execute. Preparation precedes source detachment.
type MigrationImagePrepareRequest struct {
	Target      NodeChannelTarget           `json:"target"`
	Publication MigrationPublicationRequest `json:"publication"`
	Receipt     MigrationPublication        `json:"receipt"`
	Resources   RuntimeResourceLease        `json:"resources"`
	Checkpoint  *CheckpointRestoreAuthority `json:"checkpoint,omitempty"`
}

func (r MigrationImagePrepareRequest) Validate() error {
	if err := r.Target.validate(true); err != nil {
		return err
	}
	if err := r.Receipt.ValidateFor(r.Publication); err != nil {
		return err
	}
	if err := r.Resources.Validate(); err != nil {
		return err
	}
	source := r.Publication.Capture.Request.Target
	l := r.Resources
	if r.Target.ClusterID != source.ClusterID || r.Target.SlotID == source.SlotID || r.Target.AllocationID == source.AllocationID ||
		l.OperationID != r.OperationID() || l.SlotID != r.Target.SlotID || l.ClusterID != r.Target.ClusterID ||
		l.NodeID != r.Target.NodeID || l.NodeUID != r.Target.NodeUID || l.NodeBootID != r.Target.NodeBootID {
		return fmt.Errorf("image destination does not match independently reserved carrier capacity")
	}
	if r.Checkpoint != nil {
		return r.Checkpoint.ValidateFor(r.Publication, r.Receipt)
	}
	if r.Publication.CheckpointSource != nil {
		return fmt.Errorf("checkpoint images require independent restore authorization")
	}
	if r.Target.NodeID == source.NodeID || r.Target.NodeUID == source.NodeUID {
		return fmt.Errorf("migration requires a destination on a different node")
	}
	return nil
}

// OperationID and RuntimeAssignment identify this image's destination use,
// which may differ from its immutable capture identity.
func (r MigrationImagePrepareRequest) OperationID() string {
	if r.Checkpoint != nil {
		return r.Checkpoint.Assignment.OperationID
	}
	return r.Publication.Assignment.OperationID
}

func (r MigrationImagePrepareRequest) RuntimeAssignment() runtimecontrol.Assignment {
	if r.Checkpoint != nil {
		return r.Checkpoint.Assignment.Target
	}
	return r.Publication.Assignment.Target
}

func (r MigrationImagePrepareRequest) Digest() (string, error) {
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

// MigrationImagePrepared is returned only after every manifest chunk has been
// verified and fsynced and its receipt is durable in the destination journal.
// It never establishes command readiness or authorizes resource reclamation.
type MigrationImagePrepared struct {
	RequestDigest  string `json:"request_digest"`
	ManifestDigest string `json:"manifest_digest"`
	TotalBytes     int64  `json:"total_bytes"`
}

func (p MigrationImagePrepared) ValidateFor(request MigrationImagePrepareRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.ManifestDigest != request.Receipt.Reference.ManifestDigest || p.TotalBytes <= 0 || p.TotalBytes > runtimecheckpoint.MaxImageBytes {
		return fmt.Errorf("prepared migration image changed its identity or size")
	}
	return nil
}

func NewNodeChannelMigrationImagePrepareCommand(request MigrationImagePrepareRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationImagePrepare,
		Target: request.Target, MigrationImagePrepare: &request})
}
