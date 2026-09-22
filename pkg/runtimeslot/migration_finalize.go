package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// MigrationSourceFinalizeRequest is issued by the region after it commits the
// target adoption or irreversible execution-stop receipt. It authorizes destruction only of the fenced source
// artifacts and physical carrier. Allocation purge and lease release remain
// regional operations after this command returns its full physical proof.
type MigrationSourceFinalizeRequest struct {
	Fence       MigrationSourceFenceRequest  `json:"fence"`
	SourceProof MigrationSourceFenceProof    `json:"source_proof"`
	Adoption    MigrationAdoptionReceipt     `json:"adoption"`
	Failure     *MigrationFailureStopReceipt `json:"failure,omitempty"`
	Cleanup     NodeCleanupControlRequest    `json:"cleanup"`
}

func MigrationSourceCleanupOperationID(operation string) string {
	sum := sha256.Sum256([]byte(operation))
	return "migration-source-" + hex.EncodeToString(sum[:])
}

func (r MigrationSourceFinalizeRequest) Validate() error {
	if err := r.SourceProof.ValidateFor(r.Fence); err != nil {
		return err
	}
	if r.Failure == nil {
		if err := r.Adoption.Proof.ValidateFor(r.Adoption.Request); err != nil {
			return err
		}
	} else {
		if r.Adoption != (MigrationAdoptionReceipt{}) {
			return fmt.Errorf("source cleanup has conflicting destination outcomes")
		}
		if err := r.Failure.Proof.ValidateFor(r.Failure.Request); err != nil {
			return err
		}
		restore := r.Failure.Request.Restore
		want, err := r.Fence.Digest()
		actual, ae := restore.Fence.Digest()
		if err != nil || ae != nil || actual != want || restore.Proof.Digest != r.SourceProof.Digest {
			return fmt.Errorf("failed destination does not retain the same source fence")
		}
	}
	if err := r.Cleanup.Validate(); err != nil {
		return err
	}
	capture := r.Fence.PublicationRequest.Capture.Request
	assignment := r.Fence.PublicationRequest.Assignment
	a, c := r.Adoption.Request, r.Cleanup
	if r.Failure == nil && (a.OperationID != assignment.OperationID || a.SandboxID != capture.SandboxID || a.RuntimeGeneration != assignment.Target.RuntimeGeneration ||
		a.ProcdInstanceID != capture.ProcdInstanceID || a.Target.ClusterID != capture.Target.ClusterID || a.Target.NodeUID == capture.Target.NodeUID) {
		return fmt.Errorf("migration finalization changed adopted destination")
	}
	if c.OperationID != MigrationSourceCleanupOperationID(assignment.OperationID) || c.WriterOperationID != assignment.OperationID || c.WriterRetireKind != WriterRetireKindMigration ||
		c.WriterGrantID == "" || c.WriterAuthorityDigest != r.SourceProof.Digest || c.RunscContainerID != r.SourceProof.ContainerID ||
		c.SlotID != capture.Target.SlotID || c.ClusterID != capture.Target.ClusterID || c.NodeID != capture.Target.NodeID || c.NodeUID != capture.Target.NodeUID ||
		c.NodeBootID != capture.Target.NodeBootID || c.AllocationID != capture.Target.AllocationID || c.Resources.IsZero() || c.ResourceLeaseDigest != capture.ResourceLeaseDigest {
		return fmt.Errorf("migration finalization changed source or adopted destination")
	}
	return nil
}

// Destination returns the exact target whose adoption or failed execution has
// already been attested. Callers must validate the whole request first.
func (r MigrationSourceFinalizeRequest) Destination() NodeChannelTarget {
	if r.Failure != nil {
		return r.Failure.Request.Restore.Image.Target
	}
	return r.Adoption.Request.Target
}

func (r MigrationSourceFinalizeRequest) Digest() (string, error) {
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

func (r MigrationSourceFinalizeRequest) RootFSRequest() (rootfshandoff.MigrationRootFSFinalizeRequest, error) {
	digest, err := r.Digest()
	if err != nil {
		return rootfshandoff.MigrationRootFSFinalizeRequest{}, err
	}
	return rootfshandoff.MigrationRootFSFinalizeRequest{OperationID: r.Fence.PublicationRequest.Assignment.OperationID, DetachProofDigest: r.SourceProof.RootFS.Digest, AuthorizationDigest: digest}, nil
}

// MigrationSourceFinalizeProof includes the ordinary node cleanup evidence as
// well as absence of the migration image and retained source WAL.
type MigrationSourceFinalizeProof struct {
	RequestDigest string                                     `json:"request_digest"`
	RootFS        rootfshandoff.MigrationRootFSFinalizeProof `json:"rootfs"`
	ImageAbsent   bool                                       `json:"image_absent"`
	Cleanup       NodeCleanupControlProof                    `json:"cleanup"`
}

// MigrationSourceFinalizationReceipt is a read-only projection of the source
// node's command and physical cleanup journal, not a new cleanup authority.
type MigrationSourceFinalizationReceipt struct {
	Request MigrationSourceFinalizeRequest `json:"request"`
	Proof   MigrationSourceFinalizeProof   `json:"proof"`
}

func (r MigrationSourceFinalizationReceipt) Validate() error {
	return r.Proof.ValidateFor(r.Request)
}

// MigrationSourceFinalizationResponse is historical evidence for one slot. A
// missing receipt is not permission to execute, clean up, or release resources.
type MigrationSourceFinalizationResponse struct {
	SlotID  string                              `json:"slot_id"`
	Receipt *MigrationSourceFinalizationReceipt `json:"receipt"`
}

func (r MigrationSourceFinalizationResponse) ValidateFor(slot string) error {
	if ValidateSlotID(slot) != nil || r.SlotID != slot {
		return fmt.Errorf("migration source receipt belongs to another slot")
	}
	if r.Receipt != nil {
		if err := r.Receipt.Validate(); err != nil {
			return err
		}
		if r.Receipt.Request.Cleanup.SlotID != slot {
			return fmt.Errorf("migration source receipt changed source slot")
		}
	}
	return nil
}

func MigrationSourceFinalizationPath(slot string) string {
	return SlotPath(slot) + "/migration-source-finalization"
}

func (p MigrationSourceFinalizeProof) ValidateFor(r MigrationSourceFinalizeRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	root, err := r.RootFSRequest()
	if err != nil {
		return err
	}
	digest, err := p.RootFS.ProofDigest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || !p.ImageAbsent || p.Cleanup.Validate() != nil || p.Cleanup.Request() != r.Cleanup ||
		p.RootFS.Request != root || p.RootFS.Digest != digest || !p.RootFS.BranchAbsent || !p.RootFS.MountDirectoriesAbsent ||
		p.RootFS.Parent != r.SourceProof.RootFS.Session.Parent || p.RootFS.BindingDigest != r.SourceProof.RootFS.Session.BindingDigest ||
		p.Cleanup.RootFSProofDigest != p.RootFS.Digest {
		return fmt.Errorf("migration finalization lacks exact physical cleanup evidence")
	}
	return nil
}

func NewNodeChannelMigrationFinalizeCommand(request MigrationSourceFinalizeRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationFinalize, Target: request.Fence.PublicationRequest.Capture.Request.Target, MigrationFinalize: &request})
}
