package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationCaptureFailureRequest is regional authority to abandon an uncertain
// source before publication. It is not a destination stop receipt: no target
// execution can have been authorized. The node must independently reject any
// existing publication, filesystem cut or source-fence custody.
type MigrationCaptureFailureRequest struct {
	Capture MigrationCapture          `json:"capture"`
	Cleanup NodeCleanupControlRequest `json:"cleanup"`
}

func NewNodeChannelMigrationCaptureFailureCleanupCommand(request MigrationCaptureFailureRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationCaptureFailureCleanup,
		Target: request.Capture.Request.Target, MigrationCaptureFailureCleanup: &request})
}

func NewNodeChannelMigrationCaptureFailureFinalizeCommand(request MigrationCaptureFailureFinalizeRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationCaptureFailureFinalize,
		Target: request.Request.Capture.Request.Target, MigrationCaptureFailureFinalize: &request})
}

func MigrationCaptureFailureOperationID(operation string) string {
	d := sha256.Sum256([]byte(operation))
	return "migration-failed-capture-" + hex.EncodeToString(d[:])
}

func (r MigrationCaptureFailureRequest) Digest() (string, error) {
	if err := r.Capture.Validate(); err != nil {
		return "", err
	}
	if err := r.Cleanup.Validate(); err != nil {
		return "", err
	}
	s, c := r.Capture.Request, r.Cleanup
	if r.Capture.State != MigrationCaptureUncertain || r.Capture.RootFS != nil ||
		c.OperationID != MigrationCaptureFailureOperationID(s.OperationID) || c.WriterOperationID != c.OperationID ||
		c.WriterRetireKind != WriterRetireKindCrashAbandon || c.WriterGrantID == "" || c.RunscContainerID == "" ||
		c.WriterAuthorityDigest != r.Capture.RequestDigest || c.Resources.IsZero() || c.ResourceLeaseDigest != s.ResourceLeaseDigest ||
		c.ClusterID != s.Target.ClusterID || c.NodeID != s.Target.NodeID || c.NodeUID != s.Target.NodeUID ||
		c.NodeBootID != s.Target.NodeBootID || c.SlotID != s.Target.SlotID || c.AllocationID != s.Target.AllocationID {
		return "", fmt.Errorf("failed capture cleanup changed uncertain source custody")
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(payload)
	return hex.EncodeToString(d[:]), nil
}

// MigrationCaptureFailureProof leaves the external crash WAL and capture
// image retained until the regional writer retirement has been committed.
type MigrationCaptureFailureProof struct {
	RequestDigest string                  `json:"request_digest"`
	Cleanup       NodeCleanupControlProof `json:"cleanup"`
}

func (p MigrationCaptureFailureProof) ValidateFor(r MigrationCaptureFailureRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.Cleanup.Validate() != nil || p.Cleanup.Request() != r.Cleanup {
		return fmt.Errorf("failed capture lacks exact physical cleanup proof")
	}
	return nil
}

// MigrationCaptureFailureFinalizeRequest acknowledges committed retirement;
// the node also verifies the exact terminal writer with regional authority.
type MigrationCaptureFailureFinalizeRequest struct {
	Request MigrationCaptureFailureRequest `json:"request"`
	Proof   MigrationCaptureFailureProof   `json:"proof"`
}

func (r MigrationCaptureFailureFinalizeRequest) Digest() (string, error) {
	if err := r.Proof.ValidateFor(r.Request); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(payload)
	return hex.EncodeToString(d[:]), nil
}

type MigrationCaptureFailureFinalizeProof struct {
	RequestDigest         string `json:"request_digest"`
	RootFSArtifactsAbsent bool   `json:"rootfs_artifacts_absent"`
	ImageAbsent           bool   `json:"image_absent"`
}

type MigrationCaptureFailureReceipt struct {
	Request MigrationCaptureFailureFinalizeRequest `json:"request"`
	Proof   MigrationCaptureFailureFinalizeProof   `json:"proof"`
}

func (r MigrationCaptureFailureReceipt) Validate() error { return r.Proof.ValidateFor(r.Request) }

func (p MigrationCaptureFailureFinalizeProof) ValidateFor(r MigrationCaptureFailureFinalizeRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || !p.RootFSArtifactsAbsent || !p.ImageAbsent {
		return fmt.Errorf("failed capture retains artifacts")
	}
	return nil
}
