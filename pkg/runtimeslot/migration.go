package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

const NodeMigrationCaptureControlPath = "/migration/capture"

func NewNodeChannelMigrationCaptureCommand(request MigrationCaptureRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationCapture,
		Target: request.Target, MigrationCapture: &request})
}

// Recovery is a distinct capability: it may inspect existing capture custody
// and finish its filesystem cut, but cannot initiate a checkpoint.
func NewNodeChannelMigrationRecoverCommand(request MigrationCaptureRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationRecover,
		Target: request.Target, MigrationCapture: &request})
}

// MigrationCaptureRequest is a private source-custody command. It neither
// authorizes destination execution nor publishes a consistent RootFS cut.
// The caller must already own the regional, noncancelable migration lifecycle.
// Host image paths are derived locally and never supplied by this request.
type MigrationCaptureRequest struct {
	Target              NodeChannelTarget `json:"target"`
	OperationID         string            `json:"operation_id"`
	LifecycleEpoch      int64             `json:"lifecycle_epoch"`
	SandboxID           string            `json:"sandbox_id"`
	SourceGeneration    int64             `json:"source_generation"`
	AssignmentRevision  string            `json:"assignment_revision"`
	BindingDigest       string            `json:"binding_digest"`
	ResourceLeaseDigest string            `json:"resource_lease_digest"`
	ProcdInstanceID     string            `json:"procd_instance_id"`
}

func (r MigrationCaptureRequest) Validate() error {
	if err := r.Target.validate(true); err != nil {
		return err
	}
	for name, value := range map[string]string{"operation_id": r.OperationID, "sandbox_id": r.SandboxID, "procd_instance_id": r.ProcdInstanceID} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if r.LifecycleEpoch <= 0 || r.SourceGeneration <= 0 {
		return fmt.Errorf("migration requires positive source generation and lifecycle epoch")
	}
	for name, value := range map[string]string{"assignment_revision": r.AssignmentRevision, "binding_digest": r.BindingDigest, "resource_lease_digest": r.ResourceLeaseDigest} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (r MigrationCaptureRequest) Digest() (string, error) {
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

const (
	MigrationCaptureIntent    = "intent"
	MigrationCaptureComplete  = "captured"
	MigrationCaptureUncertain = "uncertain"
)

// MigrationCapture is durable node custody, not restore authority. An intent
// recovered after driver death is ambiguous and cannot rerun checkpoint into
// the same image directory. Only a completed checkpoint call may mark captured.
type MigrationCapture struct {
	Request       MigrationCaptureRequest           `json:"request"`
	RequestDigest string                            `json:"request_digest"`
	State         string                            `json:"state"`
	RootFS        *rootfshandoff.MigrationRootFSCut `json:"rootfs,omitempty"`
}

func (c MigrationCapture) Validate() error {
	digest, err := c.Request.Digest()
	if err != nil {
		return err
	}
	if c.RequestDigest != digest {
		return fmt.Errorf("migration capture request digest changed")
	}
	if c.RootFS != nil {
		if c.State != MigrationCaptureComplete || c.RootFS.Validate() != nil ||
			c.RootFS.Request.OperationID != c.Request.OperationID || c.RootFS.Request.CaptureRequestDigest != digest ||
			c.RootFS.Request.SourceBindingDigest != c.Request.BindingDigest || c.RootFS.Request.GenerationID != "migration-"+digest {
			return fmt.Errorf("migration filesystem cut does not match its completed memory capture")
		}
	}
	switch c.State {
	case MigrationCaptureIntent, MigrationCaptureComplete, MigrationCaptureUncertain:
		return nil
	default:
		return fmt.Errorf("invalid migration capture state %q", c.State)
	}
}
