package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationFailureFinalizeRequest acknowledges regional writer retirement by
// carrying the already committed physical cleanup receipt. It authorizes only
// artifact reclamation, never execution, allocation purge or capacity release.
type MigrationFailureFinalizeRequest struct {
	Request MigrationFailureCleanupRequest `json:"request"`
	Proof   MigrationFailureCleanupProof   `json:"proof"`
}

func (r MigrationFailureFinalizeRequest) Digest() (string, error) {
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

type MigrationFailureFinalizeProof struct {
	RequestDigest         string `json:"request_digest"`
	RootFSArtifactsAbsent bool   `json:"rootfs_artifacts_absent"`
	ImageAbsent           bool   `json:"image_absent"`
}

type MigrationFailureFinalizationReceipt struct {
	Request MigrationFailureFinalizeRequest `json:"request"`
	Proof   MigrationFailureFinalizeProof   `json:"proof"`
}

func (r MigrationFailureFinalizationReceipt) Validate() error {
	return r.Proof.ValidateFor(r.Request)
}

func (p MigrationFailureFinalizeProof) ValidateFor(r MigrationFailureFinalizeRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || !p.RootFSArtifactsAbsent || !p.ImageAbsent {
		return fmt.Errorf("failed migration destination retains artifacts")
	}
	return nil
}

func NewNodeChannelMigrationFailureFinalizeCommand(request MigrationFailureFinalizeRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationFailureFinalize,
		Target: request.Request.Failure.Request.Restore.Image.Target, MigrationFailureFinalize: &request})
}
