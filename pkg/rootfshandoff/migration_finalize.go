package rootfshandoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationRootFSFinalizeRequest releases retained source artifacts after the
// target generation is adopted. Ctld verifies regional completion authority
// before passing this command to the session manager. This is never a pause,
// crash abandonment, or authorization to publish another filesystem head.
type MigrationRootFSFinalizeRequest struct {
	OperationID         string `json:"operation_id"`
	DetachProofDigest   string `json:"detach_proof_digest"`
	AuthorizationDigest string `json:"authorization_digest"`
}

func (r MigrationRootFSFinalizeRequest) Validate() error {
	return (MigrationRootFSDetachRequest{OperationID: r.OperationID, CutDigest: r.DetachProofDigest, AuthorizationDigest: r.AuthorizationDigest}).Validate()
}

func (r MigrationRootFSFinalizeRequest) ValidateFor(stage StageRequest, detached MigrationRootFSDetachProof) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return err
	}
	if err := detached.Validate(); err != nil {
		return err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return err
	}
	if stage.Identity.WriterGrantToken != "" || r.OperationID != detached.Request.OperationID || r.DetachProofDigest != detached.Digest ||
		detached.Session.Parent != stage.Parent || detached.Session.RootFSID != stage.Identity.RootFSID || detached.Session.WriterEpoch != stage.Identity.WriterEpoch ||
		detached.Session.BindingDigest != hex.EncodeToString(binding[:]) {
		return fmt.Errorf("migration finalization changed the detached source writer")
	}
	return nil
}

// MigrationRootFSFinalizeProof describes removal of the retained WAL and mount
// directories. The original detach proof remains the physical writer fence.
// This proof does not release the carrier's network or compute resource lease.
type MigrationRootFSFinalizeProof struct {
	Request                MigrationRootFSFinalizeRequest `json:"request"`
	Parent                 string                         `json:"parent"`
	BindingDigest          string                         `json:"binding_digest"`
	BranchAbsent           bool                           `json:"branch_absent"`
	MountDirectoriesAbsent bool                           `json:"mount_directories_absent"`
	Digest                 string                         `json:"digest"`
}

func (p MigrationRootFSFinalizeProof) ProofDigest() (string, error) {
	p.Digest = ""
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (p MigrationRootFSFinalizeProof) ValidateFor(stage StageRequest, detached MigrationRootFSDetachProof, request MigrationRootFSFinalizeRequest) error {
	if err := request.ValidateFor(stage, detached); err != nil {
		return err
	}
	want, err := p.ProofDigest()
	if err != nil {
		return err
	}
	if p.Request != request || p.Parent != stage.Parent || p.BindingDigest != detached.Session.BindingDigest ||
		!p.BranchAbsent || !p.MountDirectoriesAbsent || p.Digest != want {
		return fmt.Errorf("migration finalization lacks exact artifact absence proof")
	}
	return nil
}
