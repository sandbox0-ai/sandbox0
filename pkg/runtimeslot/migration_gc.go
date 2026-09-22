package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationSourceGCRequest acknowledges regional custody of the completed node
// receipt and a direct-client allocation absence observation. Only the regional
// reconciler sends it, after purge. It permits journal retention to expire; it
// grants no cleanup, execution, writer, or resource-release authority.
// The legacy wire capability is shared by source and failed-target custody;
// each node matches the exact finalization and cleanup digests before ack.
type MigrationSourceGCRequest struct {
	Target                  NodeChannelTarget `json:"target"`
	FinalizationDigest      string            `json:"finalization_digest"`
	CleanupProofDigest      string            `json:"cleanup_proof_digest"`
	AllocationAbsenceDigest string            `json:"allocation_absence_digest"`
}

func (r MigrationSourceGCRequest) Validate() error {
	if err := r.Target.validate(true); err != nil {
		return err
	}
	for name, value := range map[string]string{"finalization_digest": r.FinalizationDigest, "cleanup_proof_digest": r.CleanupProofDigest, "allocation_absence_digest": r.AllocationAbsenceDigest} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (r MigrationSourceGCRequest) Digest() (string, error) {
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

// MigrationSourceGCAcknowledgement confirms retention eligibility only. An
// already-pruned journal can acknowledge a retry without recreating evidence.
type MigrationSourceGCAcknowledgement struct {
	RequestDigest string `json:"request_digest"`
}

func (a MigrationSourceGCAcknowledgement) ValidateFor(r MigrationSourceGCRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if a.RequestDigest != want {
		return fmt.Errorf("migration source GC acknowledgement changed request")
	}
	return nil
}

func NewNodeChannelMigrationSourceGCCommand(r MigrationSourceGCRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationSourceGC, Target: r.Target, MigrationSourceGC: &r})
}
