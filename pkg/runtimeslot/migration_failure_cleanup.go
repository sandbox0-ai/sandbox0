package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationFailureCleanupRequest authorizes the existing physical slot cleanup
// only after a regional writer fence and an irreversible destination stop.
// It never publishes target writes or releases the allocation/resource lease.
type MigrationFailureCleanupRequest struct {
	Failure MigrationFailureStopReceipt `json:"failure"`
	Cleanup NodeCleanupControlRequest   `json:"cleanup"`
}

func MigrationFailureCleanupOperationID(operation string) string {
	d := sha256.Sum256([]byte(operation))
	return "migration-failed-target-" + hex.EncodeToString(d[:])
}

func (r MigrationFailureCleanupRequest) Digest() (string, error) {
	if err := r.Failure.Proof.ValidateFor(r.Failure.Request); err != nil {
		return "", err
	}
	if err := r.Cleanup.Validate(); err != nil {
		return "", err
	}
	s := r.Failure.Request.Restore
	t, c := s.Image.Target, r.Cleanup
	op := s.Image.Publication.Assignment.OperationID
	resources, err := s.Image.Resources.Digest()
	if err != nil {
		return "", err
	}
	if c.OperationID != MigrationFailureCleanupOperationID(op) || c.WriterOperationID != c.OperationID ||
		(c.WriterRetireKind != WriterRetireKindCrashAbandon && c.WriterRetireKind != WriterRetireKindCanceled) ||
		c.WriterAuthorityDigest != r.Failure.Proof.RequestDigest || c.WriterGrantID != s.Stage.Identity.WriterGrantID ||
		c.ClusterID != t.ClusterID || c.NodeID != t.NodeID || c.NodeUID != t.NodeUID || c.NodeBootID != t.NodeBootID ||
		c.SlotID != t.SlotID || c.AllocationID != t.AllocationID || c.RunscContainerID != r.Failure.Proof.ContainerID ||
		c.NetNSIdentity != s.Stage.ExpectedPolicyToken.NetNSIdentity || c.Resources != s.Image.Resources ||
		"sha256:"+c.ResourceLeaseDigest != resources {
		return "", fmt.Errorf("failed destination cleanup changed physical custody")
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(payload)
	return hex.EncodeToString(d[:]), nil
}

// MigrationFailureCleanupProof proves physical detach and carrier cleanup.
// The external crash journal, retained dirty tail and image may still exist
// until the region acknowledges writer retirement and authorizes finalization.
type MigrationFailureCleanupProof struct {
	RequestDigest string                  `json:"request_digest"`
	Cleanup       NodeCleanupControlProof `json:"cleanup"`
}

func (p MigrationFailureCleanupProof) ValidateFor(r MigrationFailureCleanupRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.Cleanup.Validate() != nil || p.Cleanup.Request() != r.Cleanup {
		return fmt.Errorf("failed destination lacks exact physical cleanup proof")
	}
	return nil
}

func NewNodeChannelMigrationFailureCleanupCommand(request MigrationFailureCleanupRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationFailureCleanup,
		Target: request.Failure.Request.Restore.Image.Target, MigrationFailureCleanup: &request})
}
