package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var _ nomadmigration.DestinationStore = (*PGSandboxStore)(nil)

const nomadMigrationDestinationPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND l.phase='committing'
    AND m.failure_request IS NULL
    AND m.source_fence_proof IS NOT NULL AND m.restore_receipt IS NULL AND m.generation_committed_at IS NULL
    AND m.source_network_policy IS NOT NULL`

func (s *PGSandboxStore) ListNomadMigrationDestinations(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationDestinationPredicate)
}

// GetNomadMigrationDestination reads only a physically fenced transaction's
// immutable image and source policy. Actual attachment/restore rechecks all
// live destination admission, TTL, writer and lifecycle conditions in the CAS.
func (s *PGSandboxStore) GetNomadMigrationDestination(ctx context.Context, id string) (*nomadmigration.Destination, error) {
	var image, prepared, fence, proof []byte
	var imageDigest, assignmentDigest, sourceSlot, targetSlot, leaseID string
	var d nomadmigration.Destination
	err := s.pool.QueryRow(ctx, `SELECT m.target_image_request,m.target_image_receipt,m.source_fence_request,m.source_fence_proof,
        m.target_image_digest,m.assignment_digest,m.source_slot_id,m.target_slot_id,m.target_resource_lease_id,m.source_network_policy,m.source_network_policy_digest
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationDestinationPredicate, id).Scan(&image, &prepared, &fence, &proof, &imageDigest, &assignmentDigest, &sourceSlot, &targetSlot, &leaseID, &d.Policy, &d.PolicyDigest)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var imageReceipt protocol.MigrationImagePrepared
	var fenceRequest protocol.MigrationSourceFenceRequest
	var fenceProof protocol.MigrationSourceFenceProof
	if json.Unmarshal(image, &d.Image) != nil || json.Unmarshal(prepared, &imageReceipt) != nil || json.Unmarshal(fence, &fenceRequest) != nil || json.Unmarshal(proof, &fenceProof) != nil || d.Validate() != nil || imageReceipt.ValidateFor(d.Image) != nil || fenceProof.ValidateFor(fenceRequest) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	actual, _ := d.Image.Digest()
	assignment, _ := d.Image.Publication.Assignment.Digest()
	publication, _ := d.Image.Publication.Digest()
	fenced, _ := fenceRequest.PublicationRequest.Digest()
	if actual != imageDigest || assignment != assignmentDigest || d.Image.Publication.Assignment.OperationID != id || d.Image.Publication.Capture.Request.Target.SlotID != sourceSlot || d.Image.Target.SlotID != targetSlot || d.Image.Resources.LeaseID != leaseID || publication != fenced || d.Image.Receipt != fenceRequest.Publication {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &d, nil
}
