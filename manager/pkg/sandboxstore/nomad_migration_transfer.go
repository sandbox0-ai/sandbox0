package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
)

var _ nomadmigration.Store = (*PGSandboxStore)(nil)

// The existing migration/lifecycle rows are the work queue. An authorized
// publication is required: this scan cannot launch or checkpoint a workload.
const nomadMigrationTransferPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
    AND l.phase='publishing' AND m.publication_request IS NOT NULL AND m.source_fence_proof IS NULL`

func (s *PGSandboxStore) ListNomadMigrationTransfers(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationTransferPredicate)
}

func (s *PGSandboxStore) listNomadMigrationWork(ctx context.Context, after string, limit int, predicate string) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 256 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT m.operation_id FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE `+predicate+` AND m.operation_id COLLATE "C" > $1 COLLATE "C"
        ORDER BY m.operation_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

// GetNomadMigrationTransfer reads one statement snapshot of immutable commands
// and their receipts. It returns nil if another worker already advanced past
// transfer; reads do not authorize a new phase or change any lease.
func (s *PGSandboxStore) GetNomadMigrationTransfer(ctx context.Context, id string) (*nomadmigration.Transfer, error) {
	var publication, published, image, prepared, fence, binding []byte
	var assignmentDigest, publicationDigest, sourceSlot, targetSlot, leaseID, procdID string
	var imageDigest, fenceDigest *string
	err := s.pool.QueryRow(ctx, `SELECT m.publication_request,m.publication_receipt,m.target_image_request,m.target_image_receipt,m.source_fence_request,
        m.assignment_digest,m.publication_digest,m.source_slot_id,m.target_slot_id,m.target_resource_lease_id,m.source_procd_instance_id,m.source_binding_digest,
        m.target_image_digest,m.source_fence_digest
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationTransferPredicate, id).Scan(&publication, &published, &image, &prepared, &fence,
		&assignmentDigest, &publicationDigest, &sourceSlot, &targetSlot, &leaseID, &procdID, &binding, &imageDigest, &fenceDigest)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var t nomadmigration.Transfer
	if json.Unmarshal(publication, &t.Publication) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	for _, item := range []struct {
		data []byte
		into any
	}{{published, &t.Published}, {image, &t.Image}, {prepared, &t.Prepared}, {fence, &t.Fence}} {
		if len(item.data) != 0 && json.Unmarshal(item.data, item.into) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if t.Validate() != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	a, _ := t.Publication.Assignment.Digest()
	p, _ := t.Publication.Digest()
	capture := t.Publication.Capture.Request
	if a != assignmentDigest || p != publicationDigest || t.Publication.Assignment.OperationID != id || capture.Target.SlotID != sourceSlot || capture.ProcdInstanceID != procdID || capture.BindingDigest != hex.EncodeToString(binding) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if t.Image != nil {
		digest, _ := t.Image.Digest()
		if imageDigest == nil || *imageDigest != digest || t.Image.Target.SlotID != targetSlot || t.Image.Resources.LeaseID != leaseID {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if imageDigest != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if t.Fence != nil {
		digest, _ := t.Fence.Digest()
		if fenceDigest == nil || *fenceDigest != digest {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if fenceDigest != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &t, nil
}
