package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
)

var _ nomadmigration.HandoverStore = (*PGSandboxStore)(nil)

const nomadMigrationHandoverPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND l.phase='committing'
    AND m.failure_request IS NULL
    AND m.procd_handover_request IS NOT NULL AND m.generation_committed_at IS NULL`

func (s *PGSandboxStore) ListNomadMigrationHandovers(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationHandoverPredicate)
}

// GetNomadMigrationHandover rechecks live writer, heartbeat, claim and lifecycle
// authority before dispatch. It neither renews leases nor changes readiness;
// the receipt/readiness CAS repeats these checks after the remote operation.
func (s *PGSandboxStore) GetNomadMigrationHandover(ctx context.Context, id string) (*nomadmigration.Handover, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT m.procd_handover_request FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationHandoverPredicate, id).Scan(&payload)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var command procdapi.RuntimeMigrationRequest
	if json.Unmarshal(payload, &command) != nil || command.Assignment.OperationID != id {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, command.Assignment)
	if err != nil {
		return nil, err
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
		return nil, ErrNomadSandboxMigrationConflict
	}
	restored, stored, receipt, err := loadNomadMigrationHandover(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	h := &nomadmigration.Handover{Restored: restored, Command: stored, Receipt: receipt}
	if h.Validate() != nil || stored.InstanceID != reservation.SourceProcdInstanceID || stored.LifecycleEpoch != reservation.Lifecycle.Epoch {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if _, err := lockNomadMigrationHandoverTarget(ctx, tx, reservation, restored.Request); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return h, nil
}
