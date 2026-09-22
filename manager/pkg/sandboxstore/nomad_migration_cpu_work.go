package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
)

var _ nomadmigration.CPUPreflightStore = (*PGSandboxStore)(nil)

// A read-only work scan cannot renew reservation or CPU authority. Legacy
// reservations without their assignment payload expire through normal cleanup.
const nomadMigrationCPUWorkPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
    AND l.phase='preparing' AND m.assignment_request IS NOT NULL
    AND m.preparation_request IS NULL AND m.cpu_preflight_destination IS NULL
    AND l.created_at + INTERVAL '2 minutes' > clock_timestamp()
    AND (m.cpu_preflight_requested_at IS NULL OR
        (m.cpu_preflight_requested_at <= clock_timestamp() AND
         m.cpu_preflight_requested_at + INTERVAL '2 minutes' > clock_timestamp()))`

func (s *PGSandboxStore) ListNomadMigrationCPUPreflights(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationCPUWorkPredicate)
}

func (s *PGSandboxStore) GetNomadMigrationCPUPreflightWork(ctx context.Context, id string) (*nomadmigration.CPUPreflightWork, error) {
	var payload []byte
	var digest, sandboxID string
	var from, to int64
	work := &nomadmigration.CPUPreflightWork{}
	err := s.pool.QueryRow(ctx, `SELECT m.assignment_request,m.assignment_digest,m.cpu_preflight_source IS NOT NULL,
        l.sandbox_id,l.from_generation,l.to_generation
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationCPUWorkPredicate, id).Scan(&payload, &digest, &work.SourceCommitted, &sandboxID, &from, &to)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(payload, &work.Assignment) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	a := work.Assignment
	want, err := a.Digest()
	if err != nil || want != digest || a.OperationID != id || a.Target.SandboxID != sandboxID ||
		a.SourceGeneration != from || a.Target.RuntimeGeneration != to {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return work, nil
}
