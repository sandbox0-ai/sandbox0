package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

var _ nomadmigration.StagingStore = (*PGSandboxStore)(nil)

const nomadMigrationStagingWorkPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND l.phase='preparing'
    AND m.assignment_request IS NOT NULL AND m.preparation_request IS NULL AND m.cpu_preflight_destination IS NOT NULL
    AND NOT m.staging_source_release_requested AND NOT m.staging_destination_release_requested
    AND (m.staging_source_receipt IS NULL OR m.staging_destination_receipt IS NULL)
    AND l.created_at <= clock_timestamp() AND l.created_at + INTERVAL '2 minutes' > clock_timestamp()
    AND m.cpu_preflight_requested_at <= clock_timestamp() AND m.cpu_preflight_requested_at + INTERVAL '2 minutes' > clock_timestamp()`

const nomadMigrationStagingReleasePredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND m.staging_request IS NOT NULL AND (
    (m.staging_source_release_receipt IS NULL AND (m.staging_source_release_requested OR m.source_finalization_receipt IS NOT NULL OR m.capture_failure_finalization_receipt IS NOT NULL OR
        (l.phase='aborted' AND (m.preparation_request IS NULL OR m.preparation_cancel_receipt IS NOT NULL) AND m.capture_request IS NULL))) OR
    (m.staging_destination_release_receipt IS NULL AND (m.staging_destination_release_requested OR m.adoption_receipt IS NOT NULL OR m.failure_finalization_receipt IS NOT NULL OR m.capture_failure_request IS NOT NULL OR
        (l.phase='aborted' AND (m.preparation_request IS NULL OR m.preparation_cancel_receipt IS NOT NULL) AND m.capture_request IS NULL))))`

func (s *PGSandboxStore) ListNomadMigrationStaging(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationStagingWorkPredicate)
}

func (s *PGSandboxStore) ListNomadMigrationStagingReleases(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationStagingReleasePredicate)
}

func (s *PGSandboxStore) GetNomadMigrationStagingAssignment(ctx context.Context, id string) (*runtimecontrol.MigrationAssignment, error) {
	var payload []byte
	var digest, sandbox string
	var from, to int64
	err := s.pool.QueryRow(ctx, `SELECT m.assignment_request,m.assignment_digest,l.sandbox_id,l.from_generation,l.to_generation
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationStagingWorkPredicate, id).Scan(&payload, &digest, &sandbox, &from, &to)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var a runtimecontrol.MigrationAssignment
	if json.Unmarshal(payload, &a) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, err := a.Digest()
	if err != nil || want != digest || a.OperationID != id || a.Target.SandboxID != sandbox || a.SourceGeneration != from || a.Target.RuntimeGeneration != to {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &a, nil
}
