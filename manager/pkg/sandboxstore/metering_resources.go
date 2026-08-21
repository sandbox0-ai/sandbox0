package sandboxstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	nomadMeteringResourceBackfillBatchSize = 500
	nomadMeteringResourceBackfillLockName  = "manager/nomad-metering-resource-backfill/v1"
)

// NomadMeteringResourceResolver derives the immutable resource shape that was
// selected for an existing Nomad sandbox before numeric metering fields were
// introduced.
type NomadMeteringResourceResolver func(*SandboxRecord) (millicpu int64, memoryMiB int64, err error)

// BackfillNomadMeteringResources upgrades pre-metering Nomad rows in bounded,
// locked batches. The sandbox trigger durably requeues every repaired source.
func (s *PGSandboxStore) BackfillNomadMeteringResources(
	ctx context.Context,
	resolve NomadMeteringResourceResolver,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("sandbox store is not configured")
	}
	if resolve == nil {
		return 0, fmt.Errorf("Nomad metering resource resolver is required")
	}
	var total int64
	for {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return total, fmt.Errorf("begin Nomad metering resource backfill: %w", err)
		}
		batch, batchErr := backfillNomadMeteringResourceBatch(ctx, tx, resolve)
		if batchErr != nil {
			_ = tx.Rollback(ctx)
			return total, batchErr
		}
		if err := tx.Commit(ctx); err != nil {
			return total, fmt.Errorf("commit Nomad metering resource backfill: %w", err)
		}
		total += batch
		if batch < nomadMeteringResourceBackfillBatchSize {
			return total, nil
		}
	}
}

func backfillNomadMeteringResourceBatch(
	ctx context.Context,
	tx pgx.Tx,
	resolve NomadMeteringResourceResolver,
) (int64, error) {
	// Serialize only batch selection, resolution, and commit. The transaction
	// lock is released after each bounded batch, so multiple manager replicas
	// can take over without mistaking another replica's locked rows for a
	// completed backfill.
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, nomadMeteringResourceBackfillLockName); err != nil {
		return 0, fmt.Errorf("lock Nomad metering resource backfill: %w", err)
	}
	rows, err := tx.Query(ctx, sandboxRecordSelectSQL()+`
		WHERE runtime_backend = $1
			AND (resource_millicpu = 0 OR resource_memory_mib = 0)
		ORDER BY sandbox_id
		FOR UPDATE
		LIMIT $2
	`, SandboxRuntimeBackendNomad, nomadMeteringResourceBackfillBatchSize)
	if err != nil {
		return 0, fmt.Errorf("list Nomad metering resource backfill rows: %w", err)
	}
	records := make([]*SandboxRecord, 0, nomadMeteringResourceBackfillBatchSize)
	for rows.Next() {
		record, err := scanSandboxRecordRows(rows)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan Nomad metering resource backfill row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate Nomad metering resource backfill rows: %w", err)
	}
	rows.Close()

	var updated int64
	for _, record := range records {
		millicpu, memoryMiB, err := resolve(record)
		if err != nil {
			return updated, fmt.Errorf("resolve Nomad metering resources for %s: %w", record.ID, err)
		}
		if millicpu <= 0 || memoryMiB <= 0 {
			return updated, fmt.Errorf("resolved Nomad metering resources for %s must be positive", record.ID)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE manager.sandboxes
			SET resource_millicpu = $2,
				resource_memory_mib = $3,
				updated_at = NOW()
			WHERE sandbox_id = $1 AND runtime_backend = $4
				AND (resource_millicpu = 0 OR resource_memory_mib = 0)
		`, record.ID, millicpu, memoryMiB, SandboxRuntimeBackendNomad)
		if err != nil {
			return updated, fmt.Errorf("backfill Nomad metering resources for %s: %w", record.ID, err)
		}
		updated += tag.RowsAffected()
	}
	return updated, nil
}
