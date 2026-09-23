package sandboxstore

import (
	"context"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Checkpoints and migrations share the same regional reservation. The grant
// precedes capture, so a publication retry cannot create unlimited objects.
func reserveNomadCheckpointCaptureUpload(ctx context.Context, tx pgx.Tx, source protocol.MigrationCaptureRequest,
	teamID, compatibilityDigest, cpuFeaturesDigest string, imageBytes int64) (protocol.MigrationCaptureUpload, error) {
	grant, err := protocol.NewMigrationCaptureUpload(source, teamID, compatibilityDigest, cpuFeaturesDigest, imageBytes)
	if err != nil {
		return protocol.MigrationCaptureUpload{}, err
	}
	if grant.ReservedBytes() > nomadMigrationCaptureUploadRegionBytes {
		return protocol.MigrationCaptureUpload{}, ErrNomadCheckpointConflict
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nomadMigrationCaptureUploadBudgetLock); err != nil {
		return protocol.MigrationCaptureUpload{}, err
	}
	var used int64
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
		 FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL)
		+
		(SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
		 FROM manager.sandbox_runtime_checkpoints
		 WHERE capture_upload_gc_completed_at IS NULL AND capture_upload_reservation_released_at IS NULL)`).Scan(&used); err != nil {
		return protocol.MigrationCaptureUpload{}, err
	}
	if used > nomadMigrationCaptureUploadRegionBytes-grant.ReservedBytes() {
		return protocol.MigrationCaptureUpload{}, ErrNomadCheckpointConflict
	}
	return grant, nil
}
