package sandboxstore

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// This conservative regional ceiling includes retained failed/unused captures
// until their exact GC completes. Exhaustion falls back to ordinary publication
// rather than preventing migration. The SQL guard enforces the same ceiling
// and transaction lock, including writes outside this method.
const nomadMigrationCaptureUploadRegionBytes = int64(64) << 30
const nomadMigrationCaptureUploadBudgetLock = int64(6129648202201)

func reserveNomadMigrationCaptureUpload(ctx context.Context, tx pgx.Tx, r *NomadSandboxMigrationReservation, assignment runtimecontrol.MigrationAssignment, imageBytes int64) (protocol.MigrationCaptureUpload, error) {
	var zero protocol.MigrationCaptureUpload
	_, err := protocol.MigrationCaptureUploadBytes(imageBytes)
	if err != nil {
		return zero, nil // Oversized captures retain the existing bounded path.
	}
	launch, err := nomadMigrationCapturedCPULaunch(ctx, tx, nomadMigrationCaptureIdentity(r))
	if err != nil {
		return zero, err
	}
	cpu, err := launch.GuestCPUProfile().Digest()
	if err != nil {
		return zero, err
	}
	grant, err := protocol.NewMigrationCaptureUpload(nomadMigrationCaptureIdentity(r), assignment.Target.TeamID,
		r.SourceSlot.CompatibilityDigest, cpu, imageBytes)
	if err != nil {
		return zero, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nomadMigrationCaptureUploadBudgetLock); err != nil {
		return zero, err
	}
	var used int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
        FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL`).Scan(&used); err != nil {
		return zero, err
	}
	if used > nomadMigrationCaptureUploadRegionBytes-grant.ReservedBytes() {
		return zero, nil
	}
	return grant, nil
}
