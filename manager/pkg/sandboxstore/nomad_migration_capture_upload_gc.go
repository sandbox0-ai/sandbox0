package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const nomadMigrationCaptureUploadGCPredicate = `m.capture_upload_gc_completed_at IS NULL AND manager.runtime_migration_capture_upload_releasable(m.operation_id)`

func (s *PGSandboxStore) ListNomadMigrationCaptureUploadGC(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationCaptureUploadGCPredicate)
}

// AuthorizeNomadMigrationCaptureUploadGC derives the exact prefix from the
// original immutable staging grant, even when capture never published an image
// or the sandbox has since been deleted. Both node custodians must have ended.
func (s *PGSandboxStore) AuthorizeNomadMigrationCaptureUploadGC(ctx context.Context, id string) (*runtimecheckpoint.CaptureScope, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var payload []byte
	var retained *string
	if err := tx.QueryRow(ctx, `SELECT staging_request,capture_upload_gc_scope_digest
        FROM manager.sandbox_runtime_migrations m WHERE operation_id=$1 AND `+nomadMigrationCaptureUploadGCPredicate+` FOR UPDATE`, id).Scan(&payload, &retained); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var request protocol.MigrationStagingRequest
	if json.Unmarshal(payload, &request) != nil || request.Validate() != nil || !request.IsSource() || request.Source.OperationID != id {
		return nil, ErrNomadSandboxMigrationConflict
	}
	scope, err := request.CaptureUpload.Scope(request.Source)
	if err != nil {
		return nil, err
	}
	digest, err := scope.Digest()
	if err != nil || retained != nil && *retained != digest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_upload_gc_scope_digest=$2 WHERE operation_id=$1`, id, digest); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &scope, nil
}

func (s *PGSandboxStore) CompleteNomadMigrationCaptureUploadGC(ctx context.Context, scope runtimecheckpoint.CaptureScope) error {
	digest, err := scope.Digest()
	if err != nil {
		return err
	}
	result, err := s.pool.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations
        SET capture_upload_gc_completed_at=COALESCE(capture_upload_gc_completed_at,clock_timestamp())
        WHERE operation_id=$1 AND capture_upload_gc_scope_digest=$2
            AND manager.runtime_migration_capture_upload_releasable(operation_id)`, scope.OperationID(), digest)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
