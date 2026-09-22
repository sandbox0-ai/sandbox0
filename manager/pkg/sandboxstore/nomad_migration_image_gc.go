package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const nomadMigrationImageGCPredicate = `m.image_gc_completed_at IS NULL AND manager.runtime_migration_image_releasable(m.operation_id)`

func (s *PGSandboxStore) ListNomadMigrationImageGC(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationImageGCPredicate)
}

// AuthorizeNomadMigrationImageGC persists the immutable object prefix only after
// physical custody has ended on both sides. It remains valid after sandbox
// deletion, since immutable commands and receipts outlive the RootFS writer FK.
func (s *PGSandboxStore) AuthorizeNomadMigrationImageGC(ctx context.Context, id string) (*runtimecheckpoint.Binding, error) {
	var request, receipt []byte
	var retained string
	err := s.pool.QueryRow(ctx, `UPDATE manager.sandbox_runtime_migrations m
        SET image_gc_binding_digest=m.publication_receipt->'reference'->>'binding_digest'
        WHERE m.operation_id=$1 AND `+nomadMigrationImageGCPredicate+`
        RETURNING publication_request,publication_receipt,image_gc_binding_digest`, id).Scan(&request, &receipt, &retained)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var command protocol.MigrationPublicationRequest
	var published protocol.MigrationPublication
	if json.Unmarshal(request, &command) != nil || json.Unmarshal(receipt, &published) != nil || published.ValidateFor(command) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	binding, err := command.Binding()
	digest, digestErr := binding.Digest()
	if err != nil || digestErr != nil || binding.OperationID != id || digest != retained {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &binding, nil
}

func (s *PGSandboxStore) CompleteNomadMigrationImageGC(ctx context.Context, binding runtimecheckpoint.Binding) error {
	digest, err := binding.Digest()
	if err != nil {
		return err
	}
	result, err := s.pool.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations m
        SET image_gc_completed_at=COALESCE(image_gc_completed_at,clock_timestamp())
        WHERE operation_id=$1 AND image_gc_binding_digest=$2
            AND manager.runtime_migration_image_releasable(operation_id)`, binding.OperationID, digest)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
