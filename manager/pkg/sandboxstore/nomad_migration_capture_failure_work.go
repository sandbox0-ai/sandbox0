package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var _ nomadmigration.CaptureFailureStore = (*PGSandboxStore)(nil)

const nomadCaptureFailurePredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND l.phase='publishing'
    AND m.capture_failure_request IS NOT NULL AND m.capture_failure_finalization_receipt IS NULL`

func (s *PGSandboxStore) ListNomadMigrationCaptureFailures(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadCaptureFailurePredicate)
}

func (s *PGSandboxStore) GetNomadMigrationCaptureFailure(ctx context.Context, id string) (*nomadmigration.CaptureFailureWork, error) {
	var request, cleanup []byte
	var digest string
	err := s.pool.QueryRow(ctx, `SELECT m.capture_failure_request,m.capture_failure_digest,m.capture_failure_cleanup_receipt
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadCaptureFailurePredicate, id).Scan(&request, &digest, &cleanup)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	w := &nomadmigration.CaptureFailureWork{}
	if json.Unmarshal(request, &w.Request) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, err := w.Request.Digest()
	if err != nil || want != digest || w.Request.Capture.Request.OperationID != id {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(cleanup) != 0 {
		w.Cleanup = &protocol.MigrationCaptureFailureProof{}
		if json.Unmarshal(cleanup, w.Cleanup) != nil || w.Cleanup.ValidateFor(w.Request) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	return w, nil
}
