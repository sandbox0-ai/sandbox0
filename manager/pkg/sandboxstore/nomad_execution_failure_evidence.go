package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Failure commands share the physical protocol. These helpers only select the
// evidence owner; lifecycle phase, leases and writer authority remain unchanged.
var executionFailureColumns = map[string]string{
	"failure": "failure_request", "failure_stopped": "failure_stop_receipt",
	"failure_cleanup": "failure_cleanup_request", "failure_cleaned": "failure_cleanup_receipt",
	"failure_finalized": "failure_finalization_receipt",
}

type executionFailureQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readNomadExecutionFailureEvidence(ctx context.Context, q executionFailureQuerier, id, key string) ([]byte, error) {
	column, ok := executionFailureColumns[key]
	if !ok {
		return nil, errors.New("unsupported execution failure evidence field")
	}
	var payload []byte
	err := q.QueryRow(ctx, `SELECT evidence->$2::text FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1
        UNION ALL SELECT `+column+` FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id, key).Scan(&payload)
	return payload, err
}

func saveNomadExecutionFailureEvidence(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn, key string, payload []byte, migrationSQL string, extra ...any) error {
	if life.Kind == SandboxLifecycleKindResume {
		if _, ok := executionFailureColumns[key]; !ok {
			return errors.New("unsupported checkpoint failure evidence field")
		}
		_, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=evidence || jsonb_build_object($2::text,$3::jsonb)
            WHERE operation_id=$1 AND evidence->$2::text IS DISTINCT FROM $3::jsonb`, life.ID, key, payload)
		return err
	}
	args := []any{life.ID, payload}
	args = append(args, extra...)
	_, err := tx.Exec(ctx, migrationSQL, args...)
	return err
}

func executionFailurePending(life *SandboxLifecycleTxn) bool {
	return life != nil && (life.Kind == SandboxLifecycleKindMigrate && life.Phase == SandboxLifecyclePhaseCommitting ||
		life.Kind == SandboxLifecycleKindResume && life.Phase == SandboxLifecyclePhasePreparing)
}

// Work stays in the existing bounded failure lanes; retained checkpoint rows
// are not another queue, and history does not schedule a fresh restore.
func (s *PGSandboxStore) listNomadExecutionFailureWork(ctx context.Context, after string, limit int, migrationPredicate, checkpointPredicate string) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 512 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT operation_id FROM (
        SELECT m.operation_id FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id WHERE `+migrationPredicate+`
        UNION ALL SELECT r.operation_id FROM manager.sandbox_runtime_checkpoint_restores r JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
        WHERE l.kind='resume' AND l.source='manual' AND NOT l.cancelable AND l.phase='preparing' AND `+checkpointPredicate+`
        ) work WHERE operation_id COLLATE "C">$1 COLLATE "C" ORDER BY operation_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func decodeNomadExecutionFailureEvidence(ctx context.Context, q executionFailureQuerier, id, key string, into any) error {
	payload, err := readNomadExecutionFailureEvidence(ctx, q, id, key)
	if err != nil {
		return err
	}
	if len(payload) == 0 || json.Unmarshal(payload, into) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
