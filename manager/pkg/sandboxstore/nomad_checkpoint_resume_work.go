package sandboxstore

import "context"

// NomadCheckpointResumeWork is scheduling data only. Restore must revalidate
// this exact lifecycle before acquiring a slot or issuing an execution command.
type NomadCheckpointResumeWork struct {
	OperationID string
	SandboxID   string
}

// ListNomadCheckpointResumes uses bounded keyset scanning, including admitted
// parent restores from running forks and durable RootFS fallbacks. Retained
// images alone never enqueue work; fallback is tied to an accepted resume.
func (s *PGSandboxStore) ListNomadCheckpointResumes(ctx context.Context, after string, limit int) ([]NomadCheckpointResumeWork, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 512 {
		return nil, ErrNomadCheckpointConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT operation_id,sandbox_id FROM (SELECT r.operation_id,l.sandbox_id
        FROM manager.sandbox_lifecycle_txns l
        JOIN manager.sandbox_runtime_checkpoint_restores r ON r.operation_id=l.txn_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE l.kind='resume' AND l.phase IN ('preparing','barriered','publishing','committing')
        AND NOT r.evidence ? 'failure' AND NOT r.evidence ? 'cancel_request'
        AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_resume_fallbacks f WHERE f.operation_id=r.operation_id AND f.reason<>'')
        AND s.desired_state='paused' AND s.deleted_at IS NULL
        AND s.runtime_generation=l.from_generation AND s.lifecycle_epoch=l.epoch
        AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
        UNION
        SELECT f.operation_id,l.sandbox_id FROM manager.sandbox_runtime_resume_fallbacks f
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=f.operation_id
        LEFT JOIN manager.sandbox_lifecycle_txns cold ON cold.txn_id=f.resume_operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE s.desired_state='paused' AND s.deleted_at IS NULL
        AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
        AND ((f.resume_operation_id IS NULL AND s.lifecycle_epoch=l.epoch
            AND (l.phase='aborted' OR f.reason<>''))
            OR (s.lifecycle_epoch=cold.epoch AND s.runtime_generation=cold.from_generation
                AND cold.phase IN ('preparing','barriered','publishing','committing','aborted')))
        ) work WHERE operation_id COLLATE "C">$1 COLLATE "C"
        ORDER BY operation_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []NomadCheckpointResumeWork
	for rows.Next() {
		var item NomadCheckpointResumeWork
		if err := rows.Scan(&item.OperationID, &item.SandboxID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
