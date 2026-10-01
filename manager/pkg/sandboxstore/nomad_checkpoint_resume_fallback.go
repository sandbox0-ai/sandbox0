package sandboxstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrNomadCheckpointFallbackQuotaRequired requests quota resolution only when
// admitting a new cold lifecycle; retries never consult mutable quota policy.
var ErrNomadCheckpointFallbackQuotaRequired = errors.New("RootFS fallback requires current quota policy")

// ResolveNomadCheckpointResumeFallback advances scheduling intent, never waiving
// execution fencing. An empty reason probes existing work; a reason requests
// pre-execution cancellation (or physical failure cleanup after authorization).
// handled with an empty operation means cleanup is still pending. Stale work
// cannot reserve another generation after the owner has moved on.
func (s *PGSandboxStore) ResolveNomadCheckpointResumeFallback(ctx context.Context, sandboxID, operation, reason string, limit *int64, quotaResolved bool) (next string, handled bool, err error) {
	var team string
	if err := s.pool.QueryRow(ctx, `SELECT team_id FROM manager.sandboxes WHERE sandbox_id=$1`, sandboxID).Scan(&team); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, ErrSandboxRecordNotFound
		}
		return "", false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)
	if err := lockActiveSandboxQuotaTeam(ctx, tx, team); err != nil {
		return "", false, err
	}
	owner, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return "", false, err
	}
	var source, replacement, savedReason string
	err = tx.QueryRow(ctx, `SELECT f.operation_id,COALESCE(f.resume_operation_id,''),f.reason
        FROM manager.sandbox_runtime_resume_fallbacks f
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=f.operation_id
        LEFT JOIN manager.sandbox_lifecycle_txns cold ON cold.txn_id=f.resume_operation_id
        WHERE l.sandbox_id=$1 AND ($2='' OR f.operation_id=$2)
        AND (l.epoch=$3 OR cold.epoch=$3)
        ORDER BY l.epoch DESC LIMIT 1 FOR UPDATE OF f`, sandboxID, operation, owner.LifecycleEpoch).Scan(&source, &replacement, &savedReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if owner.DesiredState != SandboxDesiredStatePaused || !owner.DeletedAt.IsZero() {
		return "", false, nil
	}
	var nowExpired bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(hard_expires_at<=clock_timestamp(),false) FROM manager.sandboxes WHERE sandbox_id=$1`, sandboxID).Scan(&nowExpired); err != nil {
		return "", false, err
	}
	if nowExpired {
		return "", false, nil
	}
	if replacement != "" {
		life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, replacement))
		if err != nil {
			return "", false, err
		}
		if life.Phase == SandboxLifecyclePhaseCommitted {
			return "", false, nil
		}
		if life.Phase != SandboxLifecyclePhaseAborted {
			return replacement, true, tx.Commit(ctx)
		}
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, source))
	if err != nil {
		return "", false, err
	}
	if life.Phase == SandboxLifecyclePhaseCommitted {
		return "", false, nil
	}
	if reason == "" && savedReason == "" && life.Phase != SandboxLifecyclePhaseAborted {
		// CPU preflight can fail before image custody exists. Its disposable
		// carrier retries must not extend an accepted memory attempt forever.
		// Image preparation and authorized execution keep their existing exact
		// lease deadlines and physical cancellation/failure protocols.
		var preImageExpired bool
		if err := tx.QueryRow(ctx, `SELECT f.created_at<=clock_timestamp()-INTERVAL '2 minutes'
			AND NOT r.evidence ? 'image' AND NOT r.evidence ? 'restore'
			FROM manager.sandbox_runtime_resume_fallbacks f
			JOIN manager.sandbox_runtime_checkpoint_restores r ON r.operation_id=f.operation_id
			WHERE f.operation_id=$1`, source).Scan(&preImageExpired); err != nil {
			return "", false, err
		}
		if preImageExpired {
			reason = "memory restore could not complete pre-image admission within two minutes"
		}
	}
	if reason != "" && savedReason == "" {
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_resume_fallbacks SET reason=$2 WHERE operation_id=$1`, source, reason); err != nil {
			return "", false, err
		}
		savedReason = reason
	}
	if life.Phase != SandboxLifecyclePhaseAborted {
		if savedReason == "" {
			return "", false, tx.Commit(ctx)
		}
		var executed bool
		if err := tx.QueryRow(ctx, `SELECT evidence ? 'restore' FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, source).Scan(&executed); err != nil {
			return "", false, err
		}
		if executed {
			// The existing failure worker owns stopping and reclaiming this
			// target. An uncertain reply is never treated as physical absence.
			if err := quiesceAbortedNomadResumeSlots(ctx, tx, sandboxID, source); err != nil {
				return "", false, err
			}
		} else {
			if _, err := beginNomadCheckpointRestoreCancellation(ctx, tx, owner, life, savedReason); err != nil {
				return "", false, err
			}
		}
		life, err = scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1`, source))
		if err != nil {
			return "", false, err
		}
		if life.Phase != SandboxLifecyclePhaseAborted {
			return "", true, tx.Commit(ctx)
		}
	}
	if err := ensureNomadResumePhysicalStateTerminal(ctx, tx, sandboxID); err != nil {
		if errors.Is(err, ErrNomadSandboxResumeNotReady) {
			return "", true, tx.Commit(ctx)
		}
		return "", false, err
	}
	if !quotaResolved {
		if err := tx.Commit(ctx); err != nil {
			return "", false, err
		}
		return "", true, ErrNomadCheckpointFallbackQuotaRequired
	}
	candidate, err := requestNomadSandboxResumeTx(ctx, tx, &RequestNomadSandboxResumeRequest{
		SandboxID: sandboxID, ExpectedTeamID: team, ActiveSandboxLimit: limit,
	})
	if err != nil {
		return "", false, err
	}
	if candidate.Checkpoint != nil || candidate.AlreadyActive {
		return "", false, ErrNomadCheckpointConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_resume_fallbacks SET resume_operation_id=$2 WHERE operation_id=$1`, source, candidate.OperationID); err != nil {
		return "", false, err
	}
	return candidate.OperationID, true, tx.Commit(ctx)
}

// NomadCheckpointResumeFallbackPending is a status projection of durable work.
// Waiters keep waiting through memory cleanup instead of treating a recoverable
// restore failure as the terminal result of the requested resume.
func (s *PGSandboxStore) NomadCheckpointResumeFallbackPending(ctx context.Context, sandbox string, generation, epoch int64) (bool, error) {
	var pending bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_resume_fallbacks f
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=f.operation_id
        LEFT JOIN manager.sandbox_lifecycle_txns cold ON cold.txn_id=f.resume_operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE s.sandbox_id=$1 AND s.runtime_generation=$2 AND s.lifecycle_epoch=$3
        AND s.desired_state='paused' AND s.deleted_at IS NULL
        AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
        AND ((f.resume_operation_id IS NULL AND l.epoch=s.lifecycle_epoch
            AND (l.phase='aborted' OR f.reason<>'' OR l.phase='preparing'))
            OR (cold.epoch=s.lifecycle_epoch AND cold.phase<>'committed'))
    )`, sandbox, generation, epoch).Scan(&pending)
	return pending, err
}

// NomadSandboxPausePending recognizes a durable pause while the
// source exits, including a committed pause observed through an older active
// owner snapshot. Physically resolved capture failures remain terminal errors.
func (s *PGSandboxStore) NomadSandboxPausePending(ctx context.Context, sandbox string, generation, epoch int64) (bool, error) {
	var pending bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM manager.sandbox_lifecycle_txns l
  LEFT JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=l.txn_id
  JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
  WHERE l.sandbox_id=$1 AND l.from_generation=$2 AND l.epoch=$3 AND l.kind='pause'
  AND l.phase IN ('preparing','barriered','publishing','committing','committed')
  AND (c.operation_id IS NULL OR (NOT c.evidence ? 'capture_failure' AND NOT c.evidence ? 'cancel_authorized'))
  AND s.runtime_generation=$2 AND s.lifecycle_epoch=$3 AND s.deleted_at IS NULL
  AND s.desired_state IN ('active','paused')
 )`, sandbox, generation, epoch).Scan(&pending)
	return pending, err
}
