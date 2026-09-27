package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

const rootFSStorageTransitionsPerTeam = 1000

type rootFSStorageTransactionRecorder interface {
	RecordStorageObservationTx(context.Context, pgx.Tx, *meteringpkg.StorageObservation) error
	ReconcileZeroStorageObservationTx(context.Context, pgx.Tx, *meteringpkg.StorageObservation) (*meteringpkg.StorageProjectionState, error)
}

// recordRootFSStorageTeam drains committed changes in team revision order. The
// usage row is also the mutation lock used by the database reference triggers;
// a stale scan cannot close storage created concurrently with a deletion.
func (s *PGSandboxStore) recordRootFSStorageTeam(ctx context.Context, recorder rootFSStorageTransactionRecorder, teamID string, observedAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var size int64
	var changedAt time.Time
	err = tx.QueryRow(ctx, `SELECT storage_bytes, changed_at FROM manager.rootfs_storage_usage
		WHERE team_id=$1 FOR UPDATE SKIP LOCKED`, teamID).Scan(&size, &changedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT revision, storage_bytes, changed_at, reconciliation
		FROM manager.rootfs_storage_transitions WHERE team_id=$1 ORDER BY revision LIMIT $2`, teamID, rootFSStorageTransitionsPerTeam)
	if err != nil {
		return err
	}
	type transition struct {
		revision, size int64
		at             time.Time
		reconciliation bool
	}
	var pending []transition
	for rows.Next() {
		var item transition
		if err := rows.Scan(&item.revision, &item.size, &item.at, &item.reconciliation); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range pending {
		observation := rootFSStorageObservation(teamID, item.size, item.at)
		if item.reconciliation && item.size == 0 {
			previous, err := recorder.ReconcileZeroStorageObservationTx(ctx, tx, observation)
			if err != nil {
				return err
			}
			if previous != nil && previous.SizeBytes > 0 {
				payload, err := json.Marshal(previous)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO manager.rootfs_storage_reconciliation_audit
					(team_id,reconciled_at,previous_state,reason) VALUES ($1,$2,$3,'legacy removal boundary unknown')
					ON CONFLICT DO NOTHING`, teamID, item.at, payload); err != nil {
					return err
				}
			}
		} else if err := recorder.RecordStorageObservationTx(ctx, tx, observation); err != nil {
			return err
		}
	}
	if len(pending) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM manager.rootfs_storage_transitions WHERE team_id=$1 AND revision <= $2`, teamID, pending[len(pending)-1].revision); err != nil {
			return err
		}
	}
	// Never sample past an undrained transition: that would make older zero
	// boundaries disappear when the next pass rejects late observations.
	var more bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_storage_transitions WHERE team_id=$1)`, teamID).Scan(&more); err != nil {
		return err
	}
	if !more {
		if observedAt.Before(changedAt) {
			observedAt = changedAt
		}
		// A late legacy bootstrap can resurrect a projection even after the
		// original zero transition was acknowledged. With no pending boundary
		// left, preserve it for audit instead of inventing historical usage.
		if size == 0 && len(pending) == 0 {
			previous, err := recorder.ReconcileZeroStorageObservationTx(ctx, tx, rootFSStorageObservation(teamID, 0, observedAt))
			if err != nil {
				return err
			}
			if previous != nil && previous.SizeBytes > 0 {
				payload, err := json.Marshal(previous)
				if err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO manager.rootfs_storage_reconciliation_audit
					(team_id,reconciled_at,previous_state,reason) VALUES ($1,$2,$3,'legacy projection restored after zero reconciliation')
					ON CONFLICT DO NOTHING`, teamID, observedAt, payload); err != nil {
					return err
				}
			}
		} else if err := recorder.RecordStorageObservationTx(ctx, tx, rootFSStorageObservation(teamID, size, observedAt)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.rootfs_storage_usage SET last_metered_at=clock_timestamp() WHERE team_id=$1`, teamID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func rootFSStorageObservation(teamID string, size int64, at time.Time) *meteringpkg.StorageObservation {
	return &meteringpkg.StorageObservation{
		SubjectType: meteringpkg.SubjectTypeRootFS, SubjectID: teamID,
		Product: meteringpkg.ProductSandbox, TeamID: teamID, SizeBytes: size, ObservedAt: at,
	}
}

// seedOrphanRootFSStorageAccounts also covers projections restored by the
// initial ClickHouse bootstrap after the manager migration has already run.
func (s *PGSandboxStore) seedOrphanRootFSStorageAccounts(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `WITH inserted AS (
		INSERT INTO manager.rootfs_storage_usage (team_id)
		SELECT subject_id FROM metering.storage_projection_state
		WHERE subject_type='rootfs' AND subject_id=team_id AND size_bytes>0
		AND NOT EXISTS (SELECT 1 FROM manager.rootfs_storage_usage usage WHERE usage.team_id=subject_id)
		ORDER BY subject_id LIMIT 1000
		ON CONFLICT DO NOTHING RETURNING team_id,revision,storage_bytes,changed_at
	) INSERT INTO manager.rootfs_storage_transitions (team_id,revision,storage_bytes,changed_at,reconciliation)
	SELECT team_id,revision,storage_bytes,changed_at,TRUE FROM inserted`)
	if err != nil {
		return fmt.Errorf("seed orphan rootfs storage accounts: %w", err)
	}
	return nil
}
