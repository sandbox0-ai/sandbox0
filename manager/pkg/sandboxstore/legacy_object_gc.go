package sandboxstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Deferred retirement survives worker restarts and lost upload acknowledgements.
// One indexed gate check and at most limit rows are visited in a pass.
func (s *PGSandboxStore) reconcileRootFSLegacyObjectGCHolds(ctx context.Context, limit int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var unknown bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_generations WHERE storage_inventory_required)`).Scan(&unknown); err != nil {
		return err
	}
	if unknown {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT object_key,team_id FROM manager.rootfs_legacy_object_gc_holds
		ORDER BY created_at,object_key LIMIT $1 FOR UPDATE SKIP LOCKED`, normalizeRootFSObjectLimit(limit))
	if err != nil {
		return err
	}
	type held struct{ key, team string }
	var objects []held
	for rows.Next() {
		var object held
		if err = rows.Scan(&object.key, &object.team); err != nil {
			rows.Close()
			return err
		}
		objects = append(objects, object)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if _, err = releaseUnreferencedRootFSMaterializationObject(ctx, tx, object.key, object.team); err != nil {
			return err
		}
		// A terminal reservation can have lost its successful PUT acknowledgement.
		// Preserve node GC's idempotent deletion even when uploaded_at is NULL.
		if _, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_object_deletions(object_key,team_id,next_attempt_at)
			SELECT $1,$2,NOW() WHERE NOT EXISTS(SELECT 1 FROM manager.rootfs_materialization_objects WHERE object_key=$1)
			ON CONFLICT DO NOTHING`, object.key, object.team); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Keep the publication gate and exact queue claim through the external DELETE.
// Registered publishers reserve catalog custody before PUT. Unknown legacy
// publication cannot appear between this final check and deletion.
func (s *PGSandboxStore) deleteClaimedRootFSObject(ctx context.Context, deleter RootFSObjectDeleter, item claimedRootFSObjectDeletion, opts DeletePendingRootFSObjectsOptions) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('rootfs-legacy-inventory-gc',0))`); err != nil {
		return false, err
	}
	var unknown bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_generations WHERE storage_inventory_required)`).Scan(&unknown); err != nil {
		return false, err
	}
	if unknown {
		_, err = tx.Exec(ctx, `UPDATE manager.rootfs_object_deletions SET claimed_by='',claimed_until=NULL,next_attempt_at=NOW()+INTERVAL '1 minute',updated_at=NOW() WHERE object_key=$1 AND claimed_by=$2`, item.ObjectKey, opts.ClaimedBy)
		if err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	}
	var key string
	if err = tx.QueryRow(ctx, `SELECT object_key FROM manager.rootfs_object_deletions WHERE object_key=$1 AND claimed_by=$2 AND claimed_until>clock_timestamp() FOR UPDATE`, item.ObjectKey, opts.ClaimedBy).Scan(&key); err == pgx.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	var registered bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_materialization_objects WHERE object_key=$1)`, key).Scan(&registered); err != nil {
		return false, err
	}
	if !registered {
		if contextual, ok := deleter.(interface {
			DeleteContext(context.Context, string) error
		}); ok {
			err = contextual.DeleteContext(ctx, key)
		} else {
			err = deleter.Delete(key)
		}
		if err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_object_deletions WHERE object_key=$1 AND claimed_by=$2`, key, opts.ClaimedBy); err != nil {
		return false, err
	}
	return !registered, tx.Commit(ctx)
}
