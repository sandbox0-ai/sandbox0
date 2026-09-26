package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
)

// RootFSNodeUploadRequest is an internal authenticated writer capability.
// Uploaded acknowledges a successful conditional PUT; reservation precedes PUT.
type RootFSNodeUploadRequest struct {
	GrantID, NodeUID, OperationID string
	RebaseOperationID             string
	WriterEpoch                   int64
	BindingVersion                int
	BindingDigest                 []byte
	Reference                     rootfsblock.ObjectReference
	Uploaded                      bool
}

func (s *PGSandboxStore) RecordRootFSNodeUpload(ctx context.Context, req *RootFSNodeUploadRequest) error {
	if req == nil || strings.TrimSpace(req.OperationID) != req.OperationID || req.OperationID == "" || len(req.OperationID) > 512 {
		return fmt.Errorf("node upload operation identity is required")
	}
	if err := rootfsblock.ValidateObjectReference(req.Reference); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var team string
	if req.RebaseOperationID != "" {
		if req.OperationID != req.RebaseOperationID || req.GrantID != rootfsrebase.UploadOwnerID(req.OperationID) {
			return ErrRootFSWriterGrantConflict
		}
		var sandbox, node, phase string
		if err = tx.QueryRow(ctx, `SELECT sandbox_id,worker_node_uid,phase FROM manager.sandbox_lifecycle_txns
   WHERE txn_id=$1 AND kind='rebase' AND worker_acknowledged_at IS NULL FOR UPDATE`, req.OperationID).Scan(&sandbox, &node, &phase); err != nil {
			return err
		}
		if node != req.NodeUID || phase != SandboxLifecyclePhasePreparing && phase != SandboxLifecyclePhaseBarriered && phase != SandboxLifecyclePhasePublishing && phase != SandboxLifecyclePhaseCommitting {
			return ErrRootFSWriterGrantConflict
		}
		if err = tx.QueryRow(ctx, `SELECT team_id FROM manager.sandboxes WHERE sandbox_id=$1 AND desired_state='paused'`, sandbox).Scan(&team); err != nil {
			return err
		}
	} else {
		grant, err := getRootFSWriterGrantForUpdate(ctx, tx, req.GrantID)
		if err != nil {
			return err
		}
		if grant.NodeUID != req.NodeUID || grant.WriterEpoch != req.WriterEpoch || grant.BindingVersion != req.BindingVersion || !bytes.Equal(grant.BindingDigest, req.BindingDigest) {
			return ErrRootFSWriterGrantConflict
		}
		if grant.State != RootFSWriterGrantStateConsumed && grant.State != RootFSWriterGrantStateRetiring {
			return ErrRootFSWriterGrantInvalidState
		}
		if err = tx.QueryRow(ctx, `SELECT team_id FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, grant.FilesystemID).Scan(&team); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_node_uploads(grant_id,operation_id,team_id,rebase_operation_id) VALUES($1,$2,$3,NULLIF($4,'')) ON CONFLICT DO NOTHING`, req.GrantID, req.OperationID, team, req.RebaseOperationID)
	if err != nil {
		return err
	}
	var state, owner string
	var rebase *string
	if err = tx.QueryRow(ctx, `SELECT state,team_id,rebase_operation_id FROM manager.rootfs_node_uploads WHERE grant_id=$1 AND operation_id=$2 FOR UPDATE`, req.GrantID, req.OperationID).Scan(&state, &owner, &rebase); err != nil {
		return err
	}
	if state != "uploading" || owner != team || req.RebaseOperationID == "" && rebase != nil || req.RebaseOperationID != "" && (rebase == nil || *rebase != req.RebaseOperationID) {
		return ErrRootFSGenerationConflict
	}
	var deleting bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key=$1)`, req.Reference.Key).Scan(&deleting); err != nil {
		return err
	}
	if deleting {
		return ErrRootFSGenerationConflict
	}
	if !req.Uploaded {
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, req.Reference.Key, req.Reference.Kind, req.Reference.Size, req.Reference.Checksum)
		if err != nil {
			return err
		}
	}
	var kind, checksum string
	var size int64
	if err = tx.QueryRow(ctx, `SELECT object_kind,object_size,checksum FROM manager.rootfs_materialization_objects WHERE object_key=$1 FOR UPDATE`, req.Reference.Key).Scan(&kind, &size, &checksum); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key=$1)`, req.Reference.Key).Scan(&deleting); err != nil {
		return err
	}
	if deleting {
		return ErrRootFSGenerationConflict
	}
	if kind != req.Reference.Kind || size != req.Reference.Size || checksum != req.Reference.Checksum {
		return ErrRootFSGenerationConflict
	}
	if !req.Uploaded {
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_node_upload_objects(grant_id,operation_id,object_key) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, req.GrantID, req.OperationID, req.Reference.Key)
	} else {
		result, e := tx.Exec(ctx, `UPDATE manager.rootfs_node_upload_objects SET uploaded=TRUE WHERE grant_id=$1 AND operation_id=$2 AND object_key=$3`, req.GrantID, req.OperationID, req.Reference.Key)
		err = e
		if err == nil && result.RowsAffected() != 1 {
			return ErrRootFSGenerationConflict
		}
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE manager.rootfs_materialization_objects SET uploaded_at=COALESCE(uploaded_at,clock_timestamp()),updated_at=NOW() WHERE object_key=$1`, req.Reference.Key)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func publishRootFSNodeUploads(ctx context.Context, tx rootFSWriterGrantDB, grantID, operationID string, generation *RootFSGeneration) error {
	// The migration/backfill boundary can still receive legacy publication
	// before node-upload custody exists. New node reservations fail closed until
	// migration 113 is installed; these older publications have no journal.
	var catalogExists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('manager.rootfs_node_uploads') IS NOT NULL`).Scan(&catalogExists); err != nil {
		return err
	}
	if !catalogExists {
		return nil
	}
	var state string
	var previous *string
	err := tx.QueryRow(ctx, `SELECT state,generation_id FROM manager.rootfs_node_uploads WHERE grant_id=$1 AND operation_id=$2 FOR UPDATE`, grantID, operationID).Scan(&state, &previous)
	// Composite and legacy nodes have no direct-upload journal. Inventory adopts
	// legacy objects independently before it releases their ancestry.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "published" && previous != nil && *previous == generation.ID {
		return nil
	}
	if state != "uploading" {
		return ErrRootFSGenerationConflict
	}
	var pending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_node_upload_objects WHERE grant_id=$1 AND operation_id=$2 AND NOT uploaded)`, grantID, operationID).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("%w: node upload acknowledgements are incomplete", ErrRootFSGenerationConflict)
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_generation_materialization_objects(generation_id,locator_version,object_key)
  SELECT $3,$4,pending.object_key FROM manager.rootfs_node_upload_objects pending WHERE grant_id=$1 AND operation_id=$2
  AND NOT EXISTS(SELECT 1 FROM manager.rootfs_base_artifact_objects base WHERE base.artifact_digest=$5 AND base.object_key=pending.object_key)
  ORDER BY pending.object_key ON CONFLICT DO NOTHING`, grantID, operationID, generation.ID, generation.LocatorVersion, generation.BaseArtifactDigest)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE manager.rootfs_generations SET storage_inventory_required=FALSE WHERE generation_id=$1`, generation.ID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_node_upload_objects WHERE grant_id=$1 AND operation_id=$2`, grantID, operationID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE manager.rootfs_node_uploads SET state='published',generation_id=$3,updated_at=clock_timestamp() WHERE grant_id=$1 AND operation_id=$2`, grantID, operationID, generation.ID)
	return err
}

// Terminal owner proof is durable. The additional quiet interval exceeds the
// node's bounded PUT deadline, including a lost upload acknowledgement. Each
// pass releases at most limit objects, even for a very large abandoned build.
func (s *PGSandboxStore) ReconcileRootFSNodeUploadGarbage(ctx context.Context, team string, limit int) error {
	limit = normalizeRootFSObjectLimit(limit)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_node_uploads WHERE (grant_id,operation_id) IN
  (SELECT grant_id,operation_id FROM manager.rootfs_node_uploads WHERE state IN ('published','abandoned')
   AND terminal_at IS NOT NULL AND ($2='' OR team_id=$2) AND GREATEST(updated_at,terminal_at)<clock_timestamp()-INTERVAL '2 days' ORDER BY GREATEST(updated_at,terminal_at),grant_id,operation_id LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit, team)
	if err != nil {
		return err
	}
	var grant, operation, owner string
	err = tx.QueryRow(ctx, `SELECT grant_id,operation_id,team_id FROM manager.rootfs_node_uploads
  WHERE state='uploading' AND terminal_at<clock_timestamp()-INTERVAL '2 minutes' AND ($1='' OR team_id=$1)
  ORDER BY terminal_at,grant_id,operation_id LIMIT 1 FOR UPDATE SKIP LOCKED`, team).Scan(&grant, &operation, &owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT object_key FROM manager.rootfs_node_upload_objects WHERE grant_id=$1 AND operation_id=$2 ORDER BY object_key LIMIT $3`, grant, operation, limit)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, key := range keys {
		_, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_node_upload_objects WHERE grant_id=$1 AND operation_id=$2 AND object_key=$3`, grant, operation, key)
		if err != nil {
			return err
		}
		_, err = releaseUnreferencedRootFSMaterializationObject(ctx, tx, key, owner)
		if err != nil {
			return err
		}
		// A successful PUT may have lost its acknowledgement. Deleting an absent
		// S3 key is idempotent; never omit cleanup merely because uploaded_at is NULL.
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_object_deletions(object_key,team_id,next_attempt_at)
   SELECT $1,$2,NOW() WHERE NOT EXISTS(SELECT 1 FROM manager.rootfs_materialization_objects WHERE object_key=$1)
   ON CONFLICT DO NOTHING`, key, owner)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE manager.rootfs_node_uploads SET state='abandoned',updated_at=clock_timestamp()
  WHERE grant_id=$1 AND operation_id=$2 AND NOT EXISTS(SELECT 1 FROM manager.rootfs_node_upload_objects WHERE grant_id=$1 AND operation_id=$2)`, grant, operation)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
