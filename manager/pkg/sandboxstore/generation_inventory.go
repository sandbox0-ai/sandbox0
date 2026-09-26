package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// InventoryRootFSGeneration advances one generation by at most pageLimit
// authenticated mapping pages. Its durable queue bounds memory and resumes
// after restarts; ancestry remains protected until the inventory commits.
func (s *PGSandboxStore) InventoryRootFSGeneration(ctx context.Context, source rootfsblock.RangeSource, pageLimit int) (bool, error) {
	return s.inventoryRootFSGeneration(ctx, source, "", pageLimit)
}

// InventoryRootFSGenerationForTeam advances only the explicit operator cohort.
// An empty team is rejected so a canary cannot accidentally scan all tenants.
func (s *PGSandboxStore) InventoryRootFSGenerationForTeam(ctx context.Context, source rootfsblock.RangeSource, teamID string, pageLimit int) (bool, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return false, fmt.Errorf("inventory team is required")
	}
	return s.inventoryRootFSGeneration(ctx, source, teamID, pageLimit)
}

func (s *PGSandboxStore) inventoryRootFSGeneration(ctx context.Context, source rootfsblock.RangeSource, teamID string, pageLimit int) (completed bool, err error) {
	if source == nil || pageLimit < 1 || pageLimit > 1000 {
		return false, fmt.Errorf("inventory source and page limit between 1 and 1000 are required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	started := time.Now()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id string
	var filesystemID string
	var version int64
	var payload []byte
	// Publication locks the filesystem before its generation. Take compatible
	// filesystem custody first too: generation constraint triggers recheck this
	// FK on inventory updates, so the reverse order could deadlock publication.
	err = tx.QueryRow(ctx, `SELECT generation.generation_id,filesystem.filesystem_id
		FROM manager.rootfs_filesystems filesystem JOIN manager.rootfs_generations generation USING (filesystem_id)
		WHERE ($1='' OR filesystem.team_id=$1) AND NOT inventory_complete AND durability_state='s3_materialized'
		AND (storage_inventory_required OR (`+rootFSGenerationRetainedSQL+`) OR EXISTS (
			SELECT 1 FROM manager.rootfs_generation_inventory_pages pending WHERE pending.generation_id=generation.generation_id))
		ORDER BY COALESCE(inventory_attempted_at,generation.created_at),generation.generation_id LIMIT 1
		FOR KEY SHARE OF filesystem SKIP LOCKED`, teamID).Scan(&id, &filesystemID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `SELECT locator_version,descriptor FROM manager.rootfs_generations generation
		WHERE generation_id=$1 AND filesystem_id=$2 AND NOT inventory_complete AND durability_state='s3_materialized'
		AND (storage_inventory_required OR (`+rootFSGenerationRetainedSQL+`) OR EXISTS (
			SELECT 1 FROM manager.rootfs_generation_inventory_pages pending WHERE pending.generation_id=generation.generation_id))
		FOR SHARE SKIP LOCKED`, id, filesystemID).Scan(&version, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Readers and writer handoffs may share the immutable locator while this
	// worker performs network I/O. Only other inventory workers are excluded;
	// the shared lock/FKs prevent collection or locator changes until commit.
	var acquired bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended('rootfs-inventory/' || $1,0))`, id).Scan(&acquired); err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
			// Rotate a failed root so one corrupt or unavailable mapping does
			// not prevent other teams from making progress.
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Error rotation must obey the same filesystem-before-generation
			// order and skip a busy publisher; an outage cannot reverse it.
			_, _ = s.pool.Exec(cleanup, `WITH owner AS (
				SELECT filesystem_id FROM manager.rootfs_filesystems WHERE filesystem_id=$2 FOR KEY SHARE SKIP LOCKED
			) UPDATE manager.rootfs_generations generation SET inventory_attempted_at=clock_timestamp()
			FROM owner WHERE generation.generation_id=$1 AND generation.filesystem_id=owner.filesystem_id`, id, filesystemID)
		}
	}()
	descriptor, err := rootfsblock.DecodeDescriptor(payload)
	if err != nil || descriptor.CompositeTail != nil {
		return false, fmt.Errorf("inventory generation %s is not a valid materialized descriptor: %v", id, err)
	}
	if descriptor.MappingRoot.RootDigest != descriptor.MappingRoot.Object.Checksum {
		return false, fmt.Errorf("inventory generation %s mapping root digest mismatch", id)
	}
	var initialized bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_generation_inventory_pages WHERE generation_id=$1)`, id).Scan(&initialized); err != nil {
		return false, err
	}
	if !initialized {
		locator, encodeErr := json.Marshal(descriptor.MappingRoot.Object)
		if encodeErr != nil {
			return false, encodeErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_generation_inventory_pages
			(generation_id,locator_version,locator,start_block,block_count,expected_level)
			VALUES ($1,$2,$3,0,$4,-1)`, id, version, locator, descriptor.LogicalSizeBytes/descriptor.BlockSizeBytes); err != nil {
			return false, err
		}
	}
	for range pageLimit {
		var pageID, storedVersion, start, count int64
		var level int
		var locator []byte
		err = tx.QueryRow(ctx, `SELECT page_id,locator_version,locator,start_block,block_count,expected_level
			FROM manager.rootfs_generation_inventory_pages WHERE generation_id=$1 ORDER BY page_id LIMIT 1`, id).
			Scan(&pageID, &storedVersion, &locator, &start, &count, &level)
		if errors.Is(err, pgx.ErrNoRows) {
			err = nil
			break
		}
		if err != nil {
			return false, err
		}
		if storedVersion != version {
			return false, fmt.Errorf("generation %s inventory locator changed", id)
		}
		var object rootfsblock.ObjectRange
		if err = json.Unmarshal(locator, &object); err != nil {
			return false, err
		}
		page, readErr := rootfsblock.InspectInventoryMappingPage(ctx, source, descriptor.Version, object, uint64(start), uint64(count), level)
		if readErr != nil {
			return false, readErr
		}
		if err = recordRootFSInventoryPage(ctx, tx, source, id, version, object, page); errors.Is(err, errRootFSLegacyInventoryPending) {
			_, err = tx.Exec(ctx, `UPDATE manager.rootfs_generations SET inventory_attempted_at=clock_timestamp() WHERE generation_id=$1`, id)
			if err != nil {
				return false, err
			}
			return false, tx.Commit(ctx)
		} else if err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_generation_inventory_pages WHERE generation_id=$1 AND page_id=$2`, id, pageID); err != nil {
			return false, err
		}
		if time.Since(started) >= 2*time.Second {
			break
		}
	}
	var more bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_generation_inventory_pages WHERE generation_id=$1)`, id).Scan(&more); err != nil {
		return false, err
	}
	if !more {
		if _, err = tx.Exec(ctx, `UPDATE manager.rootfs_generations SET inventory_complete=TRUE,storage_inventory_required=FALSE WHERE generation_id=$1`, id); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE manager.rootfs_generations SET inventory_attempted_at=clock_timestamp() WHERE generation_id=$1`, id); err != nil {
		return false, err
	}
	return !more, tx.Commit(ctx)
}

func recordRootFSInventoryPage(ctx context.Context, tx pgx.Tx, source rootfsblock.RangeSource, id string, version int64, object rootfsblock.ObjectRange, page rootfsblock.MappingPage) error {
	type reference struct {
		Key  string `json:"key"`
		Kind string `json:"kind"`
		End  int64  `json:"end"`
	}
	references := []reference{{object.Key, rootfsblock.ObjectKindMappingPage, object.Offset + object.StoredLength()}}
	for _, entry := range page.Entries {
		kind := rootfsblock.ObjectKindDataPack
		if entry.Kind == rootfsblock.MappingEntryChild {
			kind = rootfsblock.ObjectKindMappingPage
		}
		references = append(references, reference{entry.Object.Key, kind, entry.Object.Offset + entry.Object.StoredLength()})
	}
	encoded, err := json.Marshal(references)
	if err != nil {
		return err
	}
	if err := adoptRootFSLegacyInventoryObjects(ctx, tx, source, id, encoded); err != nil {
		return err
	}
	for _, entry := range page.Entries {
		if entry.Kind != rootfsblock.MappingEntryChild {
			continue
		}
		locator, err := json.Marshal(entry.Object)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO manager.rootfs_generation_inventory_pages
				(generation_id,locator_version,locator,start_block,block_count,expected_level)
				VALUES ($1,$2,$3,$4,$5,$6)`, id, version, locator, int64(entry.LogicalStart), int64(entry.BlockCount), int(page.Level)-1); err != nil {
			return err
		}
	}

	var valid bool
	if err := tx.QueryRow(ctx, `SELECT NOT EXISTS (
		SELECT 1 FROM jsonb_to_recordset($1::jsonb) AS reference(key TEXT,kind TEXT,"end" BIGINT)
		LEFT JOIN manager.rootfs_materialization_objects object ON object.object_key=reference.key
		WHERE object.uploaded_at IS NULL OR object.object_kind<>reference.kind OR object.object_size<reference."end")`, encoded).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("generation %s mapping refers to an unregistered, unuploaded or invalid object", id)
	}
	// Commit each page's bounded references immediately. Until the last page,
	// the ancestry FK still protects every undiscovered inherited object. This
	// avoids one final transaction inserting a whole generation's object graph.
	// Base objects have independent platform custody and are not tenant bytes.
	_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_generation_materialization_objects (generation_id,locator_version,object_key)
		SELECT DISTINCT $1::TEXT,$2::BIGINT,reference.key FROM jsonb_to_recordset($3::jsonb) AS reference(key TEXT)
		WHERE NOT EXISTS (SELECT 1 FROM manager.rootfs_base_artifact_objects base
			JOIN manager.rootfs_generations generation ON generation.base_artifact_digest=base.artifact_digest
			WHERE generation.generation_id=$1 AND base.object_key=reference.key)
		ORDER BY reference.key ON CONFLICT DO NOTHING`, id, version, encoded)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM manager.rootfs_inventory_object_custody custody
  WHERE generation_id=$1 AND object_key IN (SELECT reference.key FROM jsonb_to_recordset($2::jsonb) AS reference(key TEXT))`, id, encoded)
	return err
}
