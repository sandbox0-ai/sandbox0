package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const (
	defaultRootFSInventoryCompactLimit = 4
	maxRootFSInventoryMetadataBytes    = 64 << 20
)

// RootFSObjectReader reads immutable rootfs objects. The object-store
// implementation satisfies this interface directly.
type RootFSObjectReader interface {
	Get(key string, offset, limit int64) (io.ReadCloser, error)
}

// RootFSInventoryCandidate is a live metadata head whose initially recorded
// object set still includes generation-local or ancestor references.
type RootFSInventoryCandidate struct {
	LayerID       string
	TeamID        string
	Head          rootfshead.HeadReference
	ImageEnvelope rootfshead.Object
}

// ListRootFSInventoryCandidates returns live metadata heads that need an
// asynchronous exact object inventory. This work is deliberately kept off the
// pause, resume, and claim paths.
func (s *PGSandboxStore) ListRootFSInventoryCandidates(ctx context.Context, limit int) ([]RootFSInventoryCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = defaultRootFSInventoryCompactLimit
	}
	if limit > maxRootFSObjectDeleteLimit {
		limit = maxRootFSObjectDeleteLimit
	}
	rows, err := s.pool.Query(ctx, `
		WITH roots AS (
			SELECT f.head_layer_id AS layer_id
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE f.head_layer_id IS NOT NULL
			UNION
			SELECT head_layer_id AS layer_id
			FROM manager.rootfs_snapshots
			WHERE head_layer_id IS NOT NULL
				AND (expires_at IS NULL OR expires_at > NOW())
		)
		SELECT l.layer_id, l.team_id, l.head_object_digest,
			l.head_object_media_type, l.head_object_size, l.head_object_key,
			COALESCE(envelope.object_key, ''), COALESCE(envelope.diff_digest, ''),
			COALESCE(envelope.diff_size, 0), COALESCE(envelope.diff_media_type, '')
		FROM roots r
		JOIN manager.rootfs_layers l ON l.layer_id = r.layer_id
		LEFT JOIN LATERAL (
			SELECT object.object_key, object.diff_digest, object.diff_size, object.diff_media_type
			FROM manager.rootfs_layer_objects relation
			JOIN manager.rootfs_objects object ON object.object_key = relation.object_key
			WHERE relation.layer_id = l.layer_id
				AND object.diff_media_type = $2
			ORDER BY object.object_key ASC
			LIMIT 1
		) envelope ON TRUE
		WHERE l.object_inventory_complete = FALSE
			AND l.head_object_key <> ''
		ORDER BY l.created_at ASC
		LIMIT $1
	`, limit, rootfshead.ImageEnvelopeMediaType)
	if err != nil {
		return nil, fmt.Errorf("list rootfs inventory candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]RootFSInventoryCandidate, 0, limit)
	for rows.Next() {
		var candidate RootFSInventoryCandidate
		candidate.Head.Version = rootfshead.Version
		candidate.Head.Manifest.MediaType = rootfshead.HeadMediaType
		if err := rows.Scan(
			&candidate.LayerID,
			&candidate.TeamID,
			&candidate.Head.Manifest.Digest,
			&candidate.Head.Manifest.MediaType,
			&candidate.Head.Manifest.Size,
			&candidate.Head.Manifest.Key,
			&candidate.ImageEnvelope.Key,
			&candidate.ImageEnvelope.Digest,
			&candidate.ImageEnvelope.Size,
			&candidate.ImageEnvelope.MediaType,
		); err != nil {
			return nil, err
		}
		candidate.Head.HeadID = candidate.LayerID
		if err := candidate.Head.Validate(); err != nil {
			return nil, fmt.Errorf("validate rootfs inventory candidate %s: %w", candidate.LayerID, err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs inventory candidates: %w", err)
	}
	return candidates, nil
}

// CompleteRootFSObjectInventory atomically replaces a generation's
// conservative object set with the exact objects reachable from its complete
// metadata head. Objects removed by compaction are queued for durable S3
// deletion when no other layer references them.
func (s *PGSandboxStore) CompleteRootFSObjectInventory(ctx context.Context, candidate RootFSInventoryCandidate, objects []rootfshead.Object) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	if strings.TrimSpace(candidate.LayerID) == "" || strings.TrimSpace(candidate.TeamID) == "" {
		return false, fmt.Errorf("rootfs inventory candidate layer and team are required")
	}
	if err := candidate.Head.Validate(); err != nil {
		return false, err
	}
	objects, err := normalizeRootFSInventory(objects)
	if err != nil {
		return false, err
	}
	if !rootFSInventoryContains(objects, candidate.Head.Manifest.Key) {
		return false, fmt.Errorf("rootfs inventory for layer %s omits head object", candidate.LayerID)
	}
	marker, _, err := rootfshead.MarkerObject(candidate.Head)
	if err != nil {
		return false, err
	}
	if !rootFSInventoryContains(objects, marker.Key) {
		return false, fmt.Errorf("rootfs inventory for layer %s omits marker object", candidate.LayerID)
	}
	if strings.TrimSpace(candidate.ImageEnvelope.Key) != "" && !rootFSInventoryContains(objects, candidate.ImageEnvelope.Key) {
		return false, fmt.Errorf("rootfs inventory for layer %s omits image envelope", candidate.LayerID)
	}
	payload, err := json.Marshal(objects)
	if err != nil {
		return false, fmt.Errorf("marshal exact rootfs object inventory: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin rootfs inventory completion tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var teamID string
	var headDigest string
	var headMediaType string
	var headSize int64
	var headKey string
	var complete bool
	err = tx.QueryRow(ctx, `
		SELECT team_id, head_object_digest, head_object_media_type,
			head_object_size, head_object_key, object_inventory_complete
		FROM manager.rootfs_layers
		WHERE layer_id = $1
		FOR UPDATE
	`, candidate.LayerID).Scan(&teamID, &headDigest, &headMediaType, &headSize, &headKey, &complete)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock rootfs inventory candidate %s: %w", candidate.LayerID, err)
	}
	if complete {
		return false, nil
	}
	if teamID != candidate.TeamID || headDigest != candidate.Head.Manifest.Digest ||
		headMediaType != candidate.Head.Manifest.MediaType || headSize != candidate.Head.Manifest.Size ||
		headKey != candidate.Head.Manifest.Key {
		return false, fmt.Errorf("%w: rootfs inventory candidate %s changed", ErrRootFSHeadConflict, candidate.LayerID)
	}

	var inputCount int64
	var upsertedCount int64
	var completedCount int64
	err = tx.QueryRow(ctx, `
		WITH input AS MATERIALIZED (
			SELECT object.key AS object_key,
				object.digest AS object_digest,
				object.media_type,
				object.size AS object_size
			FROM jsonb_to_recordset($1::jsonb) AS object(
				key TEXT,
				digest TEXT,
				size BIGINT,
				media_type TEXT
			)
		),
		upserted AS (
			INSERT INTO manager.rootfs_objects (
				object_key, team_id, diff_digest, diff_media_type, diff_size,
				first_layer_id, last_referenced_at, missing_at, deleted_at,
				last_error, created_at, updated_at
			)
			SELECT object_key, $2, object_digest, media_type, object_size,
				$3, NOW(), NULL, NULL, '', NOW(), NOW()
			FROM input
			ON CONFLICT (object_key) DO UPDATE SET
				last_referenced_at = NOW(),
				missing_at = NULL,
				deleted_at = NULL,
				last_error = '',
				updated_at = NOW()
			WHERE manager.rootfs_objects.team_id = EXCLUDED.team_id
				AND manager.rootfs_objects.diff_digest = EXCLUDED.diff_digest
				AND manager.rootfs_objects.diff_media_type = EXCLUDED.diff_media_type
				AND manager.rootfs_objects.diff_size = EXCLUDED.diff_size
			RETURNING object_key
		),
		stale AS MATERIALIZED (
			SELECT relation.object_key, object.team_id
			FROM manager.rootfs_layer_objects relation
			JOIN manager.rootfs_objects object ON object.object_key = relation.object_key
			WHERE relation.layer_id = $3
				AND NOT EXISTS (
					SELECT 1 FROM input
					WHERE input.object_key = relation.object_key
				)
		),
		queued AS (
			INSERT INTO manager.rootfs_object_deletions (
				object_key, team_id, attempts, last_error,
				next_attempt_at, claimed_by, claimed_until, dead_lettered_at,
				created_at, updated_at
			)
			SELECT stale.object_key, stale.team_id, 0, '', NOW(), '', NULL, NULL, NOW(), NOW()
			FROM stale
			WHERE NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_layer_objects other
				WHERE other.object_key = stale.object_key
					AND other.layer_id <> $3
			)
			ON CONFLICT (object_key) DO UPDATE SET
				team_id = EXCLUDED.team_id,
				attempts = 0,
				last_error = '',
				next_attempt_at = NOW(),
				claimed_by = '',
				claimed_until = NULL,
				dead_lettered_at = NULL,
				updated_at = NOW()
		),
		removed AS (
			DELETE FROM manager.rootfs_layer_objects relation
			WHERE relation.layer_id = $3
				AND NOT EXISTS (
					SELECT 1 FROM input
					WHERE input.object_key = relation.object_key
				)
		),
		linked AS (
			INSERT INTO manager.rootfs_layer_objects (layer_id, object_key, created_at)
			SELECT $3, object_key, NOW()
			FROM upserted
			ON CONFLICT (layer_id, object_key) DO NOTHING
		),
		completed AS (
			UPDATE manager.rootfs_layers
			SET object_inventory_complete = TRUE,
				object_inventory_completed_at = NOW()
			WHERE layer_id = $3
				AND object_inventory_complete = FALSE
			RETURNING layer_id
		)
		SELECT
			(SELECT COUNT(*) FROM input),
			(SELECT COUNT(*) FROM upserted),
			(SELECT COUNT(*) FROM completed)
	`, payload, candidate.TeamID, candidate.LayerID).Scan(&inputCount, &upsertedCount, &completedCount)
	if err != nil {
		return false, fmt.Errorf("replace rootfs object inventory for %s: %w", candidate.LayerID, err)
	}
	if inputCount != int64(len(objects)) || upsertedCount != inputCount {
		return false, fmt.Errorf("%w: accepted %d of %d exact objects", ErrRootFSObjectConflict, upsertedCount, inputCount)
	}
	if completedCount != 1 {
		return false, fmt.Errorf("%w: rootfs inventory candidate %s was not completed", ErrRootFSHeadConflict, candidate.LayerID)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit rootfs object inventory for %s: %w", candidate.LayerID, err)
	}
	return true, nil
}

// CollectRootFSObjectInventory traverses only metadata objects. File chunks
// are inventoried from manifests without downloading file contents, keeping
// compaction proportional to metadata and outside lifecycle latency.
func CollectRootFSObjectInventory(ctx context.Context, reader RootFSObjectReader, candidate RootFSInventoryCandidate) ([]rootfshead.Object, error) {
	if reader == nil {
		return nil, fmt.Errorf("rootfs object reader is required")
	}
	if err := candidate.Head.Validate(); err != nil {
		return nil, err
	}
	prefix, err := rootFSInventoryPrefix(candidate.Head.Manifest.Key)
	if err != nil {
		return nil, err
	}

	objects := make(map[string]rootfshead.Object)
	pending := make([]rootfshead.Object, 0, 128)
	enqueue := func(object rootfshead.Object) error {
		if err := object.Validate(object.MediaType); err != nil {
			return err
		}
		if !strings.HasPrefix(object.Key, prefix+"/") {
			return fmt.Errorf("rootfs object %s escapes filesystem prefix %s", object.Key, prefix)
		}
		if existing, ok := objects[object.Key]; ok {
			if existing != object {
				return fmt.Errorf("rootfs object %s has conflicting descriptors", object.Key)
			}
			return nil
		}
		objects[object.Key] = object
		if object.MediaType != rootfshead.ChunkMediaType {
			pending = append(pending, object)
		}
		return nil
	}
	if err := enqueue(candidate.Head.Manifest); err != nil {
		return nil, err
	}

	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		object := pending[0]
		pending = pending[1:]
		payload, err := readRootFSInventoryObject(ctx, reader, object)
		if err != nil {
			return nil, err
		}
		switch object.MediaType {
		case rootfshead.HeadMediaType:
			head, err := rootfshead.DecodeHead(bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			if head.HeadID != candidate.LayerID {
				return nil, fmt.Errorf("rootfs head id %s does not match layer %s", head.HeadID, candidate.LayerID)
			}
			if head.Root.Directory == nil {
				return nil, fmt.Errorf("rootfs head %s has no root directory", candidate.LayerID)
			}
			if err := enqueue(*head.Root.Directory); err != nil {
				return nil, err
			}
		case rootfshead.DirectoryIndexMediaType:
			index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			for _, shard := range index.Shards {
				if err := enqueue(shard.Object); err != nil {
					return nil, err
				}
			}
		case rootfshead.DirectoryShardMediaType:
			shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			for _, entry := range shard.Entries {
				if entry.Directory != nil {
					if err := enqueue(*entry.Directory); err != nil {
						return nil, err
					}
				}
				if entry.File != nil {
					if err := enqueue(*entry.File); err != nil {
						return nil, err
					}
				}
			}
		case rootfshead.FileMediaType:
			manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			for _, extent := range manifest.Extents {
				if err := enqueue(extent.Object); err != nil {
					return nil, err
				}
			}
		default:
			return nil, fmt.Errorf("unsupported rootfs metadata media type %s", object.MediaType)
		}
	}

	marker, _, err := rootfshead.MarkerObject(candidate.Head)
	if err != nil {
		return nil, err
	}
	objects[marker.Key] = marker
	if strings.TrimSpace(candidate.ImageEnvelope.Key) != "" {
		if err := candidate.ImageEnvelope.Validate(rootfshead.ImageEnvelopeMediaType); err != nil {
			return nil, fmt.Errorf("invalid rootfs head image envelope: %w", err)
		}
		objects[candidate.ImageEnvelope.Key] = candidate.ImageEnvelope
	}
	result := make([]rootfshead.Object, 0, len(objects))
	for _, object := range objects {
		result = append(result, object)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func readRootFSInventoryObject(ctx context.Context, reader RootFSObjectReader, object rootfshead.Object) ([]byte, error) {
	if object.Size <= 0 || object.Size > maxRootFSInventoryMetadataBytes {
		return nil, fmt.Errorf("rootfs metadata object %s has invalid size %d", object.Key, object.Size)
	}
	stream, err := reader.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, fmt.Errorf("read rootfs metadata object %s: %w", object.Key, err)
	}
	defer stream.Close()
	payload, err := io.ReadAll(io.LimitReader(stream, object.Size+1))
	if err != nil {
		return nil, fmt.Errorf("read rootfs metadata object %s: %w", object.Key, err)
	}
	parsed, err := digest.Parse(object.Digest)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload) != parsed {
		return nil, fmt.Errorf("rootfs metadata object %s failed size or digest validation", object.Key)
	}
	return payload, ctx.Err()
}

func rootFSInventoryPrefix(headObjectKey string) (string, error) {
	const separator = "/heads/"
	position := strings.LastIndex(strings.TrimSpace(headObjectKey), separator)
	if position <= 0 {
		return "", fmt.Errorf("rootfs head object key %q has no filesystem prefix", headObjectKey)
	}
	return headObjectKey[:position], nil
}

func normalizeRootFSInventory(objects []rootfshead.Object) ([]rootfshead.Object, error) {
	byKey := make(map[string]rootfshead.Object, len(objects))
	for _, object := range objects {
		if !supportedRootFSObjectMediaType(object.MediaType) {
			return nil, fmt.Errorf("invalid rootfs object %s: unsupported media type %q", object.Key, object.MediaType)
		}
		if err := object.Validate(object.MediaType); err != nil {
			return nil, fmt.Errorf("invalid rootfs object %s: %w", object.Key, err)
		}
		if existing, ok := byKey[object.Key]; ok && existing != object {
			return nil, fmt.Errorf("rootfs object %s has conflicting descriptors", object.Key)
		}
		byKey[object.Key] = object
	}
	result := make([]rootfshead.Object, 0, len(byKey))
	for _, object := range byKey {
		result = append(result, object)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func rootFSInventoryContains(objects []rootfshead.Object, key string) bool {
	for _, object := range objects {
		if object.Key == key {
			return true
		}
	}
	return false
}
