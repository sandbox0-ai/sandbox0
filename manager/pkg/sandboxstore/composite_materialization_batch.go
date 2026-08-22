package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const rootFSMaterializationBatchIDPrefix = "rootfs-materialization-batch-v1-"

// RootFSGenerationMaterializationIdentity fixes one generation locator before
// any shared batch object is uploaded.
type RootFSGenerationMaterializationIdentity struct {
	GenerationID           string
	ExpectedLocatorVersion int64
	ExpectedDescriptor     []byte
}

type BeginRootFSGenerationMaterializationBatchRequest struct {
	BatchID          string
	PackLane         string
	TeamID           string
	FormatGeneration int
	Members          []RootFSGenerationMaterializationIdentity
}

type RootFSGenerationMaterializationBatch struct {
	BatchID          string
	PackLane         string
	TeamID           string
	FormatGeneration int
	State            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Members          []RootFSGenerationMaterializationIdentity
}

type RootFSGenerationMaterializationPublication struct {
	GenerationID           string
	ExpectedLocatorVersion int64
	ExpectedDescriptor     []byte
	MaterializedDescriptor []byte
	References             []rootfsblock.ObjectReference
}

type PublishRootFSGenerationMaterializationBatchRequest struct {
	BatchID string
	Members []RootFSGenerationMaterializationPublication
}

type RootFSGenerationMaterializationGarbageResult struct {
	AbandonedBatches int
	PurgedBatches    int
	EnqueuedObjects  int
}

// RootFSMaterializationBatchID hashes the ordered exact membership. A batch
// can therefore be resumed after a crash without silently changing pack
// boundaries or adopting a generation selected by another manager replica.
func RootFSMaterializationBatchID(
	packLane string,
	members []RootFSGenerationMaterializationIdentity,
) (string, error) {
	packLane = strings.TrimSpace(packLane)
	if packLane == "" || len(packLane) > 256 {
		return "", fmt.Errorf("materialization pack lane is required and must not exceed 256 bytes")
	}
	if len(members) == 0 || len(members) > 10_000 {
		return "", fmt.Errorf("materialization batch must contain 1..10000 members")
	}
	hasher := sha256.New()
	writeMaterializationHashPart(hasher, []byte("sandbox0-rootfs-materialization-batch-v1"))
	writeMaterializationHashPart(hasher, []byte(packLane))
	seen := make(map[string]struct{}, len(members))
	for index, member := range members {
		generationID := strings.TrimSpace(member.GenerationID)
		if generationID == "" || len(generationID) > 256 || member.ExpectedLocatorVersion <= 0 ||
			len(member.ExpectedDescriptor) == 0 || len(member.ExpectedDescriptor) > rootfsblock.MaxDescriptorBytes {
			return "", fmt.Errorf("materialization member %d has invalid immutable identity", index)
		}
		if _, found := seen[generationID]; found {
			return "", fmt.Errorf("materialization member %d duplicates generation %s", index, generationID)
		}
		seen[generationID] = struct{}{}
		descriptor, err := rootfsblock.DecodeDescriptor(member.ExpectedDescriptor)
		if err != nil || descriptor.CompositeTail == nil {
			return "", fmt.Errorf("materialization member %d descriptor must be composite durable: %v", index, err)
		}
		writeMaterializationHashPart(hasher, []byte(generationID))
		var version [8]byte
		binary.BigEndian.PutUint64(version[:], uint64(member.ExpectedLocatorVersion))
		writeMaterializationHashPart(hasher, version[:])
		descriptorDigest := sha256.Sum256(member.ExpectedDescriptor)
		writeMaterializationHashPart(hasher, descriptorDigest[:])
	}
	return rootFSMaterializationBatchIDPrefix + hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeMaterializationHashPart(hasher interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
}

// GetOldestUploadingRootFSGenerationMaterializationBatch returns one exact
// crash-resume unit. Callers must process it before creating new membership.
func (s *PGSandboxStore) GetOldestUploadingRootFSGenerationMaterializationBatch(
	ctx context.Context,
) (*RootFSGenerationMaterializationBatch, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	rows, err := s.pool.Query(ctx, `
		WITH oldest AS (
			SELECT batch_id
			FROM manager.rootfs_materialization_batches
			WHERE state = 'uploading'
			ORDER BY created_at, batch_id
			LIMIT 1
		)
		SELECT batch.batch_id, batch.pack_lane, batch.team_id,
			batch.format_generation, batch.state, batch.created_at, batch.updated_at,
			member.generation_id, member.expected_locator_version,
			member.expected_descriptor
		FROM oldest
		JOIN manager.rootfs_materialization_batches batch USING (batch_id)
		JOIN manager.rootfs_materialization_members member USING (batch_id)
		ORDER BY member.ordinal
	`)
	if err != nil {
		return nil, fmt.Errorf("list uploading rootfs materialization batch: %w", err)
	}
	defer rows.Close()
	var batch *RootFSGenerationMaterializationBatch
	for rows.Next() {
		var current RootFSGenerationMaterializationBatch
		var member RootFSGenerationMaterializationIdentity
		if err := rows.Scan(
			&current.BatchID, &current.PackLane, &current.TeamID,
			&current.FormatGeneration, &current.State, &current.CreatedAt, &current.UpdatedAt,
			&member.GenerationID, &member.ExpectedLocatorVersion, &member.ExpectedDescriptor,
		); err != nil {
			return nil, fmt.Errorf("scan uploading rootfs materialization batch: %w", err)
		}
		if batch == nil {
			batch = &current
		}
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		batch.Members = append(batch.Members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate uploading rootfs materialization batch: %w", err)
	}
	return batch, nil
}

func (s *PGSandboxStore) BeginRootFSGenerationMaterializationBatch(
	ctx context.Context,
	req *BeginRootFSGenerationMaterializationBatchRequest,
) (*RootFSGenerationMaterializationBatch, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := normalizeBeginRootFSMaterializationBatch(req)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin rootfs materialization batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_materialization_batches (
			batch_id, pack_lane, team_id, format_generation, member_count, state
		) VALUES ($1, $2, $3, $4, $5, 'uploading')
		ON CONFLICT (batch_id) DO NOTHING
	`, normalized.BatchID, normalized.PackLane, normalized.TeamID,
		normalized.FormatGeneration, len(normalized.Members))
	if err != nil {
		return nil, fmt.Errorf("insert rootfs materialization batch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		stored, err := loadRootFSMaterializationBatch(ctx, tx, normalized.BatchID, true)
		if err != nil {
			return nil, err
		}
		if err := validateExactRootFSMaterializationBatch(stored, normalized); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit existing rootfs materialization batch: %w", err)
		}
		return stored, nil
	}

	lockOrder := append([]RootFSGenerationMaterializationIdentity(nil), normalized.Members...)
	sort.Slice(lockOrder, func(left, right int) bool {
		return lockOrder[left].GenerationID < lockOrder[right].GenerationID
	})
	for _, member := range lockOrder {
		var locatorVersion int64
		var descriptor []byte
		var durability, teamID string
		var formatGeneration int
		var liveWriter bool
		err := tx.QueryRow(ctx, `
			SELECT generation.locator_version, generation.descriptor,
				generation.durability_state, filesystem.team_id,
				generation.format_generation,
				EXISTS (
					SELECT 1 FROM manager.rootfs_writer_grants writer
					WHERE writer.initial_generation_id = generation.generation_id
						AND writer.state IN ('issued', 'consumed', 'retiring')
				)
			FROM manager.rootfs_generations generation
			JOIN manager.rootfs_filesystems filesystem
				ON filesystem.filesystem_id = generation.filesystem_id
			WHERE generation.generation_id = $1
			FOR UPDATE OF generation
		`, member.GenerationID).Scan(
			&locatorVersion, &descriptor, &durability, &teamID, &formatGeneration, &liveWriter,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: generation %s", ErrRootFSFilesystemNotFound, member.GenerationID)
		}
		if err != nil {
			return nil, fmt.Errorf("lock rootfs materialization generation %s: %w", member.GenerationID, err)
		}
		if locatorVersion != member.ExpectedLocatorVersion || !bytes.Equal(descriptor, member.ExpectedDescriptor) ||
			durability != RootFSGenerationStateCompositeDurable || teamID != normalized.TeamID ||
			formatGeneration != normalized.FormatGeneration || liveWriter {
			return nil, fmt.Errorf("%w: generation %s changed before materialization batch",
				ErrRootFSGenerationConflict, member.GenerationID)
		}
	}
	for ordinal, member := range normalized.Members {
		descriptorDigest := sha256.Sum256(member.ExpectedDescriptor)
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.rootfs_materialization_members (
				batch_id, ordinal, generation_id, expected_locator_version,
				expected_descriptor, expected_descriptor_digest, state
			) VALUES ($1, $2, $3, $4, $5, $6, 'uploading')
		`, normalized.BatchID, ordinal, member.GenerationID,
			member.ExpectedLocatorVersion, member.ExpectedDescriptor, descriptorDigest[:]); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return nil, fmt.Errorf("%w: generation %s already belongs to an uploading materialization batch",
					ErrRootFSGenerationConflict, member.GenerationID)
			}
			return nil, fmt.Errorf("insert rootfs materialization member %s: %w", member.GenerationID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit rootfs materialization batch: %w", err)
	}
	return &RootFSGenerationMaterializationBatch{
		BatchID: normalized.BatchID, PackLane: normalized.PackLane,
		TeamID: normalized.TeamID, FormatGeneration: normalized.FormatGeneration,
		State: "uploading", Members: cloneRootFSMaterializationIdentities(normalized.Members),
	}, nil
}

func (s *PGSandboxStore) RegisterRootFSGenerationMaterializationBatchObject(
	ctx context.Context,
	batchID string,
	reference rootfsblock.ObjectReference,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs generation store is not configured")
	}
	batchID = strings.TrimSpace(batchID)
	if err := validateRootFSMaterializationObjectReference(reference); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin rootfs materialization object registration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT state FROM manager.rootfs_materialization_batches
		WHERE batch_id = $1 FOR UPDATE
	`, batchID).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: materialization batch %s", ErrRootFSGenerationConflict, batchID)
	} else if err != nil {
		return fmt.Errorf("lock rootfs materialization batch: %w", err)
	}
	if state == "published" {
		var kind, checksum, uploadState string
		var size int64
		var uploadedAt *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT object_record.object_kind, object_record.object_size,
				object_record.checksum, object_record.uploaded_at, batch_object.upload_state
			FROM manager.rootfs_materialization_batch_objects batch_object
			JOIN manager.rootfs_materialization_objects object_record USING (object_key)
			WHERE batch_object.batch_id = $1 AND batch_object.object_key = $2
		`, batchID, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt, &uploadState); err != nil {
			return fmt.Errorf("read published rootfs materialization object: %w", err)
		}
		if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum ||
			uploadedAt == nil || uploadState != "uploaded" {
			return fmt.Errorf("%w: published materialization object %s has different immutable metadata",
				ErrRootFSGenerationConflict, reference.Key)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit existing rootfs materialization object registration: %w", err)
		}
		return nil
	}
	if state != "uploading" {
		return fmt.Errorf("%w: materialization batch %s is %s", ErrRootFSGenerationConflict, batchID, state)
	}
	var pendingDeletion bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key = $1)
	`, reference.Key).Scan(&pendingDeletion); err != nil {
		return fmt.Errorf("check rootfs materialization object deletion fence: %w", err)
	}
	if pendingDeletion {
		return fmt.Errorf("%w: rootfs materialization object %s is pending deletion",
			ErrRootFSGenerationConflict, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_materialization_objects (
			object_key, object_kind, object_size, checksum
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (object_key) DO NOTHING
	`, reference.Key, reference.Kind, reference.Size, reference.Checksum); err != nil {
		return fmt.Errorf("register rootfs materialization object: %w", err)
	}
	var kind, checksum string
	var size int64
	var uploadedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT object_kind, object_size, checksum, uploaded_at
		FROM manager.rootfs_materialization_objects
		WHERE object_key = $1 FOR UPDATE
	`, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt); err != nil {
		return fmt.Errorf("read rootfs materialization object: %w", err)
	}
	if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum {
		return fmt.Errorf("%w: rootfs materialization object %s has different immutable metadata",
			ErrRootFSGenerationConflict, reference.Key)
	}
	uploadState := "registered"
	if uploadedAt != nil {
		uploadState = "uploaded"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_materialization_batch_objects (
			batch_id, object_key, upload_state
		) VALUES ($1, $2, $3)
		ON CONFLICT (batch_id, object_key) DO UPDATE
		SET upload_state = CASE
			WHEN manager.rootfs_materialization_batch_objects.upload_state = 'uploaded'
				THEN 'uploaded'
			ELSE EXCLUDED.upload_state
		END,
		updated_at = NOW()
	`, batchID, reference.Key, uploadState); err != nil {
		return fmt.Errorf("link rootfs materialization batch object: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches SET updated_at = NOW()
		WHERE batch_id = $1 AND state = 'uploading'
	`, batchID); err != nil {
		return fmt.Errorf("touch rootfs materialization batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs materialization object registration: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) MarkRootFSGenerationMaterializationBatchObjectUploaded(
	ctx context.Context,
	batchID, objectKey string,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs generation store is not configured")
	}
	batchID, objectKey = strings.TrimSpace(batchID), strings.TrimSpace(objectKey)
	if batchID == "" || objectKey == "" {
		return fmt.Errorf("materialization batch and object key are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin rootfs materialization upload mark: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT state FROM manager.rootfs_materialization_batches
		WHERE batch_id = $1 FOR UPDATE
	`, batchID).Scan(&state); err != nil {
		return fmt.Errorf("lock rootfs materialization batch for upload mark: %w", err)
	}
	if state == "published" {
		var uploadState string
		var uploadedAt *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT batch_object.upload_state, object_record.uploaded_at
			FROM manager.rootfs_materialization_batch_objects batch_object
			JOIN manager.rootfs_materialization_objects object_record USING (object_key)
			WHERE batch_object.batch_id = $1 AND batch_object.object_key = $2
		`, batchID, objectKey).Scan(&uploadState, &uploadedAt); err != nil {
			return fmt.Errorf("read published rootfs materialization upload mark: %w", err)
		}
		if uploadState != "uploaded" || uploadedAt == nil {
			return fmt.Errorf("%w: published materialization object %s is not uploaded",
				ErrRootFSGenerationConflict, objectKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit existing rootfs materialization upload mark: %w", err)
		}
		return nil
	}
	if state != "uploading" {
		return fmt.Errorf("%w: materialization batch %s is %s", ErrRootFSGenerationConflict, batchID, state)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batch_objects
		SET upload_state = 'uploaded', updated_at = NOW()
		WHERE batch_id = $1 AND object_key = $2
	`, batchID, objectKey)
	if err != nil {
		return fmt.Errorf("mark rootfs materialization batch object uploaded: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: object %s is not registered by batch %s",
			ErrRootFSGenerationConflict, objectKey, batchID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_objects
		SET uploaded_at = COALESCE(uploaded_at, NOW()), updated_at = NOW()
		WHERE object_key = $1
	`, objectKey); err != nil {
		return fmt.Errorf("mark rootfs materialization object uploaded: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches SET updated_at = NOW()
		WHERE batch_id = $1
	`, batchID); err != nil {
		return fmt.Errorf("touch rootfs materialization batch upload: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs materialization upload mark: %w", err)
	}
	return nil
}

// PublishRootFSGenerationMaterializationBatch atomically installs every
// descriptor in a shared pack. Exact commit-response-loss retries succeed;
// changed membership, descriptors, or object reference sets fail closed.
func (s *PGSandboxStore) PublishRootFSGenerationMaterializationBatch(
	ctx context.Context,
	req *PublishRootFSGenerationMaterializationBatchRequest,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := normalizeRootFSMaterializationBatchPublication(req)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin rootfs materialization batch publication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	batch, err := loadRootFSMaterializationBatch(ctx, tx, normalized.BatchID, true)
	if err != nil {
		return err
	}
	if len(batch.Members) != len(normalized.Members) {
		return fmt.Errorf("%w: materialization batch %s member count changed",
			ErrRootFSGenerationConflict, normalized.BatchID)
	}
	publicationByID := make(map[string]RootFSGenerationMaterializationPublication, len(normalized.Members))
	for _, member := range normalized.Members {
		publicationByID[member.GenerationID] = member
	}
	if batch.State == "published" {
		if err := validatePublishedRootFSMaterializationBatchRetry(ctx, tx, batch, publicationByID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit existing rootfs materialization batch publication: %w", err)
		}
		return nil
	}
	if batch.State != "uploading" {
		return fmt.Errorf("%w: materialization batch %s is %s",
			ErrRootFSGenerationConflict, batch.BatchID, batch.State)
	}

	for ordinal, identity := range batch.Members {
		publication, found := publicationByID[identity.GenerationID]
		if !found || publication.ExpectedLocatorVersion != identity.ExpectedLocatorVersion ||
			!bytes.Equal(publication.ExpectedDescriptor, identity.ExpectedDescriptor) {
			return fmt.Errorf("%w: materialization batch member %s identity changed",
				ErrRootFSGenerationConflict, identity.GenerationID)
		}
		oldDescriptor, err := rootfsblock.DecodeDescriptor(identity.ExpectedDescriptor)
		if err != nil || oldDescriptor.CompositeTail == nil {
			return fmt.Errorf("expected descriptor for %s is not composite durable: %v", identity.GenerationID, err)
		}
		newDescriptor, err := rootfsblock.DecodeDescriptor(publication.MaterializedDescriptor)
		if err != nil || newDescriptor.CompositeTail != nil {
			return fmt.Errorf("materialized descriptor for %s is invalid: %v", identity.GenerationID, err)
		}
		if oldDescriptor.Version != newDescriptor.Version ||
			oldDescriptor.LogicalSizeBytes != newDescriptor.LogicalSizeBytes ||
			oldDescriptor.BlockSizeBytes != newDescriptor.BlockSizeBytes {
			return fmt.Errorf("materialized descriptor for %s changes logical geometry", identity.GenerationID)
		}
		if !containsRootFSMaterializationReference(publication.References, newDescriptor.MappingRoot.Object.Key) {
			return fmt.Errorf("materialized descriptor for %s omits its mapping root object", identity.GenerationID)
		}
		if err := verifyUploadedRootFSMaterializationReferences(
			ctx, tx, batch.BatchID, publication.References,
		); err != nil {
			return fmt.Errorf("verify generation %s materialization objects: %w", identity.GenerationID, err)
		}
		var currentVersion int64
		var currentDescriptor []byte
		var currentDurability, currentBlockHead string
		if err := tx.QueryRow(ctx, `
			SELECT locator_version, descriptor, durability_state, current_block_head
			FROM manager.rootfs_generations
			WHERE generation_id = $1
			FOR UPDATE
		`, identity.GenerationID).Scan(
			&currentVersion, &currentDescriptor, &currentDurability, &currentBlockHead,
		); err != nil {
			return fmt.Errorf("lock rootfs materialization generation %s: %w", identity.GenerationID, err)
		}
		if currentVersion != identity.ExpectedLocatorVersion ||
			!bytes.Equal(currentDescriptor, identity.ExpectedDescriptor) ||
			currentDurability != RootFSGenerationStateCompositeDurable ||
			currentBlockHead != oldDescriptor.MappingRoot.RootDigest {
			return fmt.Errorf("%w: generation %s locator changed during materialization",
				ErrRootFSGenerationConflict, identity.GenerationID)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_generations
			SET current_block_head = $1, durability_state = $2,
				locator_version = locator_version + 1, descriptor = $3
			WHERE generation_id = $4 AND locator_version = $5
				AND durability_state = $6 AND descriptor = $7
		`, newDescriptor.MappingRoot.RootDigest, RootFSGenerationStateS3Materialized,
			publication.MaterializedDescriptor, identity.GenerationID,
			identity.ExpectedLocatorVersion, RootFSGenerationStateCompositeDurable,
			identity.ExpectedDescriptor)
		if err != nil {
			return fmt.Errorf("publish rootfs materialization generation %s: %w", identity.GenerationID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: generation %s locator update lost", ErrRootFSGenerationConflict, identity.GenerationID)
		}
		for _, reference := range publication.References {
			if _, err := tx.Exec(ctx, `
				INSERT INTO manager.rootfs_generation_materialization_objects (
					generation_id, locator_version, object_key
				) VALUES ($1, $2, $3)
			`, identity.GenerationID, identity.ExpectedLocatorVersion+1, reference.Key); err != nil {
				return fmt.Errorf("link generation %s materialization object %s: %w",
					identity.GenerationID, reference.Key, err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO manager.rootfs_materialization_member_objects (
					batch_id, ordinal, object_key
				) VALUES ($1, $2, $3)
			`, batch.BatchID, ordinal, reference.Key); err != nil {
				return fmt.Errorf("record batch member %s materialization object %s: %w",
					identity.GenerationID, reference.Key, err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_materialization_members
			SET state = 'published', materialized_descriptor = $3,
				published_locator_version = expected_locator_version + 1,
				updated_at = NOW()
			WHERE batch_id = $1 AND ordinal = $2 AND state = 'uploading'
		`, batch.BatchID, ordinal, publication.MaterializedDescriptor); err != nil {
			return fmt.Errorf("publish rootfs materialization member %s: %w", identity.GenerationID, err)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches
		SET state = 'published', published_at = NOW(), updated_at = NOW()
		WHERE batch_id = $1 AND state = 'uploading'
	`, batch.BatchID)
	if err != nil {
		return fmt.Errorf("publish rootfs materialization batch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: materialization batch %s state changed",
			ErrRootFSGenerationConflict, batch.BatchID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs materialization batch publication: %w", err)
	}
	return nil
}

// ReconcileRootFSGenerationMaterializationGarbage abandons only stale batches
// whose immutable generation identity has already moved away. Uploaded
// objects remain rooted by an uploading batch, a retained terminal journal,
// or a published generation locator; only objects with no such root enter the
// existing durable deletion queue.
func (s *PGSandboxStore) ReconcileRootFSGenerationMaterializationGarbage(
	ctx context.Context,
	uploadingStale, terminalRetention time.Duration,
	limit int,
) (*RootFSGenerationMaterializationGarbageResult, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	if uploadingStale < time.Minute || uploadingStale > 7*24*time.Hour {
		return nil, fmt.Errorf("materialization uploading stale interval must be between 1m and 7d")
	}
	if terminalRetention < time.Minute || terminalRetention > 30*24*time.Hour {
		return nil, fmt.Errorf("materialization terminal retention must be between 1m and 30d")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin rootfs materialization garbage reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result := &RootFSGenerationMaterializationGarbageResult{}
	staleMilliseconds := uploadingStale.Milliseconds()
	rows, err := tx.Query(ctx, `
		SELECT batch.batch_id
		FROM manager.rootfs_materialization_batches batch
		WHERE batch.state = 'uploading'
			AND batch.updated_at <= NOW() - ($1::bigint * INTERVAL '1 millisecond')
			AND EXISTS (
				SELECT 1
				FROM manager.rootfs_materialization_members member
				LEFT JOIN manager.rootfs_generations generation USING (generation_id)
				WHERE member.batch_id = batch.batch_id
					AND (
						generation.generation_id IS NULL
						OR generation.locator_version <> member.expected_locator_version
						OR generation.durability_state <> $2
						OR generation.descriptor <> member.expected_descriptor
					)
			)
		ORDER BY batch.updated_at, batch.batch_id
		LIMIT $3
		FOR UPDATE OF batch SKIP LOCKED
	`, staleMilliseconds, RootFSGenerationStateCompositeDurable, limit)
	if err != nil {
		return nil, fmt.Errorf("list stale rootfs materialization batches: %w", err)
	}
	var abandonedIDs []string
	for rows.Next() {
		var batchID string
		if err := rows.Scan(&batchID); err != nil {
			rows.Close()
			return nil, err
		}
		abandonedIDs = append(abandonedIDs, batchID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate stale rootfs materialization batches: %w", err)
	}
	rows.Close()
	for _, batchID := range abandonedIDs {
		tag, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_materialization_batches
			SET state = 'abandoned', abandon_reason = 'generation locator changed',
				abandoned_at = NOW(), updated_at = NOW()
			WHERE batch_id = $1 AND state = 'uploading'
		`, batchID)
		if err != nil {
			return nil, fmt.Errorf("abandon rootfs materialization batch %s: %w", batchID, err)
		}
		if tag.RowsAffected() != 1 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_materialization_members
			SET state = 'abandoned', updated_at = NOW()
			WHERE batch_id = $1 AND state = 'uploading'
		`, batchID); err != nil {
			return nil, fmt.Errorf("abandon rootfs materialization members for %s: %w", batchID, err)
		}
		result.AbandonedBatches++
	}

	remaining := limit - result.AbandonedBatches
	if remaining < 0 {
		remaining = 0
	}
	retentionMilliseconds := terminalRetention.Milliseconds()
	rows, err = tx.Query(ctx, `
		SELECT batch_id, team_id
		FROM manager.rootfs_materialization_batches
		WHERE state = 'abandoned'
			OR (state = 'published'
				AND updated_at <= NOW() - ($1::bigint * INTERVAL '1 millisecond'))
		ORDER BY CASE WHEN state = 'abandoned' THEN 0 ELSE 1 END,
			updated_at, batch_id
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, retentionMilliseconds, remaining)
	if err != nil {
		return nil, fmt.Errorf("list terminal rootfs materialization batches: %w", err)
	}
	type terminalBatch struct{ id, teamID string }
	var terminal []terminalBatch
	for rows.Next() {
		var item terminalBatch
		if err := rows.Scan(&item.id, &item.teamID); err != nil {
			rows.Close()
			return nil, err
		}
		terminal = append(terminal, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate terminal rootfs materialization batches: %w", err)
	}
	rows.Close()
	for _, batch := range terminal {
		objectRows, err := tx.Query(ctx, `
			SELECT object_key FROM manager.rootfs_materialization_batch_objects
			WHERE batch_id = $1 ORDER BY object_key
		`, batch.id)
		if err != nil {
			return nil, fmt.Errorf("list terminal materialization batch objects: %w", err)
		}
		var objectKeys []string
		for objectRows.Next() {
			var key string
			if err := objectRows.Scan(&key); err != nil {
				objectRows.Close()
				return nil, err
			}
			objectKeys = append(objectKeys, key)
		}
		if err := objectRows.Err(); err != nil {
			objectRows.Close()
			return nil, err
		}
		objectRows.Close()
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_materialization_batches
			WHERE batch_id = $1 AND state IN ('published', 'abandoned')
		`, batch.id); err != nil {
			return nil, fmt.Errorf("purge terminal materialization batch %s: %w", batch.id, err)
		}
		result.PurgedBatches++
		for _, objectKey := range objectKeys {
			enqueued, err := releaseUnreferencedRootFSMaterializationObject(
				ctx, tx, objectKey, batch.teamID,
			)
			if err != nil {
				return nil, err
			}
			if enqueued {
				result.EnqueuedObjects++
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit rootfs materialization garbage reconciliation: %w", err)
	}
	return result, nil
}

func releaseUnreferencedRootFSMaterializationObject(
	ctx context.Context,
	tx pgx.Tx,
	objectKey, teamID string,
) (bool, error) {
	var uploadedAt *time.Time
	err := tx.QueryRow(ctx, `
		DELETE FROM manager.rootfs_materialization_objects object_record
		WHERE object_record.object_key = $1
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_materialization_batch_objects batch_object
				WHERE batch_object.object_key = object_record.object_key
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_generation_materialization_objects locator_object
				WHERE locator_object.object_key = object_record.object_key
			)
		RETURNING uploaded_at
	`, objectKey).Scan(&uploadedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("release unreferenced materialization object %s: %w", objectKey, err)
	}
	if uploadedAt == nil {
		return false, nil
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (
			object_key, team_id, next_attempt_at, created_at, updated_at
		) VALUES ($1, $2, NOW(), NOW(), NOW())
		ON CONFLICT (object_key) DO NOTHING
	`, objectKey, strings.TrimSpace(teamID))
	if err != nil {
		return false, fmt.Errorf("enqueue unreferenced materialization object %s: %w", objectKey, err)
	}
	return tag.RowsAffected() == 1, nil
}

func normalizeBeginRootFSMaterializationBatch(
	req *BeginRootFSGenerationMaterializationBatchRequest,
) (*BeginRootFSGenerationMaterializationBatchRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs materialization batch request is required")
	}
	normalized := *req
	normalized.BatchID = strings.TrimSpace(req.BatchID)
	normalized.PackLane = strings.TrimSpace(req.PackLane)
	normalized.TeamID = strings.TrimSpace(req.TeamID)
	normalized.Members = cloneRootFSMaterializationIdentities(req.Members)
	if normalized.TeamID == "" || len(normalized.TeamID) > 256 || normalized.FormatGeneration <= 0 {
		return nil, fmt.Errorf("materialization team and positive format generation are required")
	}
	expectedLane := RootFSMaterializationPackLane(normalized.TeamID, normalized.FormatGeneration)
	if normalized.PackLane != expectedLane {
		return nil, fmt.Errorf("materialization pack lane does not match tenant and format")
	}
	expectedID, err := RootFSMaterializationBatchID(normalized.PackLane, normalized.Members)
	if err != nil {
		return nil, err
	}
	if normalized.BatchID != expectedID {
		return nil, fmt.Errorf("materialization batch ID does not match exact membership")
	}
	return &normalized, nil
}

func normalizeRootFSMaterializationBatchPublication(
	req *PublishRootFSGenerationMaterializationBatchRequest,
) (*PublishRootFSGenerationMaterializationBatchRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs materialization batch publication is required")
	}
	normalized := *req
	normalized.BatchID = strings.TrimSpace(req.BatchID)
	if !validRootFSMaterializationBatchID(normalized.BatchID) || len(req.Members) == 0 || len(req.Members) > 10_000 {
		return nil, fmt.Errorf("materialization batch ID and 1..10000 publications are required")
	}
	normalized.Members = make([]RootFSGenerationMaterializationPublication, len(req.Members))
	seen := make(map[string]struct{}, len(req.Members))
	for index, member := range req.Members {
		member.GenerationID = strings.TrimSpace(member.GenerationID)
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		member.MaterializedDescriptor = append([]byte(nil), member.MaterializedDescriptor...)
		if member.GenerationID == "" || member.ExpectedLocatorVersion <= 0 ||
			len(member.ExpectedDescriptor) == 0 || len(member.MaterializedDescriptor) == 0 {
			return nil, fmt.Errorf("materialization publication %d has incomplete identity", index)
		}
		if _, found := seen[member.GenerationID]; found {
			return nil, fmt.Errorf("materialization publication %d duplicates generation %s", index, member.GenerationID)
		}
		seen[member.GenerationID] = struct{}{}
		var err error
		member.References, err = normalizeRootFSMaterializationObjectReferences(member.References)
		if err != nil {
			return nil, fmt.Errorf("materialization publication %d: %w", index, err)
		}
		normalized.Members[index] = member
	}
	return &normalized, nil
}

func normalizeRootFSMaterializationObjectReferences(
	references []rootfsblock.ObjectReference,
) ([]rootfsblock.ObjectReference, error) {
	if len(references) == 0 || len(references) > rootfsblock.MaxMappingEntriesPerGeneration+1 {
		return nil, fmt.Errorf("materialization object references are required and bounded")
	}
	normalized := append([]rootfsblock.ObjectReference(nil), references...)
	for index := range normalized {
		normalized[index].Key = strings.TrimSpace(normalized[index].Key)
		normalized[index].Kind = strings.TrimSpace(normalized[index].Kind)
		normalized[index].Checksum = strings.TrimSpace(normalized[index].Checksum)
		if err := validateRootFSMaterializationObjectReference(normalized[index]); err != nil {
			return nil, fmt.Errorf("object reference %d: %w", index, err)
		}
	}
	sort.Slice(normalized, func(left, right int) bool { return normalized[left].Key < normalized[right].Key })
	for index := 1; index < len(normalized); index++ {
		if normalized[index-1].Key == normalized[index].Key {
			return nil, fmt.Errorf("object reference %d duplicates key %s", index, normalized[index].Key)
		}
	}
	return normalized, nil
}

func validateRootFSMaterializationObjectReference(reference rootfsblock.ObjectReference) error {
	return rootfsblock.ValidateObjectReference(reference)
}

func loadRootFSMaterializationBatch(
	ctx context.Context,
	tx pgx.Tx,
	batchID string,
	lock bool,
) (*RootFSGenerationMaterializationBatch, error) {
	if !validRootFSMaterializationBatchID(batchID) {
		return nil, fmt.Errorf("invalid rootfs materialization batch ID")
	}
	lockSQL := ""
	if lock {
		lockSQL = " FOR UPDATE OF batch, member"
	}
	rows, err := tx.Query(ctx, `
		SELECT batch.batch_id, batch.pack_lane, batch.team_id,
			batch.format_generation, batch.state, batch.created_at, batch.updated_at,
			member.generation_id, member.expected_locator_version,
			member.expected_descriptor
		FROM manager.rootfs_materialization_batches batch
		JOIN manager.rootfs_materialization_members member USING (batch_id)
		WHERE batch.batch_id = $1
		ORDER BY member.ordinal`+lockSQL, batchID)
	if err != nil {
		return nil, fmt.Errorf("load rootfs materialization batch: %w", err)
	}
	defer rows.Close()
	var batch *RootFSGenerationMaterializationBatch
	for rows.Next() {
		var current RootFSGenerationMaterializationBatch
		var member RootFSGenerationMaterializationIdentity
		if err := rows.Scan(
			&current.BatchID, &current.PackLane, &current.TeamID,
			&current.FormatGeneration, &current.State, &current.CreatedAt, &current.UpdatedAt,
			&member.GenerationID, &member.ExpectedLocatorVersion, &member.ExpectedDescriptor,
		); err != nil {
			return nil, fmt.Errorf("scan rootfs materialization batch: %w", err)
		}
		if batch == nil {
			batch = &current
		}
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		batch.Members = append(batch.Members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs materialization batch: %w", err)
	}
	if batch == nil {
		return nil, fmt.Errorf("%w: materialization batch %s", ErrRootFSGenerationConflict, batchID)
	}
	return batch, nil
}

func validateExactRootFSMaterializationBatch(
	stored *RootFSGenerationMaterializationBatch,
	req *BeginRootFSGenerationMaterializationBatchRequest,
) error {
	if stored == nil || stored.BatchID != req.BatchID || stored.PackLane != req.PackLane ||
		stored.TeamID != req.TeamID || stored.FormatGeneration != req.FormatGeneration ||
		len(stored.Members) != len(req.Members) {
		return fmt.Errorf("%w: materialization batch %s has different immutable fields",
			ErrRootFSGenerationConflict, req.BatchID)
	}
	for index := range stored.Members {
		actual, expected := stored.Members[index], req.Members[index]
		if actual.GenerationID != expected.GenerationID ||
			actual.ExpectedLocatorVersion != expected.ExpectedLocatorVersion ||
			!bytes.Equal(actual.ExpectedDescriptor, expected.ExpectedDescriptor) {
			return fmt.Errorf("%w: materialization batch %s member %d changed",
				ErrRootFSGenerationConflict, req.BatchID, index)
		}
	}
	return nil
}

func verifyUploadedRootFSMaterializationReferences(
	ctx context.Context,
	tx pgx.Tx,
	batchID string,
	references []rootfsblock.ObjectReference,
) error {
	for _, reference := range references {
		var kind, checksum, uploadState string
		var size int64
		var uploadedAt *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT object_record.object_kind, object_record.object_size,
				object_record.checksum, object_record.uploaded_at, batch_object.upload_state
			FROM manager.rootfs_materialization_batch_objects batch_object
			JOIN manager.rootfs_materialization_objects object_record USING (object_key)
			WHERE batch_object.batch_id = $1 AND batch_object.object_key = $2
		`, batchID, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt, &uploadState); err != nil {
			return fmt.Errorf("read registered object %s: %w", reference.Key, err)
		}
		if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum ||
			uploadedAt == nil || uploadState != "uploaded" {
			return fmt.Errorf("%w: materialization object %s is not durably uploaded with exact metadata",
				ErrRootFSGenerationConflict, reference.Key)
		}
	}
	return nil
}

func validatePublishedRootFSMaterializationBatchRetry(
	ctx context.Context,
	tx pgx.Tx,
	batch *RootFSGenerationMaterializationBatch,
	publicationByID map[string]RootFSGenerationMaterializationPublication,
) error {
	for ordinal, identity := range batch.Members {
		publication, found := publicationByID[identity.GenerationID]
		if !found || publication.ExpectedLocatorVersion != identity.ExpectedLocatorVersion ||
			!bytes.Equal(publication.ExpectedDescriptor, identity.ExpectedDescriptor) {
			return fmt.Errorf("%w: published materialization member %s identity changed",
				ErrRootFSGenerationConflict, identity.GenerationID)
		}
		if err := verifyUploadedRootFSMaterializationReferences(
			ctx, tx, batch.BatchID, publication.References,
		); err != nil {
			return fmt.Errorf("verify published materialization member %s objects: %w",
				identity.GenerationID, err)
		}
		var storedDescriptor, currentDescriptor []byte
		var memberState, durability string
		var publishedVersion, currentVersion *int64
		if err := tx.QueryRow(ctx, `
			SELECT member.state, member.materialized_descriptor,
				member.published_locator_version, generation.locator_version,
				generation.durability_state, generation.descriptor
			FROM manager.rootfs_materialization_members member
			JOIN manager.rootfs_generations generation USING (generation_id)
			WHERE member.batch_id = $1 AND member.ordinal = $2
		`, batch.BatchID, ordinal).Scan(
			&memberState, &storedDescriptor, &publishedVersion, &currentVersion,
			&durability, &currentDescriptor,
		); err != nil {
			return fmt.Errorf("read published materialization member %s: %w", identity.GenerationID, err)
		}
		if memberState != "published" || publishedVersion == nil || currentVersion == nil ||
			*publishedVersion != identity.ExpectedLocatorVersion+1 || *currentVersion != *publishedVersion ||
			durability != RootFSGenerationStateS3Materialized ||
			!bytes.Equal(storedDescriptor, publication.MaterializedDescriptor) ||
			!bytes.Equal(currentDescriptor, publication.MaterializedDescriptor) {
			return fmt.Errorf("%w: published materialization member %s changed",
				ErrRootFSGenerationConflict, identity.GenerationID)
		}
		rows, err := tx.Query(ctx, `
			SELECT object_key FROM manager.rootfs_materialization_member_objects
			WHERE batch_id = $1 AND ordinal = $2 ORDER BY object_key
		`, batch.BatchID, ordinal)
		if err != nil {
			return fmt.Errorf("list published materialization member objects: %w", err)
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(keys) != len(publication.References) {
			return fmt.Errorf("%w: published materialization member %s object set changed",
				ErrRootFSGenerationConflict, identity.GenerationID)
		}
		for index, key := range keys {
			if key != publication.References[index].Key {
				return fmt.Errorf("%w: published materialization member %s object set changed",
					ErrRootFSGenerationConflict, identity.GenerationID)
			}
		}
	}
	return nil
}

func containsRootFSMaterializationReference(references []rootfsblock.ObjectReference, key string) bool {
	index := sort.Search(len(references), func(index int) bool { return references[index].Key >= key })
	return index < len(references) && references[index].Key == key
}

func cloneRootFSMaterializationIdentities(
	members []RootFSGenerationMaterializationIdentity,
) []RootFSGenerationMaterializationIdentity {
	result := make([]RootFSGenerationMaterializationIdentity, len(members))
	for index, member := range members {
		member.GenerationID = strings.TrimSpace(member.GenerationID)
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		result[index] = member
	}
	return result
}

func validRootFSMaterializationBatchID(value string) bool {
	if !strings.HasPrefix(value, rootFSMaterializationBatchIDPrefix) ||
		len(value) != len(rootFSMaterializationBatchIDPrefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, rootFSMaterializationBatchIDPrefix))
	return err == nil
}
