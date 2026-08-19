package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const DefaultRootFSCompositeBacklogBytes = int64(1 << 30)

var ErrRootFSCompositeBacklogExhausted = errors.New("rootfs composite backlog capacity exhausted")

// RootFSCompositeBacklogUsage is the region-wide PostgreSQL descriptor budget.
// Descriptor bytes conservatively include both the composite tail and its
// bounded mapping envelope.
type RootFSCompositeBacklogUsage struct {
	UsedDescriptorBytes int64
	MaxDescriptorBytes  int64
	GenerationCount     int64
}

// RootFSGenerationMaterialization is one physical-locator CAS. It preserves
// the immutable logical generation ID while replacing its PostgreSQL tail with
// a complete S3 mapping root.
type RootFSGenerationMaterialization struct {
	GenerationID           string
	ExpectedLocatorVersion int64
	ExpectedDescriptor     []byte
	MaterializedDescriptor []byte
}

// SetRootFSCompositeBacklogLimit updates the singleton regional policy used by
// every manager replica. Usage is derived from rootfs_generations and is never
// maintained as a second counter.
func (s *PGSandboxStore) SetRootFSCompositeBacklogLimit(ctx context.Context, maximum int64) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs generation store is not configured")
	}
	if maximum <= 0 {
		return fmt.Errorf("rootfs composite backlog maximum must be positive")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_composite_backlog_policy
		SET max_descriptor_bytes = $1, updated_at = NOW()
		WHERE singleton = TRUE
	`, maximum)
	if err != nil {
		return fmt.Errorf("set rootfs composite backlog limit: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs composite backlog policy row is missing")
	}
	return nil
}

func (s *PGSandboxStore) GetRootFSCompositeBacklogUsage(ctx context.Context) (RootFSCompositeBacklogUsage, error) {
	if s == nil || s.pool == nil {
		return RootFSCompositeBacklogUsage{}, fmt.Errorf("rootfs generation store is not configured")
	}
	var usage RootFSCompositeBacklogUsage
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(octet_length(g.descriptor)), 0),
			COUNT(g.generation_id), policy.max_descriptor_bytes
		FROM manager.rootfs_composite_backlog_policy AS policy
		LEFT JOIN manager.rootfs_generations AS g
			ON g.durability_state = $1
		WHERE policy.singleton = TRUE
		GROUP BY policy.max_descriptor_bytes
	`, RootFSGenerationStateCompositeDurable).Scan(
		&usage.UsedDescriptorBytes, &usage.GenerationCount, &usage.MaxDescriptorBytes,
	)
	if err != nil {
		return RootFSCompositeBacklogUsage{}, fmt.Errorf("read rootfs composite backlog usage: %w", err)
	}
	return usage, nil
}

// ListCompositeRootFSGenerations returns oldest-first materializer work. The
// locator CAS, rather than this unlocked scan, owns concurrency correctness.
func (s *PGSandboxStore) ListCompositeRootFSGenerations(ctx context.Context, limit int) ([]RootFSGeneration, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	if limit <= 0 || limit > 10_000 {
		return nil, fmt.Errorf("rootfs composite scan limit must be between 1 and 10000")
	}
	rows, err := s.pool.Query(ctx, rootFSGenerationSelectSQL()+`
		WHERE durability_state = $1
		ORDER BY created_at, generation_id
		LIMIT $2
	`, RootFSGenerationStateCompositeDurable, limit)
	if err != nil {
		return nil, fmt.Errorf("list composite rootfs generations: %w", err)
	}
	defer rows.Close()
	result := make([]RootFSGeneration, 0, limit)
	for rows.Next() {
		generation, err := scanRootFSGeneration(rows)
		if err != nil {
			return nil, fmt.Errorf("scan composite rootfs generation: %w", err)
		}
		result = append(result, *generation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate composite rootfs generations: %w", err)
	}
	return result, nil
}

// PublishRootFSGenerationMaterialization atomically replaces one composite
// physical locator. An exact completed retry succeeds; a changed locator fails
// closed so concurrent workers cannot overwrite newer placement.
func (s *PGSandboxStore) PublishRootFSGenerationMaterialization(
	ctx context.Context,
	req *RootFSGenerationMaterialization,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, oldDescriptor, nextDescriptor, err := validateRootFSGenerationMaterialization(req)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_generations
		SET current_block_head = $1,
			durability_state = $2,
			locator_version = locator_version + 1,
			descriptor = $3
		WHERE generation_id = $4
			AND durability_state = $5
			AND locator_version = $6
			AND descriptor = $7
			AND current_block_head = $8
	`, nextDescriptor.MappingRoot.RootDigest, RootFSGenerationStateS3Materialized,
		normalized.MaterializedDescriptor, normalized.GenerationID,
		RootFSGenerationStateCompositeDurable, normalized.ExpectedLocatorVersion,
		normalized.ExpectedDescriptor, oldDescriptor.MappingRoot.RootDigest)
	if err != nil {
		return fmt.Errorf("publish rootfs generation materialization: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	stored, err := s.GetRootFSGeneration(ctx, normalized.GenerationID)
	if err == nil && stored.DurabilityState == RootFSGenerationStateS3Materialized &&
		stored.LocatorVersion == normalized.ExpectedLocatorVersion+1 &&
		bytes.Equal(stored.Descriptor, normalized.MaterializedDescriptor) {
		return nil
	}
	if err != nil && !errors.Is(err, ErrRootFSFilesystemNotFound) {
		return fmt.Errorf("read conflicting rootfs materialization: %w", err)
	}
	return fmt.Errorf("%w: generation %s locator changed", ErrRootFSGenerationConflict, normalized.GenerationID)
}

func validateRootFSGenerationMaterialization(
	req *RootFSGenerationMaterialization,
) (*RootFSGenerationMaterialization, rootfsblock.Descriptor, rootfsblock.Descriptor, error) {
	if req == nil || req.GenerationID == "" || req.ExpectedLocatorVersion <= 0 ||
		len(req.ExpectedDescriptor) == 0 || len(req.MaterializedDescriptor) == 0 {
		return nil, rootfsblock.Descriptor{}, rootfsblock.Descriptor{},
			fmt.Errorf("generation, locator version, and both descriptors are required")
	}
	normalized := *req
	normalized.GenerationID = strings.TrimSpace(req.GenerationID)
	if normalized.GenerationID == "" {
		return nil, rootfsblock.Descriptor{}, rootfsblock.Descriptor{}, fmt.Errorf("generation is required")
	}
	normalized.ExpectedDescriptor = append([]byte(nil), req.ExpectedDescriptor...)
	normalized.MaterializedDescriptor = append([]byte(nil), req.MaterializedDescriptor...)
	oldDescriptor, err := rootfsblock.DecodeDescriptor(normalized.ExpectedDescriptor)
	if err != nil || oldDescriptor.CompositeTail == nil {
		return nil, rootfsblock.Descriptor{}, rootfsblock.Descriptor{},
			fmt.Errorf("expected descriptor must be composite durable: %v", err)
	}
	nextDescriptor, err := rootfsblock.DecodeDescriptor(normalized.MaterializedDescriptor)
	if err != nil || nextDescriptor.CompositeTail != nil {
		return nil, rootfsblock.Descriptor{}, rootfsblock.Descriptor{},
			fmt.Errorf("materialized descriptor must contain a complete S3 mapping: %v", err)
	}
	if oldDescriptor.Version != nextDescriptor.Version ||
		oldDescriptor.LogicalSizeBytes != nextDescriptor.LogicalSizeBytes ||
		oldDescriptor.BlockSizeBytes != nextDescriptor.BlockSizeBytes {
		return nil, rootfsblock.Descriptor{}, rootfsblock.Descriptor{},
			fmt.Errorf("materialized descriptor changes logical geometry")
	}
	return &normalized, oldDescriptor, nextDescriptor, nil
}

func ensureRootFSCompositeBacklogCapacity(
	ctx context.Context,
	db rootFSWriterGrantDB,
	generation *RootFSGeneration,
) error {
	if generation == nil || generation.DurabilityState != RootFSGenerationStateCompositeDurable {
		return nil
	}
	var maximum int64
	if err := db.QueryRow(ctx, `
		SELECT max_descriptor_bytes
		FROM manager.rootfs_composite_backlog_policy
		WHERE singleton = TRUE
		FOR UPDATE
	`).Scan(&maximum); err != nil {
		return fmt.Errorf("lock rootfs composite backlog policy: %w", err)
	}
	var used int64
	if err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(octet_length(descriptor)), 0)
		FROM manager.rootfs_generations
		WHERE durability_state = $1
			AND generation_id <> $2
	`, RootFSGenerationStateCompositeDurable, generation.ID).Scan(&used); err != nil {
		return fmt.Errorf("measure rootfs composite backlog: %w", err)
	}
	requested := int64(len(generation.Descriptor))
	if requested > maximum || used > maximum-requested {
		return fmt.Errorf(
			"%w: used %d descriptor bytes, request %d bytes, limit %d bytes",
			ErrRootFSCompositeBacklogExhausted, used, requested, maximum,
		)
	}
	return nil
}
