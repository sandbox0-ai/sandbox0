package sandboxstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
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

// ListCompositeRootFSGenerations returns unbound oldest-first materializer
// work. Durable batch creation owns concurrency between manager replicas.
func (s *PGSandboxStore) ListCompositeRootFSGenerations(ctx context.Context, limit int) ([]RootFSGeneration, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	if limit <= 0 || limit > 10_000 {
		return nil, fmt.Errorf("rootfs composite scan limit must be between 1 and 10000")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT generation.generation_id, generation.filesystem_id,
			generation.parent_generation_id, generation.source_oci_digest,
			generation.base_artifact_digest, generation.base_block_root,
			generation.current_block_head, generation.writer_epoch,
			generation.format_generation, generation.durability_state,
			generation.locator_version, generation.descriptor, generation.created_at,
			filesystem.team_id
		FROM manager.rootfs_generations generation
		JOIN manager.rootfs_filesystems filesystem
			ON filesystem.filesystem_id = generation.filesystem_id
		WHERE generation.durability_state = $1
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_materialization_members member
				WHERE member.generation_id = generation.generation_id
					AND member.state = 'uploading'
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_writer_grants writer
				WHERE writer.initial_generation_id = generation.generation_id
					AND writer.state IN ('issued', 'consumed', 'retiring')
			)
		ORDER BY generation.created_at, generation.generation_id
		LIMIT $2
	`, RootFSGenerationStateCompositeDurable, limit)
	if err != nil {
		return nil, fmt.Errorf("list composite rootfs generations: %w", err)
	}
	defer rows.Close()
	result := make([]RootFSGeneration, 0, limit)
	for rows.Next() {
		var generation RootFSGeneration
		var parent *string
		if err := rows.Scan(
			&generation.ID, &generation.FilesystemID, &parent,
			&generation.SourceOCIDigest, &generation.BaseArtifactDigest,
			&generation.BaseBlockRoot, &generation.CurrentBlockHead,
			&generation.WriterEpoch, &generation.FormatGeneration,
			&generation.DurabilityState, &generation.LocatorVersion,
			&generation.Descriptor, &generation.CreatedAt,
			&generation.MaterializationTeamID,
		); err != nil {
			return nil, fmt.Errorf("scan composite rootfs generation: %w", err)
		}
		if parent != nil {
			generation.ParentGenerationID = *parent
		}
		generation.Descriptor = append([]byte(nil), generation.Descriptor...)
		generation.MaterializationPackLane = RootFSMaterializationPackLane(
			generation.MaterializationTeamID, generation.FormatGeneration,
		)
		result = append(result, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate composite rootfs generations: %w", err)
	}
	return result, nil
}

// RootFSMaterializationPackLane returns a path-safe tenant and format
// isolation identity. Raw tenant IDs never become object keys.
func RootFSMaterializationPackLane(teamID string, formatGeneration int) string {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" || formatGeneration <= 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(teamID))
	return fmt.Sprintf("tenant-sha256:%x/format:%d", sum, formatGeneration)
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
