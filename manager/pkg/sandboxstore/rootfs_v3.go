package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

// SandboxRootFSHead is the manager-owned publication record for one complete
// immutable rootfs v3 Head.
type SandboxRootFSHead struct {
	SandboxID         string
	SourceSandboxID   string
	TeamID            string
	RuntimeGeneration int64
	Parent            *rootfshead.HeadReference
	Reference         rootfshead.HeadReference
	Base              rootfshead.BaseIdentity
	Image             rootfshead.ImageReference
	InventoryComplete bool
	CreatedAt         time.Time
}

// RootFSExport is a durable OCI layer derived asynchronously from one v3 Head
// for template image publication. It is not used by pause or resume.
type RootFSExport struct {
	HeadID    string
	TeamID    string
	Object    rootfshead.Object
	DiffID    string
	CreatedAt time.Time
}

func (s *PGSandboxStore) SaveRootFSHead(ctx context.Context, head *SandboxRootFSHead) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return s.saveRootFSHeadTransaction(ctx, head, true)
}

func (t sandboxStoreTx) SaveRootFSHead(ctx context.Context, head *SandboxRootFSHead) error {
	return saveRootFSHead(ctx, t.tx, head, true)
}

func (s *PGSandboxStore) StageRootFSHead(ctx context.Context, head *SandboxRootFSHead) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return s.saveRootFSHeadTransaction(ctx, head, false)
}

func (s *PGSandboxStore) saveRootFSHeadTransaction(ctx context.Context, head *SandboxRootFSHead, publish bool) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs v3 Head transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := saveRootFSHead(ctx, tx, head, publish); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs v3 Head transaction: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GetRootFSHead(ctx context.Context, sandboxID string) (*SandboxRootFSHead, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	return getRootFSHead(ctx, s.pool, sandboxID)
}

func (t sandboxStoreTx) GetRootFSHead(ctx context.Context, sandboxID string) (*SandboxRootFSHead, error) {
	return getRootFSHead(ctx, t.tx, sandboxID)
}

func (s *PGSandboxStore) GetRootFSHeadByID(ctx context.Context, headID, teamID string) (*SandboxRootFSHead, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(headID) == "" || strings.TrimSpace(teamID) == "" {
		return nil, nil
	}
	return getRootFSHeadByID(ctx, s.pool, headID, teamID)
}

// BindSandboxToRootFSHead creates an O(1) SandboxFS branch whose initial state
// is the immutable team-scoped ImageFS Head. The first writable checkpoint is
// published as a child Head by the normal ctld capture path.
func (s *PGSandboxStore) BindSandboxToRootFSHead(ctx context.Context, sandboxID, teamID, headID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	headID = strings.TrimSpace(headID)
	if sandboxID == "" || teamID == "" || headID == "" {
		return fmt.Errorf("sandbox_id, team_id, and head_id are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, base_image_ref, base_image_digest, head_id_v3, created_at, updated_at
		)
		SELECT $1, $2, h.base_image_ref, h.base_manifest_digest, h.head_id, NOW(), NOW()
		FROM manager.rootfs_heads_v3 h
		WHERE h.head_id = $3 AND h.team_id IN ($2, $4)
		ON CONFLICT (filesystem_id) DO UPDATE SET
			head_id_v3 = EXCLUDED.head_id_v3,
			updated_at = NOW()
		WHERE manager.rootfs_filesystems.team_id = EXCLUDED.team_id
	`, sandboxID, teamID, headID, rootfshead.PublicImageFSTeamID)
	if err != nil {
		return fmt.Errorf("bind sandbox ImageFS filesystem: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: ImageFS Head %s", ErrRootFSHeadConflict, headID)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (sandbox_id, filesystem_id, team_id, created_at, updated_at)
		VALUES ($1, $1, $2, NOW(), NOW())
		ON CONFLICT (sandbox_id) DO UPDATE SET
			filesystem_id = EXCLUDED.filesystem_id,
			team_id = EXCLUDED.team_id,
			updated_at = NOW()
	`, sandboxID, teamID); err != nil {
		return fmt.Errorf("bind sandbox to ImageFS Head: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) SaveRootFSExport(ctx context.Context, export *RootFSExport) error {
	if s == nil || s.pool == nil || export == nil {
		return nil
	}
	export.HeadID = strings.TrimSpace(export.HeadID)
	export.TeamID = strings.TrimSpace(export.TeamID)
	if export.HeadID == "" || export.TeamID == "" {
		return fmt.Errorf("rootfs export head_id and team_id are required")
	}
	if err := export.Object.Validate(rootfshead.ExportLayerMediaType); err != nil {
		return err
	}
	prefix, err := rootfshead.TeamObjectPrefix(export.TeamID)
	if err != nil {
		return err
	}
	if err := rootfshead.ValidateObjectScope(prefix, export.Object); err != nil {
		return err
	}
	diffID, err := digest.Parse(strings.TrimSpace(export.DiffID))
	if err != nil || diffID.Algorithm() != digest.Canonical {
		return fmt.Errorf("rootfs export diff_id %q is invalid", export.DiffID)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_objects_v3 (
			object_key, team_id, digest, media_type, size,
			last_referenced_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
		ON CONFLICT (object_key) DO UPDATE SET
			last_referenced_at = NOW(),
			missing_at = NULL,
			deleted_at = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE manager.rootfs_objects_v3.team_id = EXCLUDED.team_id
			AND manager.rootfs_objects_v3.digest = EXCLUDED.digest
			AND manager.rootfs_objects_v3.media_type = EXCLUDED.media_type
			AND manager.rootfs_objects_v3.size = EXCLUDED.size
	`, export.Object.Key, export.TeamID, export.Object.Digest, export.Object.MediaType, export.Object.Size)
	if err != nil {
		return fmt.Errorf("save rootfs export object: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: rootfs export object %s", ErrRootFSObjectConflict, export.Object.Key)
	}
	tag, err = tx.Exec(ctx, `
		INSERT INTO manager.rootfs_head_exports_v3 (head_id, object_key, diff_id, created_at)
		SELECT h.head_id, $3, $4, COALESCE($5, NOW())
		FROM manager.rootfs_heads_v3 h
		WHERE h.head_id = $1 AND h.team_id = $2
		ON CONFLICT (head_id) DO UPDATE SET head_id = EXCLUDED.head_id
		WHERE manager.rootfs_head_exports_v3.object_key = EXCLUDED.object_key
			AND manager.rootfs_head_exports_v3.diff_id = EXCLUDED.diff_id
	`, export.HeadID, export.TeamID, export.Object.Key, diffID.String(), nullableTime(export.CreatedAt))
	if err != nil {
		return fmt.Errorf("save rootfs Head export: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: rootfs Head export %s", ErrRootFSHeadConflict, export.HeadID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs Head export: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GetRootFSExport(ctx context.Context, headID, teamID string) (*RootFSExport, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(headID) == "" || strings.TrimSpace(teamID) == "" {
		return nil, nil
	}
	var export RootFSExport
	err := s.pool.QueryRow(ctx, `
		SELECT e.head_id, h.team_id, o.object_key, o.digest, o.size, o.media_type, e.diff_id, e.created_at
		FROM manager.rootfs_head_exports_v3 e
		JOIN manager.rootfs_heads_v3 h ON h.head_id = e.head_id
		JOIN manager.rootfs_objects_v3 o ON o.object_key = e.object_key AND o.team_id = h.team_id
		WHERE e.head_id = $1 AND h.team_id = $2 AND o.deleted_at IS NULL
	`, strings.TrimSpace(headID), strings.TrimSpace(teamID)).Scan(
		&export.HeadID, &export.TeamID, &export.Object.Key, &export.Object.Digest,
		&export.Object.Size, &export.Object.MediaType, &export.DiffID, &export.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs Head export: %w", err)
	}
	return &export, nil
}

func saveRootFSHead(ctx context.Context, db rootFSStoreDB, head *SandboxRootFSHead, publish bool) error {
	if db == nil || head == nil {
		return nil
	}
	objects, err := validateRootFSHeadPublication(head)
	if err != nil {
		return err
	}
	prefix, err := rootfshead.TeamObjectPrefix(head.TeamID)
	if err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_team_prefixes_v3 (team_id, object_prefix, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (team_id) DO UPDATE SET updated_at = NOW()
		WHERE manager.rootfs_team_prefixes_v3.object_prefix = EXCLUDED.object_prefix
	`, head.TeamID, prefix)
	if err != nil {
		return fmt.Errorf("register rootfs Head team prefix: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs Head team prefix conflicts with existing mapping")
	}
	for _, object := range objects {
		tag, err := db.Exec(ctx, `
			INSERT INTO manager.rootfs_objects_v3 (
				object_key, team_id, digest, media_type, size,
				last_referenced_at, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
			ON CONFLICT (object_key) DO UPDATE SET
				last_referenced_at = NOW(),
				missing_at = NULL,
				deleted_at = NULL,
				last_error = '',
				updated_at = NOW()
			WHERE manager.rootfs_objects_v3.team_id = EXCLUDED.team_id
				AND manager.rootfs_objects_v3.digest = EXCLUDED.digest
				AND manager.rootfs_objects_v3.media_type = EXCLUDED.media_type
				AND manager.rootfs_objects_v3.size = EXCLUDED.size
		`, object.Key, head.TeamID, object.Digest, object.MediaType, object.Size)
		if err != nil {
			return fmt.Errorf("save rootfs v3 object %s: %w", object.Key, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: rootfs v3 object %s", ErrRootFSObjectConflict, object.Key)
		}
	}

	parentHeadID := ""
	if head.Parent != nil {
		parentHeadID = head.Parent.HeadID
	}
	tag, err = db.Exec(ctx, `
		INSERT INTO manager.rootfs_heads_v3 (
			head_id, team_id, source_sandbox_id, runtime_generation, parent_head_id,
			manifest_key, manifest_digest, manifest_media_type, manifest_size,
			base_image_ref, base_manifest_digest, base_chain_id,
			platform_os, platform_architecture, platform_variant,
			image_name, image_manifest_digest,
			marker_key, marker_digest, marker_media_type, marker_size,
			envelope_key, envelope_digest, envelope_media_type, envelope_size,
			inventory_complete, created_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15,
			$16, $17,
			$18, $19, $20, $21,
			$22, $23, $24, $25,
			FALSE, COALESCE($26, NOW())
		)
		ON CONFLICT (head_id) DO UPDATE SET head_id = EXCLUDED.head_id
		WHERE manager.rootfs_heads_v3.team_id = EXCLUDED.team_id
			AND manager.rootfs_heads_v3.source_sandbox_id = EXCLUDED.source_sandbox_id
			AND manager.rootfs_heads_v3.runtime_generation = EXCLUDED.runtime_generation
			AND manager.rootfs_heads_v3.parent_head_id = EXCLUDED.parent_head_id
			AND manager.rootfs_heads_v3.manifest_key = EXCLUDED.manifest_key
			AND manager.rootfs_heads_v3.manifest_digest = EXCLUDED.manifest_digest
			AND manager.rootfs_heads_v3.manifest_media_type = EXCLUDED.manifest_media_type
			AND manager.rootfs_heads_v3.manifest_size = EXCLUDED.manifest_size
			AND manager.rootfs_heads_v3.base_image_ref = EXCLUDED.base_image_ref
			AND manager.rootfs_heads_v3.base_manifest_digest = EXCLUDED.base_manifest_digest
			AND manager.rootfs_heads_v3.base_chain_id = EXCLUDED.base_chain_id
			AND manager.rootfs_heads_v3.platform_os = EXCLUDED.platform_os
			AND manager.rootfs_heads_v3.platform_architecture = EXCLUDED.platform_architecture
			AND manager.rootfs_heads_v3.platform_variant = EXCLUDED.platform_variant
			AND manager.rootfs_heads_v3.image_name = EXCLUDED.image_name
			AND manager.rootfs_heads_v3.image_manifest_digest = EXCLUDED.image_manifest_digest
			AND manager.rootfs_heads_v3.marker_key = EXCLUDED.marker_key
			AND manager.rootfs_heads_v3.marker_digest = EXCLUDED.marker_digest
			AND manager.rootfs_heads_v3.marker_media_type = EXCLUDED.marker_media_type
			AND manager.rootfs_heads_v3.marker_size = EXCLUDED.marker_size
			AND manager.rootfs_heads_v3.envelope_key = EXCLUDED.envelope_key
			AND manager.rootfs_heads_v3.envelope_digest = EXCLUDED.envelope_digest
			AND manager.rootfs_heads_v3.envelope_media_type = EXCLUDED.envelope_media_type
			AND manager.rootfs_heads_v3.envelope_size = EXCLUDED.envelope_size
	`, head.Reference.HeadID, head.TeamID, head.SourceSandboxID, head.RuntimeGeneration, parentHeadID,
		head.Reference.Manifest.Key, head.Reference.Manifest.Digest, head.Reference.Manifest.MediaType, head.Reference.Manifest.Size,
		head.Base.ImageReference, head.Base.ManifestDigest, head.Base.ChainID,
		head.Base.OS, head.Base.Architecture, head.Base.Variant,
		head.Image.Name, head.Image.ManifestDigest,
		head.Image.Marker.Key, head.Image.Marker.Digest, head.Image.Marker.MediaType, head.Image.Marker.Size,
		head.Image.Envelope.Key, head.Image.Envelope.Digest, head.Image.Envelope.MediaType, head.Image.Envelope.Size,
		nullableTime(head.CreatedAt))
	if err != nil {
		return fmt.Errorf("save rootfs v3 Head: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: immutable rootfs v3 Head %s", ErrRootFSHeadConflict, head.Reference.HeadID)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_head_prefix_guards_v3 (
			head_id, team_id, object_prefix, created_at
		)
		SELECT h.head_id, h.team_id, p.object_prefix, NOW()
		FROM manager.rootfs_heads_v3 h
		JOIN manager.rootfs_team_prefixes_v3 p ON p.team_id = h.team_id
		WHERE h.head_id = $1 AND h.inventory_complete = FALSE
		ON CONFLICT (head_id) DO NOTHING
	`, head.Reference.HeadID); err != nil {
		return fmt.Errorf("protect uninventoried rootfs v3 Head prefix: %w", err)
	}

	for _, object := range objects {
		if _, err := db.Exec(ctx, `
			INSERT INTO manager.rootfs_head_objects_v3 (head_id, object_key, conservative, created_at)
			VALUES ($1, $2, TRUE, NOW())
			ON CONFLICT (head_id, object_key) DO NOTHING
		`, head.Reference.HeadID, object.Key); err != nil {
			return fmt.Errorf("link rootfs v3 Head object: %w", err)
		}
	}
	if head.Parent != nil {
		tag, err := db.Exec(ctx, `
			INSERT INTO manager.rootfs_head_parent_guards_v3 (child_head_id, parent_head_id, created_at)
			SELECT child.head_id, parent.head_id, NOW()
			FROM manager.rootfs_heads_v3 child
			JOIN manager.rootfs_heads_v3 parent
				ON parent.head_id = $2
				AND parent.team_id IN (child.team_id, $7)
				AND parent.manifest_key = $3
				AND parent.manifest_digest = $4
				AND parent.manifest_media_type = $5
				AND parent.manifest_size = $6
			WHERE child.head_id = $1
				AND child.inventory_complete = FALSE
			ON CONFLICT (child_head_id) DO UPDATE SET parent_head_id = EXCLUDED.parent_head_id
			WHERE manager.rootfs_head_parent_guards_v3.parent_head_id = EXCLUDED.parent_head_id
		`, head.Reference.HeadID, head.Parent.HeadID,
			head.Parent.Manifest.Key, head.Parent.Manifest.Digest, head.Parent.Manifest.MediaType, head.Parent.Manifest.Size,
			rootfshead.PublicImageFSTeamID)
		if err != nil {
			return fmt.Errorf("protect parent rootfs v3 Head: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var inventoryComplete bool
			if scanErr := db.QueryRow(ctx, `SELECT inventory_complete FROM manager.rootfs_heads_v3 WHERE head_id = $1`, head.Reference.HeadID).Scan(&inventoryComplete); scanErr != nil || !inventoryComplete {
				return fmt.Errorf("%w: parent rootfs v3 Head %s", ErrRootFSHeadConflict, head.Parent.HeadID)
			}
		}
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_inventory_jobs_v3 (head_id, state, next_attempt_at, created_at, updated_at)
		VALUES ($1, 'pending', NOW(), NOW(), NOW())
		ON CONFLICT (head_id) DO NOTHING
	`, head.Reference.HeadID); err != nil {
		return fmt.Errorf("queue rootfs v3 inventory: %w", err)
	}
	if !publish {
		return nil
	}

	if _, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, base_image_ref, base_image_digest, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (filesystem_id) DO NOTHING
	`, head.SandboxID, head.TeamID, head.Base.ImageReference, head.Base.ManifestDigest); err != nil {
		return fmt.Errorf("ensure rootfs v3 filesystem: %w", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (
			sandbox_id, filesystem_id, team_id, created_at, updated_at
		)
		SELECT $1, filesystem_id, $2, NOW(), NOW()
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = $1 AND team_id = $2
		ON CONFLICT (sandbox_id) DO NOTHING
	`, head.SandboxID, head.TeamID); err != nil {
		return fmt.Errorf("ensure rootfs v3 binding: %w", err)
	}
	expectedHeadID := ""
	if head.Parent != nil {
		expectedHeadID = head.Parent.HeadID
	}
	tag, err = db.Exec(ctx, `
		UPDATE manager.rootfs_filesystems f
		SET head_id_v3 = $3,
			base_image_ref = $4,
			base_image_digest = $5,
			updated_at = NOW()
		FROM manager.sandbox_rootfs_bindings b
		WHERE b.sandbox_id = $1
			AND b.filesystem_id = f.filesystem_id
			AND b.team_id = $2
			AND f.team_id = $2
			AND (COALESCE(f.head_id_v3, '') = $6 OR f.head_id_v3 = $3)
	`, head.SandboxID, head.TeamID, head.Reference.HeadID,
		head.Base.ImageReference, head.Base.ManifestDigest, expectedHeadID)
	if err != nil {
		return fmt.Errorf("advance rootfs v3 filesystem Head: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s", ErrRootFSHeadConflict, head.SandboxID)
	}
	return nil
}

func validateRootFSHeadPublication(head *SandboxRootFSHead) ([]rootfshead.Object, error) {
	if head == nil {
		return nil, nil
	}
	head.SandboxID = strings.TrimSpace(head.SandboxID)
	head.SourceSandboxID = strings.TrimSpace(head.SourceSandboxID)
	head.TeamID = strings.TrimSpace(head.TeamID)
	if head.SandboxID == "" || head.TeamID == "" || head.RuntimeGeneration <= 0 {
		return nil, fmt.Errorf("sandbox_id, team_id, and runtime_generation are required")
	}
	if head.SourceSandboxID == "" {
		return nil, fmt.Errorf("source_sandbox_id is required")
	}
	if err := head.Reference.Validate(); err != nil {
		return nil, err
	}
	if err := head.Base.Validate(); err != nil {
		return nil, err
	}
	if err := head.Image.Validate(); err != nil {
		return nil, err
	}
	if head.Image.Platform.OS != head.Base.OS || head.Image.Platform.Architecture != head.Base.Architecture || head.Image.Platform.Variant != head.Base.Variant {
		return nil, fmt.Errorf("rootfs Head image platform does not match base platform")
	}
	prefix, err := rootfshead.TeamObjectPrefix(head.TeamID)
	if err != nil {
		return nil, err
	}
	objects := []rootfshead.Object{head.Reference.Manifest, head.Image.Marker, head.Image.Envelope}
	unique := make(map[string]rootfshead.Object, len(objects))
	for _, object := range objects {
		if err := object.Validate(""); err != nil {
			return nil, err
		}
		if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
			return nil, err
		}
		if existing, ok := unique[object.Key]; ok && existing != object {
			return nil, fmt.Errorf("rootfs object %s has conflicting descriptors", object.Key)
		}
		unique[object.Key] = object
	}
	objects = objects[:0]
	for _, object := range unique {
		objects = append(objects, object)
	}
	if head.Parent != nil {
		if err := head.Parent.Validate(); err != nil {
			return nil, err
		}
		if head.Parent.HeadID == head.Reference.HeadID {
			return nil, fmt.Errorf("rootfs Head cannot reference itself as parent")
		}
		if err := rootfshead.ValidateReadableObjectScope(prefix, head.Parent.Manifest); err != nil {
			return nil, err
		}
	}
	return objects, nil
}

func getRootFSHead(ctx context.Context, db rootFSStoreDB, sandboxID string) (*SandboxRootFSHead, error) {
	var head SandboxRootFSHead
	var parentID, parentKey, parentDigest, parentMediaType *string
	var parentSize *int64
	err := db.QueryRow(ctx, `
		SELECT b.sandbox_id, h.source_sandbox_id, h.team_id, h.runtime_generation,
			h.head_id, h.manifest_key, h.manifest_digest, h.manifest_media_type, h.manifest_size,
			h.base_image_ref, h.base_manifest_digest, h.base_chain_id,
			h.platform_os, h.platform_architecture, h.platform_variant,
			h.image_name, h.image_manifest_digest,
			h.marker_key, h.marker_digest, h.marker_media_type, h.marker_size,
			h.envelope_key, h.envelope_digest, h.envelope_media_type, h.envelope_size,
			h.inventory_complete, h.created_at,
			p.head_id, p.manifest_key, p.manifest_digest, p.manifest_media_type, p.manifest_size
		FROM manager.sandbox_rootfs_bindings b
		JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
		JOIN manager.rootfs_heads_v3 h ON h.head_id = f.head_id_v3 AND h.team_id IN (b.team_id, $2)
		LEFT JOIN manager.rootfs_heads_v3 p ON p.head_id = NULLIF(h.parent_head_id, '') AND p.team_id IN (h.team_id, $2)
		WHERE b.sandbox_id = $1
	`, strings.TrimSpace(sandboxID), rootfshead.PublicImageFSTeamID).Scan(
		&head.SandboxID, &head.SourceSandboxID, &head.TeamID, &head.RuntimeGeneration,
		&head.Reference.HeadID, &head.Reference.Manifest.Key, &head.Reference.Manifest.Digest,
		&head.Reference.Manifest.MediaType, &head.Reference.Manifest.Size,
		&head.Base.ImageReference, &head.Base.ManifestDigest, &head.Base.ChainID,
		&head.Base.OS, &head.Base.Architecture, &head.Base.Variant,
		&head.Image.Name, &head.Image.ManifestDigest,
		&head.Image.Marker.Key, &head.Image.Marker.Digest, &head.Image.Marker.MediaType, &head.Image.Marker.Size,
		&head.Image.Envelope.Key, &head.Image.Envelope.Digest, &head.Image.Envelope.MediaType, &head.Image.Envelope.Size,
		&head.InventoryComplete, &head.CreatedAt,
		&parentID, &parentKey, &parentDigest, &parentMediaType, &parentSize,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get rootfs v3 Head: %w", err)
	}
	head.Reference.Version = rootfshead.Version
	head.Image.Platform.OS = head.Base.OS
	head.Image.Platform.Architecture = head.Base.Architecture
	head.Image.Platform.Variant = head.Base.Variant
	if parentID != nil && parentKey != nil && parentDigest != nil && parentMediaType != nil && parentSize != nil {
		head.Parent = &rootfshead.HeadReference{
			Version: rootfshead.Version,
			HeadID:  *parentID,
			Manifest: rootfshead.Object{
				Key: *parentKey, Digest: *parentDigest, MediaType: *parentMediaType, Size: *parentSize,
			},
		}
	}
	return &head, nil
}

func getRootFSHeadByID(ctx context.Context, db rootFSStoreDB, headID, teamID string) (*SandboxRootFSHead, error) {
	var head SandboxRootFSHead
	var parentID, parentKey, parentDigest, parentMediaType *string
	var parentSize *int64
	err := db.QueryRow(ctx, `
		SELECT h.source_sandbox_id, h.source_sandbox_id, h.team_id, h.runtime_generation,
			h.head_id, h.manifest_key, h.manifest_digest, h.manifest_media_type, h.manifest_size,
			h.base_image_ref, h.base_manifest_digest, h.base_chain_id,
			h.platform_os, h.platform_architecture, h.platform_variant,
			h.image_name, h.image_manifest_digest,
			h.marker_key, h.marker_digest, h.marker_media_type, h.marker_size,
			h.envelope_key, h.envelope_digest, h.envelope_media_type, h.envelope_size,
			h.inventory_complete, h.created_at,
			p.head_id, p.manifest_key, p.manifest_digest, p.manifest_media_type, p.manifest_size
		FROM manager.rootfs_heads_v3 h
		LEFT JOIN manager.rootfs_heads_v3 p ON p.head_id = NULLIF(h.parent_head_id, '') AND p.team_id IN (h.team_id, $3)
		WHERE h.head_id = $1 AND h.team_id IN ($2, $3)
	`, strings.TrimSpace(headID), strings.TrimSpace(teamID), rootfshead.PublicImageFSTeamID).Scan(
		&head.SandboxID, &head.SourceSandboxID, &head.TeamID, &head.RuntimeGeneration,
		&head.Reference.HeadID, &head.Reference.Manifest.Key, &head.Reference.Manifest.Digest,
		&head.Reference.Manifest.MediaType, &head.Reference.Manifest.Size,
		&head.Base.ImageReference, &head.Base.ManifestDigest, &head.Base.ChainID,
		&head.Base.OS, &head.Base.Architecture, &head.Base.Variant,
		&head.Image.Name, &head.Image.ManifestDigest,
		&head.Image.Marker.Key, &head.Image.Marker.Digest, &head.Image.Marker.MediaType, &head.Image.Marker.Size,
		&head.Image.Envelope.Key, &head.Image.Envelope.Digest, &head.Image.Envelope.MediaType, &head.Image.Envelope.Size,
		&head.InventoryComplete, &head.CreatedAt,
		&parentID, &parentKey, &parentDigest, &parentMediaType, &parentSize,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs v3 Head by id: %w", err)
	}
	head.Reference.Version = rootfshead.Version
	head.Image.Platform.OS = head.Base.OS
	head.Image.Platform.Architecture = head.Base.Architecture
	head.Image.Platform.Variant = head.Base.Variant
	if parentID != nil && parentKey != nil && parentDigest != nil && parentMediaType != nil && parentSize != nil {
		head.Parent = &rootfshead.HeadReference{
			Version: rootfshead.Version,
			HeadID:  *parentID,
			Manifest: rootfshead.Object{
				Key: *parentKey, Digest: *parentDigest, MediaType: *parentMediaType, Size: *parentSize,
			},
		}
	}
	return &head, nil
}
