-- +goose Up

CREATE TABLE manager.rootfs_objects_v3 (
    object_key TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size BIGINT NOT NULL CHECK (size > 0),
    last_referenced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rootfs_objects_v3_team_updated
    ON manager.rootfs_objects_v3(team_id, updated_at DESC);

CREATE INDEX idx_rootfs_objects_v3_unreferenced
    ON manager.rootfs_objects_v3(last_referenced_at ASC)
    WHERE deleted_at IS NULL;

CREATE TABLE manager.rootfs_heads_v3 (
    head_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    source_sandbox_id TEXT NOT NULL,
    runtime_generation BIGINT NOT NULL,
    parent_head_id TEXT NOT NULL DEFAULT '',
    manifest_key TEXT NOT NULL UNIQUE,
    manifest_digest TEXT NOT NULL,
    manifest_media_type TEXT NOT NULL,
    manifest_size BIGINT NOT NULL CHECK (manifest_size > 0),
    base_image_ref TEXT NOT NULL,
    base_manifest_digest TEXT NOT NULL,
    base_chain_id TEXT NOT NULL,
    platform_os TEXT NOT NULL,
    platform_architecture TEXT NOT NULL,
    platform_variant TEXT NOT NULL DEFAULT '',
    image_name TEXT NOT NULL,
    image_manifest_digest TEXT NOT NULL,
    marker_key TEXT NOT NULL,
    marker_digest TEXT NOT NULL,
    marker_media_type TEXT NOT NULL,
    marker_size BIGINT NOT NULL CHECK (marker_size > 0),
    envelope_key TEXT NOT NULL,
    envelope_digest TEXT NOT NULL,
    envelope_media_type TEXT NOT NULL,
    envelope_size BIGINT NOT NULL CHECK (envelope_size > 0),
    inventory_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rootfs_heads_v3_team_created
    ON manager.rootfs_heads_v3(team_id, created_at DESC);

CREATE INDEX idx_rootfs_heads_v3_parent
    ON manager.rootfs_heads_v3(parent_head_id)
    WHERE parent_head_id <> '';

CREATE TABLE manager.rootfs_head_objects_v3 (
    head_id TEXT NOT NULL REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL REFERENCES manager.rootfs_objects_v3(object_key) ON DELETE RESTRICT,
    conservative BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (head_id, object_key)
);

CREATE INDEX idx_rootfs_head_objects_v3_object
    ON manager.rootfs_head_objects_v3(object_key);

CREATE TABLE manager.rootfs_head_exports_v3 (
    head_id TEXT PRIMARY KEY REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL REFERENCES manager.rootfs_objects_v3(object_key) ON DELETE RESTRICT,
    diff_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rootfs_head_exports_v3_object
    ON manager.rootfs_head_exports_v3(object_key);

CREATE TABLE manager.rootfs_head_parent_guards_v3 (
    child_head_id TEXT PRIMARY KEY REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE CASCADE,
    parent_head_id TEXT NOT NULL REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (child_head_id <> parent_head_id)
);

CREATE INDEX idx_rootfs_head_parent_guards_v3_parent
    ON manager.rootfs_head_parent_guards_v3(parent_head_id);

CREATE TABLE manager.rootfs_inventory_jobs_v3 (
    head_id TEXT PRIMARY KEY REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE CASCADE,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT NOT NULL DEFAULT '',
    claimed_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (state IN ('pending', 'running', 'complete', 'dead'))
);

CREATE INDEX idx_rootfs_inventory_jobs_v3_due
    ON manager.rootfs_inventory_jobs_v3(next_attempt_at ASC, created_at ASC)
    WHERE state IN ('pending', 'running');

ALTER TABLE manager.rootfs_filesystems
    ADD COLUMN head_id_v3 TEXT REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE RESTRICT;

CREATE INDEX idx_rootfs_filesystems_head_v3
    ON manager.rootfs_filesystems(head_id_v3)
    WHERE head_id_v3 IS NOT NULL;

ALTER TABLE manager.rootfs_snapshots
    ALTER COLUMN head_layer_id DROP NOT NULL,
    ADD COLUMN head_id_v3 TEXT REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE RESTRICT;

CREATE INDEX idx_rootfs_snapshots_head_v3
    ON manager.rootfs_snapshots(head_id_v3)
    WHERE head_id_v3 IS NOT NULL;

ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN expected_head_id_v3 TEXT NOT NULL DEFAULT '',
    ADD COLUMN prepared_head_id_v3 TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP COLUMN prepared_head_id_v3,
    DROP COLUMN expected_head_id_v3;

DROP INDEX manager.idx_rootfs_snapshots_head_v3;
ALTER TABLE manager.rootfs_snapshots DROP COLUMN head_id_v3;
DELETE FROM manager.rootfs_snapshots WHERE head_layer_id IS NULL;
ALTER TABLE manager.rootfs_snapshots ALTER COLUMN head_layer_id SET NOT NULL;

DROP INDEX manager.idx_rootfs_filesystems_head_v3;
ALTER TABLE manager.rootfs_filesystems DROP COLUMN head_id_v3;

DROP INDEX manager.idx_rootfs_inventory_jobs_v3_due;
DROP TABLE manager.rootfs_inventory_jobs_v3;
DROP INDEX manager.idx_rootfs_head_parent_guards_v3_parent;
DROP TABLE manager.rootfs_head_parent_guards_v3;
DROP INDEX manager.idx_rootfs_head_objects_v3_object;
DROP INDEX manager.idx_rootfs_head_exports_v3_object;
DROP TABLE manager.rootfs_head_exports_v3;
DROP TABLE manager.rootfs_head_objects_v3;
DROP INDEX manager.idx_rootfs_heads_v3_parent;
DROP INDEX manager.idx_rootfs_heads_v3_team_created;
DROP TABLE manager.rootfs_heads_v3;
DROP INDEX manager.idx_rootfs_objects_v3_unreferenced;
DROP INDEX manager.idx_rootfs_objects_v3_team_updated;
DROP TABLE manager.rootfs_objects_v3;
