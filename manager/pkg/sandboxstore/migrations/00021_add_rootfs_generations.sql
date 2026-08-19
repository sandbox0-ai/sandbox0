-- +goose Up

CREATE TABLE IF NOT EXISTS manager.rootfs_base_artifacts (
    artifact_digest TEXT PRIMARY KEY,
    source_oci_ref TEXT NOT NULL,
    source_oci_digest TEXT NOT NULL,
    base_block_root TEXT NOT NULL,
    format_generation INTEGER NOT NULL CHECK (format_generation > 0),
    state TEXT NOT NULL CHECK (state IN ('building', 'ready', 'failed', 'retired')),
    descriptor BYTEA NOT NULL CHECK (octet_length(descriptor) <= 65536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_oci_digest, artifact_digest, format_generation)
);

CREATE INDEX IF NOT EXISTS idx_rootfs_base_artifacts_source_ready
    ON manager.rootfs_base_artifacts(source_oci_digest, format_generation DESC)
    WHERE state = 'ready';

ALTER TABLE manager.rootfs_filesystems
    ADD COLUMN IF NOT EXISTS storage_format TEXT NOT NULL DEFAULT 'legacy-layer'
        CHECK (storage_format IN ('legacy-layer', 'block-cow-v1')),
    ADD COLUMN IF NOT EXISTS base_artifact_digest TEXT,
    ADD COLUMN IF NOT EXISTS format_generation INTEGER,
    ADD COLUMN IF NOT EXISTS head_generation_id TEXT;

ALTER TABLE manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_base_artifact_fk
        FOREIGN KEY (base_artifact_digest)
        REFERENCES manager.rootfs_base_artifacts(artifact_digest)
        ON DELETE RESTRICT,
    ADD CONSTRAINT rootfs_filesystems_format_shape_check
        CHECK (
            (storage_format = 'legacy-layer'
                AND base_artifact_digest IS NULL
                AND format_generation IS NULL
                AND head_generation_id IS NULL)
            OR
            (storage_format = 'block-cow-v1'
                AND base_artifact_digest IS NOT NULL
                AND format_generation IS NOT NULL
                AND format_generation > 0
                AND head_layer_id IS NULL)
        );

CREATE TABLE IF NOT EXISTS manager.rootfs_generations (
    generation_id TEXT PRIMARY KEY,
    filesystem_id TEXT NOT NULL
        REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE RESTRICT,
    parent_generation_id TEXT
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT,
    source_oci_digest TEXT NOT NULL,
    base_artifact_digest TEXT NOT NULL
        REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT,
    base_block_root TEXT NOT NULL,
    current_block_head TEXT NOT NULL,
    writer_epoch BIGINT NOT NULL CHECK (writer_epoch >= 0),
    format_generation INTEGER NOT NULL CHECK (format_generation > 0),
    durability_state TEXT NOT NULL CHECK (durability_state IN (
        'local_sealed', 'composite_durable', 's3_materialized'
    )),
    locator_version BIGINT NOT NULL CHECK (locator_version > 0),
    descriptor BYTEA NOT NULL CHECK (octet_length(descriptor) <= 65536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (filesystem_id, writer_epoch),
    CHECK (parent_generation_id IS NULL OR parent_generation_id <> generation_id)
);

ALTER TABLE manager.rootfs_filesystems
    ADD CONSTRAINT rootfs_filesystems_head_generation_fk
        FOREIGN KEY (head_generation_id)
        REFERENCES manager.rootfs_generations(generation_id)
        ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_rootfs_generations_filesystem_created
    ON manager.rootfs_generations(filesystem_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rootfs_generations_parent
    ON manager.rootfs_generations(parent_generation_id)
    WHERE parent_generation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rootfs_filesystems_generation_head
    ON manager.rootfs_filesystems(head_generation_id)
    WHERE head_generation_id IS NOT NULL;

ALTER TABLE manager.rootfs_writer_grants
    ADD COLUMN IF NOT EXISTS initial_generation_id TEXT NOT NULL DEFAULT '';

UPDATE manager.rootfs_writer_grants
SET initial_generation_id = initial_head_layer_id
WHERE initial_generation_id = '';

-- +goose Down

ALTER TABLE manager.rootfs_writer_grants
    DROP COLUMN IF EXISTS initial_generation_id;
DROP INDEX IF EXISTS manager.idx_rootfs_filesystems_generation_head;
DROP INDEX IF EXISTS manager.idx_rootfs_generations_parent;
DROP INDEX IF EXISTS manager.idx_rootfs_generations_filesystem_created;
ALTER TABLE manager.rootfs_filesystems
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_head_generation_fk;
DROP TABLE IF EXISTS manager.rootfs_generations;
ALTER TABLE manager.rootfs_filesystems
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_format_shape_check,
    DROP CONSTRAINT IF EXISTS rootfs_filesystems_base_artifact_fk,
    DROP COLUMN IF EXISTS head_generation_id,
    DROP COLUMN IF EXISTS format_generation,
    DROP COLUMN IF EXISTS base_artifact_digest,
    DROP COLUMN IF EXISTS storage_format;
DROP INDEX IF EXISTS manager.idx_rootfs_base_artifacts_source_ready;
DROP TABLE IF EXISTS manager.rootfs_base_artifacts;
