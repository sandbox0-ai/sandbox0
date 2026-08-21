-- +goose Up

-- A materialization batch fixes exact membership before any object PUT. The
-- complete batch publishes atomically, so a crash cannot leave only part of a
-- shared pack reachable from PostgreSQL.
CREATE TABLE manager.rootfs_materialization_batches (
    batch_id TEXT PRIMARY KEY,
    pack_lane TEXT NOT NULL CHECK (pack_lane <> '' AND octet_length(pack_lane) <= 256),
    team_id TEXT NOT NULL CHECK (team_id <> ''),
    format_generation INTEGER NOT NULL CHECK (format_generation > 0),
    member_count INTEGER NOT NULL CHECK (member_count > 0 AND member_count <= 10000),
    state TEXT NOT NULL CHECK (state IN ('uploading', 'published', 'abandoned')),
    abandon_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    abandoned_at TIMESTAMPTZ,
    CHECK (
        (state = 'uploading' AND published_at IS NULL AND abandoned_at IS NULL AND abandon_reason = '')
        OR (state = 'published' AND published_at IS NOT NULL AND abandoned_at IS NULL AND abandon_reason = '')
        OR (state = 'abandoned' AND published_at IS NULL AND abandoned_at IS NOT NULL AND abandon_reason <> '')
    )
);

CREATE INDEX idx_rootfs_materialization_batches_state
    ON manager.rootfs_materialization_batches(state, updated_at, batch_id);

CREATE TABLE manager.rootfs_materialization_members (
    batch_id TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_batches(batch_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 10000),
    -- The immutable identity must outlive a generation deleted after a
    -- successful publication so commit-response-loss retries remain auditable.
    -- Uploading batches are fenced explicitly by MarkSandboxDeleted.
    generation_id TEXT NOT NULL,
    expected_locator_version BIGINT NOT NULL CHECK (expected_locator_version > 0),
    expected_descriptor BYTEA NOT NULL
        CHECK (octet_length(expected_descriptor) BETWEEN 1 AND 65536),
    expected_descriptor_digest BYTEA NOT NULL
        CHECK (octet_length(expected_descriptor_digest) = 32),
    state TEXT NOT NULL CHECK (state IN ('uploading', 'published', 'abandoned')),
    materialized_descriptor BYTEA,
    published_locator_version BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, ordinal),
    UNIQUE (batch_id, generation_id),
    CHECK (
        (state = 'published' AND materialized_descriptor IS NOT NULL
            AND published_locator_version = expected_locator_version + 1)
        OR (state IN ('uploading', 'abandoned') AND materialized_descriptor IS NULL
            AND published_locator_version IS NULL)
    )
);

CREATE UNIQUE INDEX idx_rootfs_materialization_members_uploading_generation
    ON manager.rootfs_materialization_members(generation_id)
    WHERE state = 'uploading';

CREATE TABLE manager.rootfs_materialization_objects (
    object_key TEXT PRIMARY KEY,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('data_pack', 'mapping_page')),
    object_size BIGINT NOT NULL CHECK (object_size > 0),
    checksum TEXT NOT NULL CHECK (checksum ~ '^sha256:[0-9a-f]{64}$'),
    uploaded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE manager.rootfs_materialization_batch_objects (
    batch_id TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_batches(batch_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    upload_state TEXT NOT NULL CHECK (upload_state IN ('registered', 'uploaded')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, object_key)
);

CREATE INDEX idx_rootfs_materialization_batch_objects_state
    ON manager.rootfs_materialization_batch_objects(upload_state, updated_at, batch_id);

-- These are current immutable-locator roots. A whole shared object remains
-- live while any generation locator references one of its ranges.
CREATE TABLE manager.rootfs_generation_materialization_objects (
    generation_id TEXT NOT NULL
        REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE,
    locator_version BIGINT NOT NULL CHECK (locator_version > 0),
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (generation_id, locator_version, object_key)
);

CREATE INDEX idx_rootfs_generation_materialization_objects_object
    ON manager.rootfs_generation_materialization_objects(object_key, generation_id, locator_version);

-- Preserve the exact per-member set for commit-response-loss validation and
-- for proving that shared batch objects were not silently attached to another
-- logical generation.
CREATE TABLE manager.rootfs_materialization_member_objects (
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    object_key TEXT NOT NULL
        REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, ordinal, object_key),
    FOREIGN KEY (batch_id, ordinal)
        REFERENCES manager.rootfs_materialization_members(batch_id, ordinal) ON DELETE CASCADE
);

-- +goose Down

DROP TABLE IF EXISTS manager.rootfs_materialization_member_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_generation_materialization_objects_object;
DROP TABLE IF EXISTS manager.rootfs_generation_materialization_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_materialization_batch_objects_state;
DROP TABLE IF EXISTS manager.rootfs_materialization_batch_objects;
DROP TABLE IF EXISTS manager.rootfs_materialization_objects;
DROP INDEX IF EXISTS manager.idx_rootfs_materialization_members_uploading_generation;
DROP TABLE IF EXISTS manager.rootfs_materialization_members;
DROP INDEX IF EXISTS manager.idx_rootfs_materialization_batches_state;
DROP TABLE IF EXISTS manager.rootfs_materialization_batches;
