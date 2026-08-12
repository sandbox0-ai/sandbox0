-- +goose Up

CREATE TABLE manager.rootfs_team_prefixes_v3 (
    team_id TEXT PRIMARY KEY,
    object_prefix TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE manager.rootfs_capture_leases_v3 (
    sandbox_id TEXT NOT NULL REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    runtime_generation BIGINT NOT NULL CHECK (runtime_generation > 0),
    team_id TEXT NOT NULL,
    object_prefix TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    protect_all BOOLEAN NOT NULL DEFAULT TRUE,
    object_epoch BIGINT NOT NULL DEFAULT 1 CHECK (object_epoch > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sandbox_id, runtime_generation)
);

CREATE INDEX idx_rootfs_capture_leases_v3_prefix
    ON manager.rootfs_capture_leases_v3(object_prefix, active, protect_all);

CREATE TABLE manager.rootfs_capture_lease_objects_v3 (
    sandbox_id TEXT NOT NULL,
    runtime_generation BIGINT NOT NULL,
    object_epoch BIGINT NOT NULL CHECK (object_epoch > 0),
    object_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sandbox_id, runtime_generation, object_epoch, object_key),
    FOREIGN KEY (sandbox_id, runtime_generation)
        REFERENCES manager.rootfs_capture_leases_v3(sandbox_id, runtime_generation)
        ON DELETE CASCADE
);

CREATE INDEX idx_rootfs_capture_lease_objects_v3_key
    ON manager.rootfs_capture_lease_objects_v3(object_key);

CREATE TABLE manager.rootfs_write_leases_v3 (
    lease_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    object_prefix TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rootfs_write_leases_v3_prefix_expiry
    ON manager.rootfs_write_leases_v3(object_prefix, expires_at);

CREATE TABLE manager.rootfs_head_prefix_guards_v3 (
    head_id TEXT PRIMARY KEY REFERENCES manager.rootfs_heads_v3(head_id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    object_prefix TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rootfs_head_prefix_guards_v3_team_prefix
    ON manager.rootfs_head_prefix_guards_v3(team_id, object_prefix);

-- +goose Down

DROP INDEX manager.idx_rootfs_head_prefix_guards_v3_team_prefix;
DROP TABLE manager.rootfs_head_prefix_guards_v3;
DROP INDEX manager.idx_rootfs_write_leases_v3_prefix_expiry;
DROP TABLE manager.rootfs_write_leases_v3;
DROP INDEX manager.idx_rootfs_capture_lease_objects_v3_key;
DROP TABLE manager.rootfs_capture_lease_objects_v3;
DROP INDEX manager.idx_rootfs_capture_leases_v3_prefix;
DROP TABLE manager.rootfs_capture_leases_v3;
DROP TABLE manager.rootfs_team_prefixes_v3;
