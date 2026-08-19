-- +goose Up

CREATE TABLE IF NOT EXISTS manager.rootfs_composite_backlog_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    max_descriptor_bytes BIGINT NOT NULL CHECK (max_descriptor_bytes > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO manager.rootfs_composite_backlog_policy (
    singleton, max_descriptor_bytes
) VALUES (TRUE, 1073741824)
ON CONFLICT (singleton) DO NOTHING;

-- +goose Down

DROP TABLE IF EXISTS manager.rootfs_composite_backlog_policy;
