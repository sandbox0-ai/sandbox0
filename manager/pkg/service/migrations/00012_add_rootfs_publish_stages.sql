-- +goose Up

CREATE TABLE IF NOT EXISTS manager.rootfs_publish_stages (
    stage_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    ctld_address TEXT NOT NULL,
    runtime_generation BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    release_after TIMESTAMPTZ NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (runtime_generation >= 0),
    CHECK (release_after >= expires_at)
);

CREATE INDEX IF NOT EXISTS idx_rootfs_publish_stages_release
    ON manager.rootfs_publish_stages(release_after ASC, created_at ASC);

DROP TRIGGER IF EXISTS update_rootfs_publish_stages_updated_at ON manager.rootfs_publish_stages;
CREATE TRIGGER update_rootfs_publish_stages_updated_at
    BEFORE UPDATE ON manager.rootfs_publish_stages
    FOR EACH ROW
    EXECUTE FUNCTION manager.update_updated_at_column();

-- +goose Down

DROP TRIGGER IF EXISTS update_rootfs_publish_stages_updated_at ON manager.rootfs_publish_stages;
DROP INDEX IF EXISTS manager.idx_rootfs_publish_stages_release;
DROP TABLE IF EXISTS manager.rootfs_publish_stages;
