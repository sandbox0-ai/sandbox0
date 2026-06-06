-- +goose Up

ALTER TABLE manager.sandbox_filesystems
    ADD COLUMN IF NOT EXISTS lifecycle_owner_sandbox_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_sandbox_filesystems_lifecycle_owner
    ON manager.sandbox_filesystems(lifecycle_owner_sandbox_id)
    WHERE lifecycle_owner_sandbox_id <> '';

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandbox_filesystems_lifecycle_owner;

ALTER TABLE manager.sandbox_filesystems
    DROP COLUMN IF EXISTS lifecycle_owner_sandbox_id;
