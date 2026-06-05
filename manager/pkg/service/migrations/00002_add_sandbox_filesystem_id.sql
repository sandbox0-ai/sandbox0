-- +goose Up

ALTER TABLE sandboxes
    ADD COLUMN IF NOT EXISTS filesystem_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_sandboxes_team_filesystem
    ON sandboxes(team_id, filesystem_id)
    WHERE filesystem_id <> '';

-- +goose Down

DROP INDEX IF EXISTS idx_sandboxes_team_filesystem;

ALTER TABLE sandboxes
    DROP COLUMN IF EXISTS filesystem_id;
