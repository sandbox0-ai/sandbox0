-- +goose Up
ALTER TABLE users
    DROP CONSTRAINT IF EXISTS fk_users_default_team;

ALTER TABLE users
    DROP COLUMN IF EXISTS default_team_id;

-- +goose Down
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS default_team_id UUID;

ALTER TABLE users
    DROP CONSTRAINT IF EXISTS fk_users_default_team;

ALTER TABLE users
    ADD CONSTRAINT fk_users_default_team
    FOREIGN KEY (default_team_id) REFERENCES teams(id) ON DELETE SET NULL;
