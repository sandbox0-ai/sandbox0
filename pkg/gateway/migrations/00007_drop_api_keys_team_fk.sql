-- +goose Up
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_team_id_fkey;

-- +goose Down
ALTER TABLE api_keys
    ADD CONSTRAINT api_keys_team_id_fkey
    FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE;
