-- +goose Up
ALTER TABLE IF EXISTS user_ssh_public_keys
    ADD COLUMN IF NOT EXISTS team_id TEXT;

ALTER TABLE IF EXISTS user_ssh_public_keys
    DROP CONSTRAINT IF EXISTS user_ssh_public_keys_fingerprint_sha256_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_ssh_public_keys_team_fingerprint
    ON user_ssh_public_keys(team_id, fingerprint_sha256)
    WHERE team_id IS NOT NULL AND team_id <> '';

CREATE INDEX IF NOT EXISTS idx_user_ssh_public_keys_team_user_id
    ON user_ssh_public_keys(team_id, user_id);

-- +goose Down
DROP INDEX IF EXISTS idx_user_ssh_public_keys_team_user_id;
DROP INDEX IF EXISTS idx_user_ssh_public_keys_team_fingerprint;

DELETE FROM user_ssh_public_keys a
USING user_ssh_public_keys b
WHERE a.ctid < b.ctid
  AND a.fingerprint_sha256 = b.fingerprint_sha256;

ALTER TABLE IF EXISTS user_ssh_public_keys
    DROP COLUMN IF EXISTS team_id;

ALTER TABLE IF EXISTS user_ssh_public_keys
    ADD CONSTRAINT user_ssh_public_keys_fingerprint_sha256_key UNIQUE (fingerprint_sha256);
