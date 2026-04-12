-- +goose Up
ALTER TABLE IF EXISTS user_ssh_public_keys
    DROP CONSTRAINT IF EXISTS user_ssh_public_keys_user_id_fkey;

-- +goose Down
DELETE FROM user_ssh_public_keys
WHERE NOT EXISTS (
    SELECT 1 FROM users WHERE users.id = user_ssh_public_keys.user_id
);

ALTER TABLE IF EXISTS user_ssh_public_keys
    ADD CONSTRAINT user_ssh_public_keys_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
