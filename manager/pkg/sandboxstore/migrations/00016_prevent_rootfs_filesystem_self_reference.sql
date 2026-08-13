-- +goose Up

UPDATE manager.rootfs_filesystems
SET source_filesystem_id = NULL,
    updated_at = NOW()
WHERE source_filesystem_id = filesystem_id;

-- Do not add a CHECK constraint while a region may still run a legacy manager.
-- The legacy same-sandbox restore statement can temporarily write this value,
-- so enforcing it here would make the additive schema incompatible with an old
-- data-plane cluster. Current writers already avoid self-references. Add the
-- constraint in a later migration after every legacy writer has been retired
-- and the cleanup above has been repeated.

-- +goose Down

-- The cleanup is intentionally irreversible. Restoring a corrupt
-- self-reference would recreate a cycle in the legacy filesystem graph.
