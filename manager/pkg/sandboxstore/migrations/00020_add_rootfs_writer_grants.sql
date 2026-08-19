-- +goose Up

ALTER TABLE manager.rootfs_filesystems
    ADD COLUMN IF NOT EXISTS writer_epoch BIGINT NOT NULL DEFAULT 0
        CHECK (writer_epoch >= 0);

CREATE TABLE IF NOT EXISTS manager.rootfs_writer_grants (
    grant_id TEXT PRIMARY KEY,
    filesystem_id TEXT NOT NULL
        REFERENCES manager.rootfs_filesystems(filesystem_id) ON DELETE CASCADE,
    sandbox_id TEXT NOT NULL
        REFERENCES manager.sandboxes(sandbox_id) ON DELETE RESTRICT,
    claim_id TEXT NOT NULL,
    slot_id TEXT NOT NULL,
    issue_operation_id TEXT NOT NULL UNIQUE,
    writer_epoch BIGINT NOT NULL CHECK (writer_epoch > 0),
    state TEXT NOT NULL CHECK (state IN (
        'issued', 'consumed', 'retiring', 'retired', 'canceled'
    )),
    initial_head_layer_id TEXT NOT NULL DEFAULT '',
    binding_version INTEGER NOT NULL CHECK (binding_version > 0),
    binding_digest BYTEA NOT NULL CHECK (octet_length(binding_digest) = 32),
    token_digest BYTEA NOT NULL CHECK (octet_length(token_digest) = 32),
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    consumer_node_uid TEXT NOT NULL DEFAULT '',
    -- First successful consumer only; ownership is node/binding/token/epoch.
    consumer_ctld_pod_uid TEXT NOT NULL DEFAULT '',
    consume_expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    retire_operation_id TEXT NOT NULL DEFAULT '',
    retire_kind TEXT NOT NULL DEFAULT '' CHECK (retire_kind IN (
        '', 'planned_publish', 'prelaunch_abort'
    )),
    retire_proof_digest BYTEA,
    retire_started_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (filesystem_id, writer_epoch)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rootfs_writer_grants_live_filesystem
    ON manager.rootfs_writer_grants(filesystem_id)
    WHERE state IN ('issued', 'consumed', 'retiring');

CREATE UNIQUE INDEX IF NOT EXISTS idx_rootfs_writer_grants_live_claim
    ON manager.rootfs_writer_grants(claim_id)
    WHERE state IN ('issued', 'consumed', 'retiring');

CREATE UNIQUE INDEX IF NOT EXISTS idx_rootfs_writer_grants_live_slot
    ON manager.rootfs_writer_grants(node_uid, node_boot_id, slot_id)
    WHERE state IN ('issued', 'consumed', 'retiring');

CREATE UNIQUE INDEX IF NOT EXISTS idx_rootfs_writer_grants_retire_operation
    ON manager.rootfs_writer_grants(retire_operation_id)
    WHERE retire_operation_id <> '';

CREATE INDEX IF NOT EXISTS idx_rootfs_writer_grants_lease_expiry
    ON manager.rootfs_writer_grants(state, lease_expires_at)
    WHERE state IN ('consumed', 'retiring');

-- Terminal grant history may be removed with an unreferenced filesystem, but
-- deleting a filesystem must never revoke a live writer as a side effect.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.prevent_live_rootfs_writer_delete()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM manager.rootfs_writer_grants
        WHERE filesystem_id = OLD.filesystem_id
          AND state IN ('issued', 'consumed', 'retiring')
    ) THEN
        RAISE EXCEPTION 'rootfs filesystem % has a live writer grant', OLD.filesystem_id
            USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS prevent_live_rootfs_writer_delete
    ON manager.rootfs_filesystems;
CREATE TRIGGER prevent_live_rootfs_writer_delete
    BEFORE DELETE ON manager.rootfs_filesystems
    FOR EACH ROW
    EXECUTE FUNCTION manager.prevent_live_rootfs_writer_delete();

-- +goose Down

DROP TRIGGER IF EXISTS prevent_live_rootfs_writer_delete
    ON manager.rootfs_filesystems;
DROP FUNCTION IF EXISTS manager.prevent_live_rootfs_writer_delete();
DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_lease_expiry;
DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_retire_operation;
DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_live_slot;
DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_live_claim;
DROP INDEX IF EXISTS manager.idx_rootfs_writer_grants_live_filesystem;
DROP TABLE IF EXISTS manager.rootfs_writer_grants;
ALTER TABLE manager.rootfs_filesystems
    DROP COLUMN IF EXISTS writer_epoch;
