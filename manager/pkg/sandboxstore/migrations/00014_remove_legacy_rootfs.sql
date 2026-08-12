-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM manager.sandbox_rootfs_states LIMIT 1)
        OR EXISTS (SELECT 1 FROM manager.sandbox_rootfs_heads LIMIT 1)
        OR EXISTS (SELECT 1 FROM manager.rootfs_layers LIMIT 1)
        OR EXISTS (SELECT 1 FROM manager.rootfs_objects LIMIT 1)
    THEN
        RAISE EXCEPTION
            'legacy rootfs data remains; delete legacy sandboxes or migrate them to COW v3 before applying migration 00014';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TABLE manager.sandbox_rootfs_states;
DROP TABLE manager.sandbox_rootfs_heads;

DROP INDEX manager.idx_rootfs_snapshots_head;
ALTER TABLE manager.rootfs_snapshots
    DROP COLUMN head_layer_id;

DROP INDEX manager.idx_rootfs_filesystems_head;
ALTER TABLE manager.rootfs_filesystems
    DROP COLUMN head_layer_id;

ALTER TABLE manager.sandbox_lifecycle_txns
    DROP COLUMN prepared_head_layer_id,
    DROP COLUMN expected_head_layer_id;

DROP TABLE manager.rootfs_objects;
DROP TABLE manager.rootfs_layers;

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        'migration 00014 is an irreversible rootfs format cutover; restore a database backup for rollback';
END
$$;
-- +goose StatementEnd
