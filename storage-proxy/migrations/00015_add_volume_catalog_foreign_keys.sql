-- +goose Up

DELETE FROM sandbox_volume_mounts AS mount
WHERE NOT EXISTS (
    SELECT 1
    FROM sandbox_volumes AS volume
    WHERE volume.id = mount.volume_id
);

DELETE FROM snapshot_coordinations AS coordination
WHERE NOT EXISTS (
    SELECT 1
    FROM sandbox_volumes AS volume
    WHERE volume.id = coordination.volume_id
);

ALTER TABLE sandbox_volume_mounts
    ADD CONSTRAINT sandbox_volume_mounts_volume_id_fkey
    FOREIGN KEY (volume_id)
    REFERENCES sandbox_volumes(id)
    ON DELETE CASCADE;

ALTER TABLE snapshot_coordinations
    ADD CONSTRAINT snapshot_coordinations_volume_id_fkey
    FOREIGN KEY (volume_id)
    REFERENCES sandbox_volumes(id)
    ON DELETE CASCADE;

-- +goose Down

ALTER TABLE snapshot_coordinations
    DROP CONSTRAINT IF EXISTS snapshot_coordinations_volume_id_fkey;

ALTER TABLE sandbox_volume_mounts
    DROP CONSTRAINT IF EXISTS sandbox_volume_mounts_volume_id_fkey;
