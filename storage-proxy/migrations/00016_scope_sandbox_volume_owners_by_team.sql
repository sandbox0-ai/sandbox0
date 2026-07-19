-- +goose Up

ALTER TABLE sandbox_volume_owners
    ADD COLUMN team_id TEXT;

UPDATE sandbox_volume_owners AS owner
SET team_id = volume.team_id
FROM sandbox_volumes AS volume
WHERE volume.id = owner.volume_id;

ALTER TABLE sandbox_volume_owners
    ALTER COLUMN team_id SET NOT NULL;

ALTER TABLE sandbox_volumes
    ADD CONSTRAINT sandbox_volumes_id_team_id_key
    UNIQUE (id, team_id);

ALTER TABLE sandbox_volume_owners
    DROP CONSTRAINT sandbox_volume_owners_volume_id_fkey;

ALTER TABLE sandbox_volume_owners
    ADD CONSTRAINT sandbox_volume_owners_volume_team_id_fkey
    FOREIGN KEY (volume_id, team_id)
    REFERENCES sandbox_volumes(id, team_id)
    ON DELETE CASCADE;

DROP INDEX idx_sandbox_volume_owners_live_owner_purpose;
CREATE UNIQUE INDEX idx_sandbox_volume_owners_live_owner_purpose
    ON sandbox_volume_owners(team_id, owner_cluster_id, owner_sandbox_id, purpose)
    WHERE cleanup_requested_at IS NULL;

DROP INDEX idx_sandbox_volume_owners_cluster_sandbox;
CREATE INDEX idx_sandbox_volume_owners_cluster_sandbox
    ON sandbox_volume_owners(team_id, owner_cluster_id, owner_sandbox_id);

-- +goose Down

DROP INDEX idx_sandbox_volume_owners_cluster_sandbox;
CREATE INDEX idx_sandbox_volume_owners_cluster_sandbox
    ON sandbox_volume_owners(owner_cluster_id, owner_sandbox_id);

DROP INDEX idx_sandbox_volume_owners_live_owner_purpose;
CREATE UNIQUE INDEX idx_sandbox_volume_owners_live_owner_purpose
    ON sandbox_volume_owners(owner_cluster_id, owner_sandbox_id, purpose)
    WHERE cleanup_requested_at IS NULL;

ALTER TABLE sandbox_volume_owners
    DROP CONSTRAINT sandbox_volume_owners_volume_team_id_fkey;

ALTER TABLE sandbox_volume_owners
    ADD CONSTRAINT sandbox_volume_owners_volume_id_fkey
    FOREIGN KEY (volume_id)
    REFERENCES sandbox_volumes(id)
    ON DELETE CASCADE;

ALTER TABLE sandbox_volumes
    DROP CONSTRAINT sandbox_volumes_id_team_id_key;

ALTER TABLE sandbox_volume_owners
    DROP COLUMN team_id;
