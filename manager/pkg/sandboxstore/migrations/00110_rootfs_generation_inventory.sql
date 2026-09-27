-- +goose Up
-- Parent identity is immutable provenance, not a read-time block dependency.
-- Only a checksum-verified complete mapping inventory permits its release.
ALTER TABLE manager.rootfs_generations DROP CONSTRAINT rootfs_generations_parent_generation_id_fkey;
ALTER TABLE manager.rootfs_generations ADD COLUMN inventory_complete BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE manager.rootfs_generations ADD COLUMN inventory_attempted_at TIMESTAMPTZ;
-- A real FK keeps an incomplete child safe against concurrent parent deletion.
-- The immutable parent ID survives after this temporary read dependency ends.
CREATE TABLE manager.rootfs_generation_dependencies (
    generation_id TEXT PRIMARY KEY REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE,
    parent_generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX rootfs_generation_dependencies_parent_idx ON manager.rootfs_generation_dependencies(parent_generation_id);
INSERT INTO manager.rootfs_generation_dependencies
SELECT generation_id,parent_generation_id FROM manager.rootfs_generations WHERE parent_generation_id IS NOT NULL;
-- +goose StatementBegin
CREATE FUNCTION manager.track_rootfs_generation_dependency() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' AND NEW.parent_generation_id IS NOT NULL AND NOT NEW.inventory_complete THEN
        INSERT INTO manager.rootfs_generation_dependencies VALUES (NEW.generation_id,NEW.parent_generation_id);
    ELSIF TG_OP='UPDATE' AND NEW.inventory_complete AND NOT OLD.inventory_complete THEN
        DELETE FROM manager.rootfs_generation_dependencies WHERE generation_id=NEW.generation_id;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER track_rootfs_generation_dependency AFTER INSERT OR UPDATE OF inventory_complete
ON manager.rootfs_generations FOR EACH ROW EXECUTE FUNCTION manager.track_rootfs_generation_dependency();
CREATE INDEX rootfs_generations_inventory_pending_idx ON manager.rootfs_generations((COALESCE(inventory_attempted_at,created_at)), generation_id)
WHERE NOT inventory_complete AND durability_state='s3_materialized';
CREATE TABLE manager.rootfs_generation_inventory_pages (
    generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE,
    locator_version BIGINT NOT NULL,
    page_id BIGSERIAL NOT NULL,
    locator JSONB NOT NULL,
    start_block BIGINT NOT NULL,
    block_count BIGINT NOT NULL,
    expected_level INTEGER NOT NULL,
    PRIMARY KEY (generation_id, page_id)
);
-- These indexes bound the anti-joins used by generation retirement.
CREATE INDEX rootfs_generations_retirement_order_idx ON manager.rootfs_generations(created_at,generation_id);
CREATE INDEX rootfs_rollbacks_old_generation_idx ON manager.rootfs_head_rollbacks(old_generation_id);
CREATE INDEX rootfs_rollbacks_new_generation_idx ON manager.rootfs_head_rollbacks(new_generation_id);
CREATE INDEX runtime_slots_source_generation_idx ON manager.runtime_slots(source_generation_id);
CREATE INDEX rootfs_running_forks_source_generation_idx ON manager.rootfs_running_forks(source_generation_id);
CREATE INDEX rootfs_running_forks_checkpoint_generation_idx ON manager.rootfs_running_forks(checkpoint_generation_id);
CREATE INDEX rootfs_materialization_members_generation_idx ON manager.rootfs_materialization_members(generation_id);
CREATE INDEX runtime_checkpoint_refs_generation_idx ON manager.sandbox_runtime_checkpoint_refs(generation_id);
CREATE INDEX runtime_checkpoint_forks_generation_idx ON manager.sandbox_runtime_checkpoint_forks(generation_id);
CREATE INDEX rootfs_writer_grants_live_initial_idx ON manager.rootfs_writer_grants(initial_generation_id)
WHERE state IN ('issued','consumed','retiring');
CREATE INDEX rootfs_running_captures_source_generation_idx ON manager.rootfs_running_template_captures(source_generation_id);
CREATE INDEX rootfs_running_captures_checkpoint_generation_idx ON manager.rootfs_running_template_captures(checkpoint_generation_id);
CREATE INDEX rootfs_lifecycle_live_expected_generation_idx ON manager.sandbox_lifecycle_txns(expected_generation_id)
WHERE phase NOT IN ('committed','aborted');
CREATE INDEX rootfs_lifecycle_live_prepared_generation_idx ON manager.sandbox_lifecycle_txns(prepared_generation_id)
WHERE phase NOT IN ('committed','aborted');
CREATE INDEX rootfs_lifecycle_live_target_generation_idx ON manager.sandbox_lifecycle_txns(target_generation_id)
WHERE phase NOT IN ('committed','aborted');

-- +goose Down
-- Reintroducing the old ancestry FK after collection would reference removed
-- provenance identities. Preserve history rather than inventing parents.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM manager.rootfs_generations child WHERE child.parent_generation_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM manager.rootfs_generations parent WHERE parent.generation_id=child.parent_generation_id)) THEN
        RAISE EXCEPTION 'generation inventory rollback requires retained parent identities';
    END IF;
END $$;
-- +goose StatementEnd
DROP INDEX manager.rootfs_lifecycle_live_target_generation_idx;
DROP INDEX manager.rootfs_lifecycle_live_prepared_generation_idx;
DROP INDEX manager.rootfs_lifecycle_live_expected_generation_idx;
DROP INDEX manager.rootfs_running_captures_checkpoint_generation_idx;
DROP INDEX manager.rootfs_running_captures_source_generation_idx;
DROP INDEX manager.rootfs_writer_grants_live_initial_idx;
DROP INDEX manager.rootfs_generations_retirement_order_idx;
DROP INDEX manager.rootfs_rollbacks_old_generation_idx;
DROP INDEX manager.rootfs_rollbacks_new_generation_idx;
DROP INDEX manager.runtime_slots_source_generation_idx;
DROP INDEX manager.rootfs_running_forks_source_generation_idx;
DROP INDEX manager.rootfs_running_forks_checkpoint_generation_idx;
DROP INDEX manager.rootfs_materialization_members_generation_idx;
DROP INDEX manager.runtime_checkpoint_refs_generation_idx;
DROP INDEX manager.runtime_checkpoint_forks_generation_idx;
DROP TABLE manager.rootfs_generation_inventory_pages;
DROP TRIGGER track_rootfs_generation_dependency ON manager.rootfs_generations;
DROP FUNCTION manager.track_rootfs_generation_dependency();
DROP TABLE manager.rootfs_generation_dependencies;
DROP INDEX manager.rootfs_generations_inventory_pending_idx;
ALTER TABLE manager.rootfs_generations DROP COLUMN inventory_complete;
ALTER TABLE manager.rootfs_generations DROP COLUMN inventory_attempted_at;
ALTER TABLE manager.rootfs_generations ADD CONSTRAINT rootfs_generations_parent_generation_id_fkey
FOREIGN KEY (parent_generation_id) REFERENCES manager.rootfs_generations(generation_id) ON DELETE RESTRICT;
