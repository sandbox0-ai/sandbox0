-- +goose Up
-- Node builders reserve each object before PUT. Publication transfers this
-- custody into generation references atomically, including memory cuts.
CREATE TABLE manager.rootfs_node_uploads (
    grant_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    rebase_operation_id TEXT,
    state TEXT NOT NULL DEFAULT 'uploading' CHECK (state IN ('uploading','published','abandoned')),
    generation_id TEXT,
    terminal_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (grant_id,operation_id)
);
CREATE INDEX rootfs_node_uploads_retirement_idx ON manager.rootfs_node_uploads(terminal_at,updated_at,grant_id,operation_id);
CREATE INDEX rootfs_node_uploads_replay_gc_idx ON manager.rootfs_node_uploads((GREATEST(updated_at,terminal_at)),grant_id,operation_id)
WHERE state IN ('published','abandoned') AND terminal_at IS NOT NULL;
CREATE TABLE manager.rootfs_node_upload_objects (
    grant_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    object_key TEXT NOT NULL REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    uploaded BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY(grant_id,operation_id,object_key),
    FOREIGN KEY(grant_id,operation_id) REFERENCES manager.rootfs_node_uploads(grant_id,operation_id) ON DELETE CASCADE
);
CREATE INDEX rootfs_node_upload_objects_key_idx ON manager.rootfs_node_upload_objects(object_key);
-- Keep terminal proof even after writer rows are compacted. A timestamp alone
-- never terminates a live grant; only its existing physical terminal CAS does.
-- +goose StatementBegin
CREATE FUNCTION manager.close_rootfs_node_upload_owner() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('retired','canceled') AND OLD.state NOT IN ('retired','canceled') THEN
        UPDATE manager.rootfs_node_uploads SET terminal_at=clock_timestamp()
        WHERE grant_id=NEW.grant_id AND terminal_at IS NULL;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER close_rootfs_node_upload_owner AFTER UPDATE OF state ON manager.rootfs_writer_grants
FOR EACH ROW EXECUTE FUNCTION manager.close_rootfs_node_upload_owner();

CREATE INDEX rootfs_node_uploads_rebase_idx ON manager.rootfs_node_uploads(rebase_operation_id) WHERE rebase_operation_id IS NOT NULL;
-- Offline rebase has no live sandbox writer. Its exact node cleanup ack is
-- the existing physical terminal authority, including a rejected rebase.
-- +goose StatementBegin
CREATE FUNCTION manager.close_rootfs_rebase_upload_owner() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind='rebase' AND NEW.phase IN ('committed','aborted')
        AND NEW.worker_acknowledged_at IS NOT NULL AND octet_length(NEW.worker_proof_digest)=32 THEN
        UPDATE manager.rootfs_node_uploads SET terminal_at=COALESCE(terminal_at,clock_timestamp())
        WHERE rebase_operation_id=NEW.txn_id;
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER close_rootfs_rebase_upload_owner AFTER UPDATE OF worker_acknowledged_at,phase ON manager.sandbox_lifecycle_txns
FOR EACH ROW EXECUTE FUNCTION manager.close_rootfs_rebase_upload_owner();

-- Full-object verification advances independently of a mapping page. Custody
-- prevents an interruption between verification and reference transfer from
-- orphaning an adopted object; each transfer remains bounded by one page.
CREATE TABLE manager.rootfs_inventory_object_custody (
    generation_id TEXT NOT NULL REFERENCES manager.rootfs_generations(generation_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
    PRIMARY KEY(generation_id,object_key)
);
CREATE INDEX rootfs_inventory_object_custody_key_idx ON manager.rootfs_inventory_object_custody(object_key);

-- Legacy direct node uploads were never registered in the regional catalog.
-- Preserve them until bounded inventory authenticates all reachable objects.
ALTER TABLE manager.rootfs_generations ADD COLUMN storage_inventory_required BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE manager.rootfs_generations generation SET storage_inventory_required=TRUE
WHERE durability_state='s3_materialized'
AND NOT EXISTS(SELECT 1 FROM manager.rootfs_base_artifacts base WHERE base.artifact_digest=generation.base_artifact_digest AND base.descriptor=generation.descriptor)
AND NOT EXISTS(SELECT 1 FROM manager.rootfs_generation_materialization_objects reference WHERE reference.generation_id=generation.generation_id);
-- +goose StatementBegin
CREATE FUNCTION manager.require_rootfs_node_inventory() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.storage_inventory_required := NEW.durability_state='s3_materialized'
        AND NOT EXISTS(SELECT 1 FROM manager.rootfs_base_artifacts base WHERE base.artifact_digest=NEW.base_artifact_digest AND base.descriptor=NEW.descriptor);
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER require_rootfs_node_inventory BEFORE INSERT ON manager.rootfs_generations
FOR EACH ROW EXECUTE FUNCTION manager.require_rootfs_node_inventory();

-- +goose Down
-- Pending upload protection cannot be removed underneath a live node.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='node upload custody requires a compatible forward recovery';
END $$;
-- +goose StatementEnd
