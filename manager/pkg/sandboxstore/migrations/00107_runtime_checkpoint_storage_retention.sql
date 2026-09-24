-- +goose Up
-- Keep immutable writer identity in checkpoint history while allowing terminal
-- source storage to be collected after its physical lease and image custody end.
ALTER TABLE manager.sandbox_runtime_checkpoints
    ADD COLUMN source_writer_grant_ref TEXT REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE RESTRICT,
    ADD COLUMN storage_released_at TIMESTAMPTZ;
UPDATE manager.sandbox_runtime_checkpoints SET source_writer_grant_ref=source_writer_grant_id;
ALTER TABLE manager.sandbox_runtime_checkpoints
    DROP CONSTRAINT sandbox_runtime_checkpoints_source_writer_grant_id_fkey,
    ADD CONSTRAINT runtime_checkpoint_storage_retention_shape CHECK (
        (source_writer_grant_ref=source_writer_grant_id AND storage_released_at IS NULL)
        OR (source_writer_grant_ref IS NULL AND storage_released_at IS NOT NULL));

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_checkpoint_storage_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=c.source_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=s.resource_lease_id
        JOIN manager.rootfs_writer_grants writer ON writer.grant_id=c.source_writer_grant_id
        WHERE c.operation_id=id
            AND s.state='terminal' AND octet_length(s.terminal_proof_digest)=32
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
            AND writer.state IN ('retired','canceled')
            AND ((l.phase='committed' AND c.evidence ? 'finalized')
                OR (l.phase='aborted' AND (NOT c.evidence ? 'capture_authorized'
                    OR c.evidence ? 'capture_failure_gc_ack')))
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs r
                WHERE r.checkpoint_id=c.operation_id)
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
                JOIN manager.sandbox_lifecycle_txns restore_l ON restore_l.txn_id=r.operation_id
                WHERE r.checkpoint_id=c.operation_id
                    AND restore_l.phase IN ('preparing','barriered','publishing','committing'))
            AND NOT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks f
                JOIN manager.sandbox_lifecycle_txns fork_l ON fork_l.txn_id=f.operation_id
                WHERE f.checkpoint_id=c.operation_id
                    AND fork_l.phase IN ('preparing','barriered','publishing','committing'))
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_storage_retention() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.storage_released_at IS NOT NULL OR
            (NEW.source_writer_grant_ref IS NOT NULL AND NEW.source_writer_grant_ref<>NEW.source_writer_grant_id) THEN
            RAISE EXCEPTION 'Checkpoint must retain its source writer' USING ERRCODE='23514';
        END IF;
        NEW.source_writer_grant_ref := NEW.source_writer_grant_id;
    ELSIF ROW(NEW.source_writer_grant_ref,NEW.storage_released_at)
        IS DISTINCT FROM ROW(OLD.source_writer_grant_ref,OLD.storage_released_at) THEN
        IF OLD.storage_released_at IS NOT NULL OR NEW.source_writer_grant_ref IS NOT NULL
            OR NEW.storage_released_at IS NULL OR NOT manager.runtime_checkpoint_storage_releasable(OLD.operation_id) THEN
            RAISE EXCEPTION 'Checkpoint storage release requires terminal custody' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_storage_retention_guard BEFORE INSERT OR UPDATE
    ON manager.sandbox_runtime_checkpoints FOR EACH ROW
    EXECUTE FUNCTION manager.guard_runtime_checkpoint_storage_retention();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Released checkpoint storage cannot reacquire a deleted writer' USING ERRCODE='55000'; END $$;
-- +goose StatementEnd
