-- +goose Up
-- An uncertain pre-publication source cannot use destination-stop evidence.
-- This immutable lane excludes image publication and target execution forever.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN capture_failure_request JSONB,
    ADD COLUMN capture_failure_digest TEXT,
    ADD COLUMN capture_failure_cleanup_receipt JSONB,
    ADD COLUMN capture_failure_finalization_receipt JSONB,
    ADD COLUMN capture_failure_allocation_gc_request JSONB,
    ADD COLUMN capture_failure_allocation_gc_receipt JSONB,
    ADD COLUMN capture_failure_completed_at TIMESTAMPTZ,
    ADD CONSTRAINT migration_capture_failure_shape CHECK (
        (capture_failure_request IS NULL AND capture_failure_digest IS NULL
            AND capture_failure_cleanup_receipt IS NULL AND capture_failure_finalization_receipt IS NULL
            AND capture_failure_completed_at IS NULL AND capture_failure_allocation_gc_request IS NULL AND capture_failure_allocation_gc_receipt IS NULL) OR
        (capture_failure_request IS NOT NULL AND capture_failure_digest IS NOT NULL AND capture_failure_digest ~ '^[0-9a-f]{64}$'
            AND capture_request IS NOT NULL AND publication_request IS NULL AND failure_request IS NULL
            AND jsonb_typeof(capture_failure_request)='object' AND octet_length(capture_failure_request::text)<=32768
            AND (capture_failure_request->'capture'->'request') IS NOT DISTINCT FROM capture_request
            AND (capture_failure_request->'capture'->>'request_digest') IS NOT DISTINCT FROM capture_digest
            AND (capture_failure_request->'capture'->>'state') IS NOT DISTINCT FROM 'uncertain'
            AND NOT (capture_failure_request->'capture' ? 'rootfs')
            AND (capture_failure_cleanup_receipt IS NULL OR (jsonb_typeof(capture_failure_cleanup_receipt)='object' AND octet_length(capture_failure_cleanup_receipt::text)<=32768))
            AND (capture_failure_finalization_receipt IS NULL OR (capture_failure_cleanup_receipt IS NOT NULL AND jsonb_typeof(capture_failure_finalization_receipt)='object' AND octet_length(capture_failure_finalization_receipt::text)<=4096))
            AND ((capture_failure_completed_at IS NULL AND capture_failure_allocation_gc_request IS NULL AND capture_failure_allocation_gc_receipt IS NULL) OR
                (capture_failure_completed_at IS NOT NULL AND capture_failure_finalization_receipt IS NOT NULL
                    AND capture_failure_allocation_gc_request IS NOT NULL AND capture_failure_allocation_gc_receipt IS NOT NULL
                    AND octet_length(capture_failure_allocation_gc_request::text)<=8192 AND octet_length(capture_failure_allocation_gc_receipt::text)<=4096))));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_capture_failure() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF (OLD.capture_failure_request IS NOT NULL AND ROW(NEW.capture_failure_request,NEW.capture_failure_digest)
            IS DISTINCT FROM ROW(OLD.capture_failure_request,OLD.capture_failure_digest)) OR
        (OLD.capture_failure_cleanup_receipt IS NOT NULL AND NEW.capture_failure_cleanup_receipt IS DISTINCT FROM OLD.capture_failure_cleanup_receipt) OR
        (OLD.capture_failure_finalization_receipt IS NOT NULL AND NEW.capture_failure_finalization_receipt IS DISTINCT FROM OLD.capture_failure_finalization_receipt) OR
        (OLD.capture_failure_completed_at IS NOT NULL AND ROW(NEW.capture_failure_completed_at,NEW.capture_failure_allocation_gc_request,NEW.capture_failure_allocation_gc_receipt)
            IS DISTINCT FROM ROW(OLD.capture_failure_completed_at,OLD.capture_failure_allocation_gc_request,OLD.capture_failure_allocation_gc_receipt)) THEN
        RAISE EXCEPTION 'Failed capture authority and evidence are immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.capture_failure_request IS NULL AND NEW.capture_failure_request IS NOT NULL THEN
        SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF current_phase<>'publishing' THEN
            RAISE EXCEPTION 'Failed capture requires existing pre-publication authority' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_capture_failure_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_capture_failure();

-- +goose StatementBegin
CREATE FUNCTION manager.check_runtime_migration_capture_failed_terminal() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE op TEXT;
BEGIN
    IF TG_TABLE_NAME='sandbox_runtime_migrations' THEN
        op := NEW.operation_id;
    ELSIF TG_TABLE_NAME='sandbox_lifecycle_txns' THEN
        op := NEW.txn_id;
    ELSE
        SELECT operation_id INTO op FROM manager.sandbox_runtime_migrations WHERE source_slot_id=NEW.slot_id AND capture_failure_request IS NOT NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=m.source_slot_id
        WHERE m.operation_id=op AND m.capture_failure_request IS NOT NULL
            AND (m.capture_failure_completed_at IS NOT NULL OR l.phase='aborted' OR s.state='terminal')) AND NOT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots s ON s.slot_id=m.source_slot_id JOIN manager.runtime_slots t ON t.slot_id=m.target_slot_id
        JOIN manager.runtime_resource_leases sl ON sl.lease_id=s.resource_lease_id
        JOIN manager.runtime_resource_leases tl ON tl.lease_id=m.target_resource_lease_id
        WHERE m.operation_id=op AND m.capture_failure_completed_at IS NOT NULL
            AND m.capture_failure_finalization_receipt IS NOT NULL AND m.capture_failure_allocation_gc_receipt IS NOT NULL
            AND l.phase='aborted' AND l.aborted_at IS NOT NULL
            AND s.state='terminal' AND s.terminal_reason='migration_capture_failed' AND octet_length(s.orphan_observation_digest)=32 AND octet_length(s.terminal_proof_digest)=32
            AND t.state='terminal' AND octet_length(t.terminal_proof_digest)=32
            AND t.claim_id='' AND t.claim_operation_id='' AND t.writer_grant_id IS NULL AND t.resource_lease_id IS NULL
            AND sl.lease_state='released' AND sl.released_at IS NOT NULL AND tl.lease_state='released' AND tl.released_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Failed capture terminal state requires source cleanup and unused destination release' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER runtime_migration_capture_failed_terminal_check AFTER UPDATE ON manager.sandbox_runtime_migrations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.capture_failure_request IS NOT NULL)
    EXECUTE FUNCTION manager.check_runtime_migration_capture_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_migration_capture_failed_lifecycle_check AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.kind='migrate') EXECUTE FUNCTION manager.check_runtime_migration_capture_failed_terminal();
CREATE CONSTRAINT TRIGGER runtime_migration_capture_failed_slot_check AFTER UPDATE OF state ON manager.runtime_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION manager.check_runtime_migration_capture_failed_terminal();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_migration_staging() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT; created TIMESTAMPTZ; unused BOOLEAN;
BEGIN
    IF (OLD.staging_request IS NOT NULL AND NEW.staging_request IS DISTINCT FROM OLD.staging_request)
        OR (OLD.staging_source_receipt IS NOT NULL AND NEW.staging_source_receipt IS DISTINCT FROM OLD.staging_source_receipt)
        OR (OLD.staging_destination_receipt IS NOT NULL AND NEW.staging_destination_receipt IS DISTINCT FROM OLD.staging_destination_receipt)
        OR (OLD.staging_source_release_requested AND NOT NEW.staging_source_release_requested)
        OR (OLD.staging_destination_release_requested AND NOT NEW.staging_destination_release_requested)
        OR (OLD.staging_source_release_receipt IS NOT NULL AND NEW.staging_source_release_receipt IS DISTINCT FROM OLD.staging_source_release_receipt)
        OR (OLD.staging_destination_release_receipt IS NOT NULL AND NEW.staging_destination_release_receipt IS DISTINCT FROM OLD.staging_destination_release_receipt) THEN
        RAISE EXCEPTION 'Migration staging authority is immutable' USING ERRCODE='23514';
    END IF;
    SELECT phase,created_at INTO current_phase,created FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF (OLD.staging_request IS NULL AND NEW.staging_request IS NOT NULL)
        OR (OLD.staging_source_receipt IS NULL AND NEW.staging_source_receipt IS NOT NULL)
        OR (OLD.staging_destination_receipt IS NULL AND NEW.staging_destination_receipt IS NOT NULL) THEN
        IF current_phase<>'preparing' OR NEW.preparation_request IS NOT NULL
            OR NEW.cpu_preflight_destination IS NULL OR NEW.staging_source_release_requested OR NEW.staging_destination_release_requested
            OR created + INTERVAL '2 minutes' <= clock_timestamp()
            OR NEW.cpu_preflight_requested_at IS NULL OR NEW.cpu_preflight_requested_at > clock_timestamp()
            OR NEW.cpu_preflight_requested_at + INTERVAL '2 minutes' <= clock_timestamp() THEN
            RAISE EXCEPTION 'Staging admission requires fresh unprepared migration authority' USING ERRCODE='23514';
        END IF;
        IF (NEW.staging_request->'source'->'target'->>'node_uid') COLLATE "C" < (NEW.staging_request->'destination'->>'node_uid') COLLATE "C" THEN
            IF NEW.staging_destination_receipt IS NOT NULL AND NEW.staging_source_receipt IS NULL THEN
                RAISE EXCEPTION 'Staging acquisition order changed' USING ERRCODE='23514';
            END IF;
        ELSE
            IF NEW.staging_source_receipt IS NOT NULL AND NEW.staging_destination_receipt IS NULL THEN
                RAISE EXCEPTION 'Staging acquisition order changed' USING ERRCODE='23514';
            END IF;
        END IF;
    END IF;
    IF (OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL)
        OR (OLD.capture_request IS NULL AND NEW.capture_request IS NOT NULL) THEN
        IF NEW.staging_source_receipt IS NULL OR NEW.staging_destination_receipt IS NULL
            OR NEW.staging_source_release_requested OR NEW.staging_destination_release_requested THEN
            RAISE EXCEPTION 'Migration execution requires both staging reservations' USING ERRCODE='23514';
        END IF;
    END IF;
    unused := current_phase='aborted' AND NEW.capture_request IS NULL
        AND (NEW.preparation_request IS NULL OR NEW.preparation_cancel_receipt IS NOT NULL);
    IF NOT OLD.staging_source_release_requested AND NEW.staging_source_release_requested
        AND NOT unused AND NEW.source_finalization_receipt IS NULL AND NEW.capture_failure_finalization_receipt IS NULL THEN
        RAISE EXCEPTION 'Source staging release lacks cleanup evidence' USING ERRCODE='23514';
    END IF;
    IF NOT OLD.staging_destination_release_requested AND NEW.staging_destination_release_requested
        AND NOT unused AND NEW.adoption_receipt IS NULL AND NEW.failure_finalization_receipt IS NULL AND NEW.capture_failure_request IS NULL THEN
        RAISE EXCEPTION 'Destination staging release lacks cleanup evidence' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.runtime_migration_storage_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots source ON source.slot_id=m.source_slot_id
        JOIN manager.runtime_slots target ON target.slot_id=m.target_slot_id
        JOIN manager.runtime_resource_leases sl ON sl.lease_id=source.resource_lease_id
        JOIN manager.runtime_resource_leases tl ON tl.lease_id=m.target_resource_lease_id
        JOIN manager.rootfs_writer_grants writer ON writer.grant_id=m.source_writer_grant_id
        WHERE m.operation_id=id
            AND source.state='terminal' AND octet_length(source.terminal_proof_digest)=32
            AND target.state='terminal' AND octet_length(target.terminal_proof_digest)=32
            AND sl.lease_state='released' AND sl.released_at IS NOT NULL
            AND tl.lease_state='released' AND tl.released_at IS NOT NULL
            AND writer.state IN ('retired','canceled')
            AND ((l.phase='aborted' AND m.capture_request IS NULL
                    AND (m.preparation_request IS NULL OR m.preparation_cancel_receipt IS NOT NULL))
                OR (l.phase='aborted' AND m.capture_failure_completed_at IS NOT NULL AND m.capture_failure_finalization_receipt IS NOT NULL)
                OR (l.phase='aborted' AND m.failure_completed_at IS NOT NULL AND m.failure_finalization_receipt IS NOT NULL)
                OR (l.phase='committed' AND m.generation_committed_at IS NOT NULL
                    AND m.source_finalization_receipt IS NOT NULL))
            AND (m.staging_request IS NULL OR
                (m.staging_source_release_receipt IS NOT NULL AND m.staging_destination_release_receipt IS NOT NULL))
    );
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Failed capture authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
