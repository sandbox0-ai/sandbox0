-- +goose Up
ALTER TABLE manager.sandbox_runtime_checkpoints
    ADD COLUMN capture_upload_gc_scope_digest TEXT,
    ADD COLUMN capture_upload_gc_completed_at TIMESTAMPTZ,
    ADD COLUMN capture_upload_reservation_released_at TIMESTAMPTZ;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_migration_capture_upload() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE upload_grant JSONB; wanted BIGINT; used BIGINT; reserved BIGINT;
BEGIN
    IF OLD.staging_request IS NULL AND NEW.staging_request ? 'capture_upload' THEN
        upload_grant := NEW.staging_request->'capture_upload';
        wanted := (2*(((NEW.staging_request->>'bytes')::bigint+8388607)/8388608)+256)*8388608;
        IF jsonb_typeof(upload_grant) IS DISTINCT FROM 'object'
            OR upload_grant->>'version' IS DISTINCT FROM '1'
            OR upload_grant->>'team_id' IS DISTINCT FROM NEW.assignment_request->'target'->>'team_id'
            OR upload_grant->>'compatibility_digest' IS DISTINCT FROM
                (SELECT compatibility_digest FROM manager.runtime_slots WHERE slot_id=NEW.source_slot_id)
            OR upload_grant->>'cpu_features_digest' IS NULL OR upload_grant->>'cpu_features_digest' !~ '^sha256:[0-9a-f]{64}$'
            OR upload_grant->>'scope_digest' IS NULL OR upload_grant->>'scope_digest' !~ '^sha256:[0-9a-f]{64}$'
            OR upload_grant->>'max_bytes' IS DISTINCT FROM wanted::text
            OR wanted<8388608 OR wanted>274877906944
            OR NEW.cpu_preflight_source IS NULL OR NEW.cpu_preflight_destination IS NULL THEN
            RAISE EXCEPTION 'Capture upload requires exact bounded source staging' USING ERRCODE='23514';
        END IF;
        -- Same transaction lock as regional admission; never count node cache
        -- capacity as permission to retain unbounded regional image objects.
        PERFORM pg_advisory_xact_lock(6129648202201);
        SELECT
            (SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
                FROM manager.sandbox_runtime_migrations
                WHERE operation_id<>NEW.operation_id AND capture_upload_gc_completed_at IS NULL)
            +
            (SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
                FROM manager.sandbox_runtime_checkpoints
                WHERE capture_upload_gc_completed_at IS NULL AND capture_upload_reservation_released_at IS NULL)
            INTO used;
        reserved := manager.runtime_migration_capture_upload_reserved_bytes(NEW.staging_request);
        IF used+reserved>68719476736 THEN
            RAISE EXCEPTION 'Regional capture upload budget exhausted' USING ERRCODE='23514';
        END IF;
    END IF;
    IF OLD.capture_upload_gc_scope_digest IS NOT NULL AND NEW.capture_upload_gc_scope_digest IS DISTINCT FROM OLD.capture_upload_gc_scope_digest
        OR OLD.capture_upload_gc_completed_at IS NOT NULL AND NEW.capture_upload_gc_completed_at IS DISTINCT FROM OLD.capture_upload_gc_completed_at THEN
        RAISE EXCEPTION 'Capture upload GC authority is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.capture_upload_gc_scope_digest IS NOT NULL AND (
        NEW.capture_upload_gc_scope_digest !~ '^sha256:[0-9a-f]{64}$'
        OR NEW.capture_upload_gc_scope_digest IS DISTINCT FROM NEW.staging_request->'capture_upload'->>'scope_digest'
        OR NOT manager.runtime_migration_capture_upload_releasable(NEW.operation_id)) THEN
        RAISE EXCEPTION 'Capture upload GC lacks terminal custody' USING ERRCODE='23514';
    END IF;
    IF NEW.capture_upload_gc_completed_at IS NOT NULL AND NEW.capture_upload_gc_scope_digest IS NULL THEN
        RAISE EXCEPTION 'Capture upload GC lacks exact scope' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_checkpoint_capture_upload_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        WHERE c.operation_id=id AND c.evidence->'staging' ? 'capture_upload'
            AND (
                (l.phase='aborted' AND c.evidence ? 'staging_released'
                    AND NOT c.evidence ? 'capture_authorized')
                OR (l.phase='aborted' AND c.evidence ?& ARRAY['capture_failure_staging_released','capture_failure_gc_ack'])
                OR (c.image_gc_completed_at IS NOT NULL)
            )
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_checkpoint_capture_upload_reservation_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.runtime_slots src ON src.slot_id=c.source_slot_id
        JOIN manager.runtime_resource_leases lease ON lease.lease_id=src.resource_lease_id
        WHERE c.operation_id=id AND c.evidence->'staging' ? 'capture_upload'
            AND c.evidence ?& ARRAY['published','retained','finalized']
            AND l.phase='committed' AND src.state='terminal'
            AND octet_length(src.terminal_proof_digest)=32
            AND lease.lease_state='released' AND lease.released_at IS NOT NULL
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_capture_upload() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE upload_grant JSONB; wanted BIGINT; used BIGINT; reserved BIGINT;
BEGIN
    IF OLD.evidence->'staging' IS NOT NULL AND NEW.evidence->'staging' IS DISTINCT FROM OLD.evidence->'staging' THEN
        RAISE EXCEPTION 'Checkpoint staging grant is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.evidence->'staging' IS NULL AND NEW.evidence->'staging' ? 'capture_upload' THEN
        upload_grant := NEW.evidence->'staging'->'capture_upload';
        wanted := (2*(((NEW.evidence->'staging'->>'bytes')::bigint+8388607)/8388608)+256)*8388608;
        IF jsonb_typeof(upload_grant) IS DISTINCT FROM 'object'
            OR upload_grant->>'version' IS DISTINCT FROM '1'
            OR upload_grant->>'team_id' IS DISTINCT FROM NEW.evidence->'assignment'->>'team_id'
            OR upload_grant->>'compatibility_digest' IS DISTINCT FROM NEW.compatibility_digest
            OR upload_grant->>'cpu_features_digest' IS NULL OR upload_grant->>'cpu_features_digest' !~ '^sha256:[0-9a-f]{64}$'
            OR upload_grant->>'scope_digest' IS NULL OR upload_grant->>'scope_digest' !~ '^sha256:[0-9a-f]{64}$'
            OR upload_grant->>'max_bytes' IS DISTINCT FROM wanted::text
            OR wanted<8388608 OR wanted>274877906944
            OR NEW.evidence->'cpu' IS NULL THEN
            RAISE EXCEPTION 'Checkpoint capture upload requires exact bounded source staging' USING ERRCODE='23514';
        END IF;
        PERFORM pg_advisory_xact_lock(6129648202201);
        SELECT
            (SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
                FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL)
            +
            (SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
                FROM manager.sandbox_runtime_checkpoints
                WHERE operation_id<>NEW.operation_id AND capture_upload_gc_completed_at IS NULL
                    AND capture_upload_reservation_released_at IS NULL)
            INTO used;
        reserved := manager.runtime_migration_capture_upload_reserved_bytes(NEW.evidence->'staging');
        IF used+reserved>68719476736 THEN
            RAISE EXCEPTION 'Regional capture upload budget exhausted' USING ERRCODE='23514';
        END IF;
    END IF;
    IF OLD.capture_upload_gc_scope_digest IS NOT NULL AND NEW.capture_upload_gc_scope_digest IS DISTINCT FROM OLD.capture_upload_gc_scope_digest
        OR OLD.capture_upload_gc_completed_at IS NOT NULL AND NEW.capture_upload_gc_completed_at IS DISTINCT FROM OLD.capture_upload_gc_completed_at THEN
        RAISE EXCEPTION 'Checkpoint capture upload GC authority is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.capture_upload_gc_scope_digest IS NOT NULL AND (
        NEW.capture_upload_gc_scope_digest !~ '^sha256:[0-9a-f]{64}$'
        OR NEW.capture_upload_gc_scope_digest IS DISTINCT FROM NEW.evidence->'staging'->'capture_upload'->>'scope_digest'
        OR NOT manager.runtime_checkpoint_capture_upload_releasable(NEW.operation_id)) THEN
        RAISE EXCEPTION 'Checkpoint capture upload GC lacks terminal custody' USING ERRCODE='23514';
    END IF;
    IF NEW.capture_upload_gc_completed_at IS NOT NULL AND NEW.capture_upload_gc_scope_digest IS NULL THEN
        RAISE EXCEPTION 'Checkpoint capture upload GC lacks exact scope' USING ERRCODE='23514';
    END IF;
    IF OLD.capture_upload_reservation_released_at IS NOT NULL
        AND NEW.capture_upload_reservation_released_at IS DISTINCT FROM OLD.capture_upload_reservation_released_at THEN
        RAISE EXCEPTION 'Checkpoint capture upload reservation release is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.capture_upload_reservation_released_at IS NOT NULL
        AND NOT manager.runtime_checkpoint_capture_upload_reservation_releasable(NEW.operation_id) THEN
        RAISE EXCEPTION 'Checkpoint capture upload reservation release lacks physical completion' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_capture_upload_guard
    BEFORE UPDATE OF evidence,capture_upload_gc_scope_digest,capture_upload_gc_completed_at,capture_upload_reservation_released_at
    ON manager.sandbox_runtime_checkpoints FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_capture_upload();

CREATE INDEX idx_runtime_checkpoint_capture_upload_gc
    ON manager.sandbox_runtime_checkpoints (operation_id COLLATE "C")
    WHERE evidence->'staging' ? 'capture_upload' AND capture_upload_gc_completed_at IS NULL
        AND (image_gc_completed_at IS NOT NULL OR evidence ? 'capture_failure_gc_ack'
            OR evidence ? 'staging_released');

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Checkpoint capture upload custody cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
