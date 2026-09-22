-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN capture_upload_gc_scope_digest TEXT,
    ADD COLUMN capture_upload_gc_completed_at TIMESTAMPTZ;

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_migration_capture_upload_reserved_bytes(request JSONB) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN request ? 'capture_upload'
        THEN (request->'capture_upload'->>'max_bytes')::bigint
            + (request->'capture_upload'->>'max_bytes')::bigint/32 + 8388608
        ELSE 0 END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.runtime_migration_capture_upload_releasable(id TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.runtime_slots src ON src.slot_id=m.source_slot_id
        LEFT JOIN manager.runtime_resource_leases sl ON sl.lease_id=src.resource_lease_id
        WHERE m.operation_id=id AND l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
            AND m.staging_request ? 'capture_upload'
            AND m.staging_source_release_receipt IS NOT NULL
            AND m.staging_destination_release_receipt IS NOT NULL
            AND (
                -- No capture was authorized: an unchanged source may still run.
                (l.phase='aborted' AND m.capture_request IS NULL
                    AND (m.preparation_request IS NULL OR m.preparation_cancel_receipt IS NOT NULL))
                OR (
                    src.state='terminal' AND octet_length(src.terminal_proof_digest)=32
                    AND sl.lease_state='released' AND sl.released_at IS NOT NULL
                    AND (
                        (l.phase='committed' AND m.generation_committed_at IS NOT NULL
                            AND m.adoption_receipt IS NOT NULL AND m.source_finalization_receipt IS NOT NULL)
                        OR (l.phase='aborted' AND m.capture_failure_completed_at IS NOT NULL
                            AND m.capture_failure_finalization_receipt IS NOT NULL
                            AND m.capture_failure_allocation_gc_receipt IS NOT NULL)
                        OR (l.phase='aborted' AND m.failure_completed_at IS NOT NULL
                            AND m.failure_finalization_receipt IS NOT NULL AND m.source_finalization_receipt IS NOT NULL)
                    )
                )
            )
    );
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_capture_upload() RETURNS TRIGGER LANGUAGE plpgsql AS $$
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
        SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
            INTO used FROM manager.sandbox_runtime_migrations
            WHERE operation_id<>NEW.operation_id AND capture_upload_gc_completed_at IS NULL;
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
CREATE TRIGGER runtime_migration_capture_upload_guard
    BEFORE UPDATE OF staging_request,capture_upload_gc_scope_digest,capture_upload_gc_completed_at
    ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_capture_upload();

CREATE INDEX idx_runtime_migration_capture_upload_gc
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE staging_request ? 'capture_upload' AND capture_upload_gc_completed_at IS NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Capture upload grants and collection custody must be retained' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
