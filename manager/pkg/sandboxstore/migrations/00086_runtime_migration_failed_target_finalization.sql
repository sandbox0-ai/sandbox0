-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN failure_finalization_receipt JSONB,
    ADD CONSTRAINT migration_failure_finalization_shape CHECK (failure_finalization_receipt IS NULL OR
        (failure_cleanup_receipt IS NOT NULL AND jsonb_typeof(failure_finalization_receipt)='object'
            AND octet_length(failure_finalization_receipt::text)<=4096));
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_failure_finalization() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.failure_finalization_receipt IS NOT NULL AND NEW.failure_finalization_receipt IS DISTINCT FROM OLD.failure_finalization_receipt THEN
        RAISE EXCEPTION 'Failed target artifact finalization is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_failure_finalization_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_failure_finalization();
-- A final failed target image receipt permits its independent staging release.
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
        AND NOT unused AND NEW.source_finalization_receipt IS NULL THEN
        RAISE EXCEPTION 'Source staging release lacks cleanup evidence' USING ERRCODE='23514';
    END IF;
    IF NOT OLD.staging_destination_release_requested AND NEW.staging_destination_release_requested
        AND NOT unused AND NEW.adoption_receipt IS NULL AND NEW.failure_finalization_receipt IS NULL THEN
        RAISE EXCEPTION 'Destination staging release lacks cleanup evidence' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Failed target finalization evidence cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
