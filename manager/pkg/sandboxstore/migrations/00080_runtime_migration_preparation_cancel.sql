-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN preparation_cancel_request JSONB,
    ADD COLUMN preparation_cancel_digest TEXT,
    ADD COLUMN preparation_cancel_receipt JSONB,
    ADD CONSTRAINT migration_preparation_cancel_shape CHECK (
        (preparation_cancel_request IS NULL AND preparation_cancel_digest IS NULL AND preparation_cancel_receipt IS NULL) OR
        (preparation_cancel_request IS NOT NULL AND preparation_cancel_digest IS NOT NULL AND preparation_cancel_digest ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(preparation_cancel_request)='object' AND octet_length(preparation_cancel_request::text)<=1048576
            AND preparation_request IS NOT NULL AND capture_request IS NULL
            AND (preparation_cancel_receipt IS NULL OR
                (jsonb_typeof(preparation_cancel_receipt)='object' AND octet_length(preparation_cancel_receipt::text)<=4096))));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_preparation_cancel() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE phase TEXT;
BEGIN
    IF (OLD.preparation_cancel_request IS NOT NULL AND
        ROW(NEW.preparation_cancel_request,NEW.preparation_cancel_digest) IS DISTINCT FROM ROW(OLD.preparation_cancel_request,OLD.preparation_cancel_digest))
        OR (OLD.preparation_cancel_receipt IS NOT NULL AND NEW.preparation_cancel_receipt IS DISTINCT FROM OLD.preparation_cancel_receipt) THEN
        RAISE EXCEPTION 'Migration preparation cancellation is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.preparation_cancel_request IS NOT NULL THEN
        IF NEW.capture_request IS NOT NULL OR NEW.preparation_request IS NULL
            OR NEW.preparation_cancel_request->'request'->>'action' IS DISTINCT FROM 'cancel'
            OR ((NEW.preparation_cancel_request->'request') - 'action') IS DISTINCT FROM (NEW.preparation_request - 'action') THEN
            RAISE EXCEPTION 'Cancellation requires an exact preparation with no capture authority' USING ERRCODE='23514';
        END IF;
        IF OLD.preparation_cancel_request IS NULL OR (OLD.preparation_cancel_receipt IS NULL AND NEW.preparation_cancel_receipt IS NOT NULL) THEN
            SELECT l.phase INTO phase FROM manager.sandbox_lifecycle_txns l WHERE l.txn_id=NEW.operation_id;
            IF phase<>'barriered' THEN
                RAISE EXCEPTION 'Cancellation cannot replace a later migration phase' USING ERRCODE='23514';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_preparation_cancel_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_preparation_cancel();

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
        AND NOT unused AND NEW.adoption_receipt IS NULL THEN
        RAISE EXCEPTION 'Destination staging release lacks cleanup evidence' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE INDEX idx_runtime_migration_pending_cancel ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE preparation_request IS NOT NULL AND capture_request IS NULL AND preparation_cancel_receipt IS NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration cancellation must retain recovery evidence' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
