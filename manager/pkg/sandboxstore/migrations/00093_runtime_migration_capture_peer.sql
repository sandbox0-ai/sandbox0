-- +goose Up
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN capture_peer_request JSONB,
    ADD COLUMN capture_peer_source_digest TEXT,
    ADD COLUMN capture_peer_destination_digest TEXT,
    ADD COLUMN capture_peer_source_receipt JSONB,
    ADD COLUMN capture_peer_destination_receipt JSONB,
    ADD COLUMN capture_peer_disabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT migration_capture_peer_authority CHECK (
        (capture_peer_request IS NULL AND capture_peer_source_digest IS NULL
            AND capture_peer_destination_digest IS NULL AND capture_peer_source_receipt IS NULL
            AND capture_peer_destination_receipt IS NULL AND NOT capture_peer_disabled) OR
        (capture_peer_request IS NOT NULL AND capture_peer_source_digest IS NOT NULL
            AND capture_peer_destination_digest IS NOT NULL
            AND jsonb_typeof(capture_peer_request)='object'
            AND octet_length(capture_peer_request::text)<=65536
            AND capture_peer_source_digest ~ '^[0-9a-f]{64}$'
            AND capture_peer_destination_digest ~ '^[0-9a-f]{64}$'
            AND capture_peer_request->'staging' IS NOT DISTINCT FROM staging_request
            AND capture_peer_request->'source' IS NOT DISTINCT FROM staging_source_receipt
            AND capture_peer_request->'destination' IS NOT DISTINCT FROM staging_destination_receipt
            AND staging_source_receipt IS NOT NULL AND staging_destination_receipt IS NOT NULL)),
    ADD CONSTRAINT migration_capture_peer_receipts CHECK (
        (capture_peer_source_receipt IS NULL OR
            (jsonb_typeof(capture_peer_source_receipt)='object'
                AND octet_length(capture_peer_source_receipt::text)<=256
                AND capture_peer_source_receipt->>'request_digest' IS NOT DISTINCT FROM capture_peer_source_digest
                AND capture_peer_destination_receipt IS NOT NULL))
        AND (capture_peer_destination_receipt IS NULL OR
            (jsonb_typeof(capture_peer_destination_receipt)='object'
                AND octet_length(capture_peer_destination_receipt::text)<=256
                AND capture_peer_destination_receipt->>'request_digest' IS NOT DISTINCT FROM capture_peer_destination_digest)));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_capture_peer() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF OLD.capture_peer_request IS NOT NULL AND
        ROW(NEW.capture_peer_request,NEW.capture_peer_source_digest,NEW.capture_peer_destination_digest)
        IS DISTINCT FROM ROW(OLD.capture_peer_request,OLD.capture_peer_source_digest,OLD.capture_peer_destination_digest)
        OR OLD.capture_peer_source_receipt IS NOT NULL AND NEW.capture_peer_source_receipt IS DISTINCT FROM OLD.capture_peer_source_receipt
        OR OLD.capture_peer_destination_receipt IS NOT NULL AND NEW.capture_peer_destination_receipt IS DISTINCT FROM OLD.capture_peer_destination_receipt
        OR OLD.capture_peer_disabled AND NOT NEW.capture_peer_disabled THEN
        RAISE EXCEPTION 'Capture peer authority and outcomes are immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.capture_peer_request IS NULL AND NEW.capture_peer_request IS NOT NULL
        OR OLD.capture_peer_source_receipt IS NULL AND NEW.capture_peer_source_receipt IS NOT NULL
        OR OLD.capture_peer_destination_receipt IS NULL AND NEW.capture_peer_destination_receipt IS NOT NULL
        OR NOT OLD.capture_peer_disabled AND NEW.capture_peer_disabled THEN
        SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF current_phase IS DISTINCT FROM 'preparing' OR NEW.preparation_request IS NOT NULL
            OR NEW.capture_request IS NOT NULL OR NEW.staging_source_release_requested
            OR NEW.staging_destination_release_requested OR OLD.capture_peer_disabled
            OR NOT (NEW.staging_request ? 'capture_upload')
            OR NEW.staging_source_receipt->'peer' IS NULL
            OR NEW.staging_destination_receipt->'peer' IS NULL THEN
            RAISE EXCEPTION 'Capture peer requires live pre-capture staging' USING ERRCODE='23514';
        END IF;
    END IF;
    IF OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL
        AND NEW.capture_peer_request IS NOT NULL AND NOT NEW.capture_peer_disabled
        AND (NEW.capture_peer_source_receipt IS NULL OR NEW.capture_peer_destination_receipt IS NULL) THEN
        RAISE EXCEPTION 'Source preparation must await the capture peer decision' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_capture_peer_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_capture_peer();

-- The staging worker also owns pending peer decisions. Keep those rows inside
-- its existing bounded, ordered work index after both pool receipts arrive.
DROP INDEX manager.idx_runtime_migration_staging_pending;
CREATE INDEX idx_runtime_migration_staging_pending ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE preparation_request IS NULL AND cpu_preflight_destination IS NOT NULL
        AND (staging_source_receipt IS NULL OR staging_destination_receipt IS NULL OR
            (capture_peer_request IS NOT NULL AND NOT capture_peer_disabled
                AND (capture_peer_source_receipt IS NULL OR capture_peer_destination_receipt IS NULL)));

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Capture peer authority and cancellation history must be retained' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
