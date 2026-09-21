-- +goose Up
-- Preparation/cancellation delivery cannot follow mutable slot observations.
-- Legacy prepared commands keep NULL and are not automatically redispatched.
ALTER TABLE manager.sandbox_runtime_migrations ADD COLUMN preparation_address TEXT,
    ADD CONSTRAINT migration_preparation_address_shape CHECK (preparation_address IS NULL OR
        (octet_length(preparation_address) BETWEEN 1 AND 256 AND preparation_request IS NOT NULL));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_preparation_address() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.preparation_address IS DISTINCT FROM OLD.preparation_address
        AND (OLD.preparation_address IS NOT NULL OR OLD.preparation_request IS NOT NULL) THEN
        RAISE EXCEPTION 'Migration preparation endpoint cannot change or be backfilled' USING ERRCODE='23514';
    END IF;
    IF NEW.preparation_address IS NOT NULL AND NEW.preparation_cancel_request IS NOT NULL
        AND NEW.preparation_cancel_request->>'address' IS DISTINCT FROM NEW.preparation_address THEN
        RAISE EXCEPTION 'Migration cancellation changed original source endpoint' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_preparation_address_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_preparation_address();

CREATE INDEX idx_runtime_migration_pending_source_execution
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE assignment_request IS NOT NULL AND preparation_cancel_request IS NULL AND publication_request IS NULL
        AND staging_source_receipt IS NOT NULL AND staging_destination_receipt IS NOT NULL
        AND NOT staging_source_release_requested AND NOT staging_destination_release_requested;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Migration dispatch evidence cannot be discarded'; END $$;
-- +goose StatementEnd
