-- +goose Up
-- Publication receipts preserve source custody. They grant neither another
-- writer nor execution, and cannot advance the sandbox's visible generation.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN publication_request JSONB,
    ADD COLUMN publication_digest TEXT,
    ADD COLUMN publication_receipt JSONB,
    ADD CONSTRAINT migration_publication_pair CHECK (
        (publication_request IS NULL AND publication_digest IS NULL) OR
        (publication_request IS NOT NULL AND publication_digest IS NOT NULL
            AND capture_request IS NOT NULL
            AND jsonb_typeof(publication_request) = 'object'
            AND octet_length(publication_request::text) <= 1048576
            AND publication_digest ~ '^[0-9a-f]{64}$')),
    ADD CONSTRAINT migration_publication_receipt CHECK (
        publication_receipt IS NULL OR
        (publication_request IS NOT NULL AND jsonb_typeof(publication_receipt) = 'object'
            AND octet_length(publication_receipt::text) <= 16384));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_publication() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_phase TEXT;
BEGIN
    IF (OLD.publication_request IS NOT NULL AND
        ROW(NEW.publication_request,NEW.publication_digest)
        IS DISTINCT FROM ROW(OLD.publication_request,OLD.publication_digest))
        OR (OLD.publication_receipt IS NOT NULL AND NEW.publication_receipt IS DISTINCT FROM OLD.publication_receipt) THEN
        RAISE EXCEPTION 'Migration publication evidence is immutable' USING ERRCODE = '23514';
    END IF;
    SELECT phase INTO current_phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
    IF ((OLD.publication_request IS NULL AND NEW.publication_request IS NOT NULL)
        OR (OLD.publication_receipt IS NULL AND NEW.publication_receipt IS NOT NULL))
        AND current_phase <> 'publishing' THEN
        RAISE EXCEPTION 'Migration publication requires its committed lifecycle phase' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_publication_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_publication();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration image publication must retain recovery evidence' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd
