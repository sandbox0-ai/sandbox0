-- +goose Up
-- Preserve the exact already-applied source policy with preparation authority.
-- Legacy incomplete rows remain readable but cannot synthesize policy bytes.
ALTER TABLE manager.sandbox_runtime_migrations
    ADD COLUMN source_network_policy TEXT,
    ADD COLUMN source_network_policy_digest TEXT,
    ADD CONSTRAINT migration_source_network_policy_pair CHECK (
        (source_network_policy IS NULL AND source_network_policy_digest IS NULL) OR
        (source_network_policy IS NOT NULL AND source_network_policy_digest IS NOT NULL
            AND preparation_request IS NOT NULL
            AND octet_length(source_network_policy) BETWEEN 1 AND 65536
            AND source_network_policy_digest ~ '^sha256:[0-9a-f]{64}$'));

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_migration_source_policy() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.source_network_policy IS NOT NULL AND
        ROW(NEW.source_network_policy,NEW.source_network_policy_digest)
        IS DISTINCT FROM ROW(OLD.source_network_policy,OLD.source_network_policy_digest) THEN
        RAISE EXCEPTION 'Migration source policy is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.source_network_policy IS NULL AND NEW.source_network_policy IS NOT NULL
        AND OLD.preparation_request IS NOT NULL THEN
        RAISE EXCEPTION 'Migration policy cannot be reconstructed after preparation' USING ERRCODE = '23514';
    END IF;
    IF OLD.preparation_request IS NULL AND NEW.preparation_request IS NOT NULL
        AND NEW.source_network_policy IS NULL THEN
        RAISE EXCEPTION 'Migration preparation requires source policy custody' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_source_policy_guard BEFORE UPDATE ON manager.sandbox_runtime_migrations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_migration_source_policy();

CREATE INDEX idx_runtime_migration_pending_destination
    ON manager.sandbox_runtime_migrations (operation_id COLLATE "C")
    WHERE source_fence_proof IS NOT NULL AND restore_receipt IS NULL AND generation_committed_at IS NULL
        AND source_network_policy IS NOT NULL;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Migration source policy must retain recovery evidence' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd
