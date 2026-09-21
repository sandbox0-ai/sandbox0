-- +goose Up
-- A durably published and fenced source may be reclaimed after either target
-- adoption or an irreversible target execution stop. Failed target storage and
-- capacity remain separately owned; source cleanup must not complete migration.
ALTER TABLE manager.sandbox_runtime_migrations
    DROP CONSTRAINT migration_source_finalization_pair,
    ADD CONSTRAINT migration_source_finalization_pair CHECK (
        (source_finalization_request IS NULL AND source_finalization_digest IS NULL) OR
        (source_finalization_request IS NOT NULL AND source_finalization_digest IS NOT NULL
            AND source_finalization_digest ~ '^[0-9a-f]{64}$'
            AND source_fence_proof IS NOT NULL
            AND ((adoption_receipt IS NOT NULL AND generation_committed_at IS NOT NULL AND failure_request IS NULL)
                OR (failure_request IS NOT NULL AND failure_stop_receipt IS NOT NULL AND generation_committed_at IS NULL AND adoption_request IS NULL))
            AND octet_length(source_finalization_request::text)<=4194304));

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Failed source cleanup authority cannot be discarded' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
