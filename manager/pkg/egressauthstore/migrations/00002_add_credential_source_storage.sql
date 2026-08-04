-- +goose Up

ALTER TABLE credential_source_versions
    ADD COLUMN IF NOT EXISTS storage_kind TEXT NOT NULL DEFAULT 'plaintext_pg';

ALTER TABLE credential_source_versions
    ADD COLUMN IF NOT EXISTS storage_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE credential_source_versions
SET storage_kind = 'plaintext_pg'
WHERE storage_kind = '';

CREATE INDEX IF NOT EXISTS idx_credential_source_versions_storage_kind
    ON credential_source_versions (storage_kind);

-- +goose Down

DROP INDEX IF EXISTS idx_credential_source_versions_storage_kind;

ALTER TABLE credential_source_versions
    DROP COLUMN IF EXISTS storage_payload;

ALTER TABLE credential_source_versions
    DROP COLUMN IF EXISTS storage_kind;
