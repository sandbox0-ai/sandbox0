-- +goose Up

ALTER TABLE credential_source_versions
    ADD COLUMN IF NOT EXISTS resolver_kind TEXT;

UPDATE credential_source_versions v
SET resolver_kind = s.resolver_kind
FROM credential_sources s
WHERE v.source_id = s.id
  AND (v.resolver_kind IS NULL OR v.resolver_kind = '');

ALTER TABLE credential_source_versions
    ALTER COLUMN resolver_kind SET NOT NULL;

-- +goose Down

ALTER TABLE credential_source_versions
    DROP COLUMN IF EXISTS resolver_kind;
