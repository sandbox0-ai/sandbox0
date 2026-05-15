-- +goose Up
ALTER TABLE functions
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_functions_team_id_active ON functions(team_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_functions_domain_label_active ON functions(domain_label) WHERE deleted_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_functions_domain_label_active;
DROP INDEX IF EXISTS idx_functions_team_id_active;
ALTER TABLE functions
    DROP COLUMN IF EXISTS deleted_at,
    DROP COLUMN IF EXISTS enabled;
