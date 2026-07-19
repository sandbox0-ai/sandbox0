-- +goose Up

ALTER TABLE team_states
    ADD COLUMN admission_disabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN deleted_at TIMESTAMPTZ;

CREATE INDEX idx_team_states_admission_disabled
    ON team_states(team_id)
    WHERE admission_disabled OR deleted_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_team_states_admission_disabled;

ALTER TABLE team_states
    DROP COLUMN IF EXISTS deleted_at,
    DROP COLUMN IF EXISTS admission_disabled;
