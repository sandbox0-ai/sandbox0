-- +goose Up

CREATE INDEX idx_team_states_deleted_retention
    ON team_states(deleted_at, team_id)
    WHERE admission_disabled AND deleted_at IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_team_states_deleted_retention;
