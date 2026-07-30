-- +goose Up
CREATE TABLE team_admission_states (
    team_id UUID PRIMARY KEY REFERENCES teams(id) ON DELETE CASCADE,
    version BIGINT NOT NULL CHECK (version >= 0),
    state TEXT NOT NULL CHECK (state IN ('allowed', 'restricted')),
    source TEXT NOT NULL CHECK (source <> ''),
    reason TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS team_admission_states;
