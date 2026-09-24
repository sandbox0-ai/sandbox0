-- +goose Up
ALTER TABLE team_admission_states
ADD COLUMN pause_required BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE team_admission_states
ADD CONSTRAINT team_admission_pause_requires_restriction
CHECK (NOT pause_required OR state = 'restricted');

CREATE INDEX team_admission_pause_required_idx
ON team_admission_states (team_id, version)
WHERE pause_required;

-- +goose Down
DROP INDEX IF EXISTS team_admission_pause_required_idx;
ALTER TABLE team_admission_states
DROP CONSTRAINT IF EXISTS team_admission_pause_requires_restriction;
ALTER TABLE team_admission_states
DROP COLUMN IF EXISTS pause_required;
