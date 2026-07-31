-- +goose Up
-- Regional gateways receive team IDs from the global identity directory, so
-- admission projection must not require a duplicate local team row.
ALTER TABLE team_admission_states
DROP CONSTRAINT IF EXISTS team_admission_states_team_id_fkey;

COMMENT ON COLUMN team_admission_states.team_id IS
    'Team identifier owned by the authoritative identity directory; it may not exist in this regional teams table.';

-- +goose StatementBegin
CREATE FUNCTION delete_local_team_admission_state()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM team_admission_states WHERE team_id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER delete_local_team_admission_state
AFTER DELETE ON teams
FOR EACH ROW EXECUTE FUNCTION delete_local_team_admission_state();

-- +goose Down
DROP TRIGGER IF EXISTS delete_local_team_admission_state ON teams;
DROP FUNCTION IF EXISTS delete_local_team_admission_state();

COMMENT ON COLUMN team_admission_states.team_id IS NULL;

ALTER TABLE team_admission_states
DROP CONSTRAINT IF EXISTS team_admission_states_team_id_fkey;

-- Keep pre-existing federated projection rows during rollback. The unvalidated
-- constraint still restores the old behavior for subsequent writes.
ALTER TABLE team_admission_states
ADD CONSTRAINT team_admission_states_team_id_fkey
FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE NOT VALID;
