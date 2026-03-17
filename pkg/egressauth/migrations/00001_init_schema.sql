CREATE TABLE IF NOT EXISTS sandbox_egress_credential_bindings (
    cluster_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    team_id TEXT NOT NULL DEFAULT '',
    bindings JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id, sandbox_id)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_egress_credential_bindings_team
    ON sandbox_egress_credential_bindings (team_id);

DROP TRIGGER IF EXISTS update_sandbox_egress_credential_bindings_updated_at
    ON sandbox_egress_credential_bindings;
CREATE TRIGGER update_sandbox_egress_credential_bindings_updated_at
    BEFORE UPDATE ON sandbox_egress_credential_bindings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
