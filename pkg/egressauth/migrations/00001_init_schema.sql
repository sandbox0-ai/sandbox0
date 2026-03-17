CREATE TABLE IF NOT EXISTS credential_sources (
    id BIGSERIAL PRIMARY KEY,
    team_id TEXT NOT NULL,
    name TEXT NOT NULL,
    resolver_kind TEXT NOT NULL,
    current_version BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, name)
);

CREATE TABLE IF NOT EXISTS credential_source_versions (
    source_id BIGINT NOT NULL REFERENCES credential_sources(id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    spec_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, version)
);

CREATE TABLE IF NOT EXISTS sandbox_egress_credential_bindings (
    cluster_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    team_id TEXT NOT NULL DEFAULT '',
    ref TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    source_id BIGINT NOT NULL REFERENCES credential_sources(id),
    source_version BIGINT NOT NULL,
    projection JSONB NOT NULL DEFAULT '{}'::jsonb,
    cache_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id, sandbox_id, ref),
    FOREIGN KEY (source_id, source_version)
        REFERENCES credential_source_versions(source_id, version)
);

CREATE INDEX IF NOT EXISTS idx_sandbox_egress_credential_bindings_lookup
    ON sandbox_egress_credential_bindings (cluster_id, sandbox_id);

CREATE INDEX IF NOT EXISTS idx_sandbox_egress_credential_bindings_team
    ON sandbox_egress_credential_bindings (team_id);

DROP TRIGGER IF EXISTS update_credential_sources_updated_at
    ON credential_sources;
CREATE TRIGGER update_credential_sources_updated_at
    BEFORE UPDATE ON credential_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_sandbox_egress_credential_bindings_updated_at
    ON sandbox_egress_credential_bindings;
CREATE TRIGGER update_sandbox_egress_credential_bindings_updated_at
    BEFORE UPDATE ON sandbox_egress_credential_bindings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
