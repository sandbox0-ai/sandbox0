-- +goose Up

ALTER TABLE manager.sandbox_runtime_claims
    ADD COLUMN credential_binding_digest TEXT NOT NULL
        DEFAULT 'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945';

ALTER TABLE manager.sandbox_runtime_claims
    ADD CONSTRAINT sandbox_runtime_claims_credential_binding_digest_check
    CHECK (credential_binding_digest ~ '^sha256:[0-9a-f]{64}$');

ALTER TABLE manager.sandbox_network_mutations
    ADD COLUMN credential_binding_digest TEXT NOT NULL
        DEFAULT 'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945';

ALTER TABLE manager.sandbox_network_mutations
    ADD CONSTRAINT sandbox_network_mutations_credential_binding_digest_check
    CHECK (credential_binding_digest ~ '^sha256:[0-9a-f]{64}$');

CREATE TABLE manager.sandbox_network_mutation_bindings (
    operation_id TEXT NOT NULL
        REFERENCES manager.sandbox_network_mutations(operation_id) ON DELETE CASCADE,
    sandbox_id TEXT NOT NULL REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    ref TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    source_id BIGINT NOT NULL REFERENCES scheduler.credential_sources(id),
    projection JSONB NOT NULL,
    cache_policy JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (operation_id, ref),
    UNIQUE (sandbox_id, ref),
    CONSTRAINT sandbox_network_mutation_bindings_identity_bounds CHECK (
        operation_id <> '' AND octet_length(operation_id) <= 512
        AND sandbox_id <> '' AND octet_length(sandbox_id) <= 512
        AND team_id <> '' AND octet_length(team_id) <= 512
        AND ref <> '' AND octet_length(ref) <= 512
        AND source_ref <> '' AND octet_length(source_ref) <= 512
    ),
    CONSTRAINT sandbox_network_mutation_bindings_payload_bounds CHECK (
        jsonb_typeof(projection) = 'object'
        AND jsonb_typeof(cache_policy) IN ('object', 'null')
        AND octet_length(projection::text) <= 65536
        AND octet_length(cache_policy::text) <= 4096
    )
);

CREATE INDEX idx_sandbox_network_mutation_bindings_source
    ON manager.sandbox_network_mutation_bindings(source_id);

-- +goose Down

DROP INDEX IF EXISTS manager.idx_sandbox_network_mutation_bindings_source;
DROP TABLE IF EXISTS manager.sandbox_network_mutation_bindings;

ALTER TABLE manager.sandbox_network_mutations
    DROP CONSTRAINT IF EXISTS sandbox_network_mutations_credential_binding_digest_check;
ALTER TABLE manager.sandbox_network_mutations
    DROP COLUMN IF EXISTS credential_binding_digest;

ALTER TABLE manager.sandbox_runtime_claims
    DROP CONSTRAINT IF EXISTS sandbox_runtime_claims_credential_binding_digest_check;
ALTER TABLE manager.sandbox_runtime_claims
    DROP COLUMN IF EXISTS credential_binding_digest;
