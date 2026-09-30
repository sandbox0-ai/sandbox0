-- +goose Up
CREATE TABLE manager.runtime_node_release_artifacts (
    cluster_id TEXT NOT NULL CHECK (octet_length(cluster_id) BETWEEN 1 AND 512),
    node_uid TEXT NOT NULL CHECK (octet_length(node_uid) BETWEEN 1 AND 512),
    source_commit TEXT NOT NULL CHECK (source_commit ~ '^[0-9a-f]{40}$'),
    bundle_sha256 TEXT NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    artifact JSONB NOT NULL CHECK (jsonb_typeof(artifact)='object' AND octet_length(artifact::text)<=65536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id,node_uid)
);
CREATE TABLE manager.runtime_release_policies (
    cluster_id TEXT PRIMARY KEY CHECK (octet_length(cluster_id) BETWEEN 1 AND 512),
    source_commit TEXT NOT NULL CHECK (source_commit ~ '^[0-9a-f]{40}$'),
    bundle_sha256 TEXT NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    operation_id TEXT NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    revision BIGINT NOT NULL CHECK (revision>0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE manager.runtime_release_operations (
    cluster_id TEXT NOT NULL,
    operation_id TEXT NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    source_commit TEXT NOT NULL CHECK (source_commit ~ '^[0-9a-f]{40}$'),
    bundle_sha256 TEXT NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    revision BIGINT NOT NULL CHECK (revision>0),
    request_digest TEXT NOT NULL CHECK(request_digest ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id,operation_id)
);

CREATE TABLE manager.runtime_release_probes (
    operation_id TEXT PRIMARY KEY CHECK(octet_length(operation_id) BETWEEN 1 AND 256),
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    source_commit TEXT NOT NULL CHECK(source_commit ~ '^[0-9a-f]{40}$'),
    bundle_sha256 TEXT NOT NULL CHECK(bundle_sha256 ~ '^[0-9a-f]{64}$'),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW()+INTERVAL '10 minutes',
    command_completed_at TIMESTAMPTZ,
    command_stdout_sha256 TEXT CHECK(command_stdout_sha256 ~ '^[0-9a-f]{64}$'),
    compatibility_digest TEXT,
    sandbox_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The database guard also fences predecessor managers during a rolling control
-- update. An application-side placement filter alone is not admission authority.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_release_lease() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lease_state='active' THEN
        PERFORM pg_advisory_xact_lock_shared(hashtextextended('sandbox0-runtime-release/' || NEW.cluster_id,0));
        IF EXISTS(SELECT 1 FROM manager.runtime_release_policies policy
            WHERE policy.cluster_id=NEW.cluster_id AND NOT EXISTS(
                SELECT 1 FROM manager.runtime_node_release_artifacts artifact
                WHERE artifact.cluster_id=NEW.cluster_id AND artifact.node_uid=NEW.node_uid
                AND artifact.source_commit=policy.source_commit AND artifact.bundle_sha256=policy.bundle_sha256))
            AND NOT EXISTS(SELECT 1 FROM manager.runtime_release_probes probe
                JOIN manager.runtime_node_release_artifacts artifact USING(cluster_id,node_uid)
                WHERE probe.operation_id=NEW.operation_id AND probe.cluster_id=NEW.cluster_id
                AND probe.node_id=NEW.node_id AND probe.node_uid=NEW.node_uid AND probe.node_boot_id=NEW.node_boot_id
                AND probe.expires_at>NOW() AND artifact.source_commit=probe.source_commit AND artifact.bundle_sha256=probe.bundle_sha256) THEN
            RAISE EXCEPTION 'runtime release does not admit new node leases' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_release_lease_admission BEFORE INSERT ON manager.runtime_resource_leases
    FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_release_lease();

-- +goose Down
DROP TRIGGER runtime_release_lease_admission ON manager.runtime_resource_leases;
DROP FUNCTION manager.guard_runtime_release_lease();
DROP TABLE manager.runtime_release_probes;
DROP TABLE manager.runtime_release_operations;
DROP TABLE manager.runtime_release_policies;
DROP TABLE manager.runtime_node_release_artifacts;
