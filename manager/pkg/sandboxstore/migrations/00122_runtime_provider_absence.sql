-- +goose Up
-- Independent regional evidence; never a replacement-host namespace proof.
CREATE TABLE manager.runtime_provider_absence (
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    scope JSONB NOT NULL,
    first_observation JSONB NOT NULL,
    last_observation JSONB NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    proof_digest BYTEA UNIQUE,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (cluster_id,node_id,node_uid,node_boot_id),
    CHECK (cluster_id<>'' AND node_id<>'' AND node_boot_id<>''),
    CHECK (COALESCE(scope->>'provider','')='aliyun' AND COALESCE(scope->>'account_id','')<>''
        AND COALESCE(scope->>'region_id','')<>'' AND COALESCE(scope->>'instance_id','')<>''),
    CHECK (node_uid='ecs/'||(scope->>'region_id')||'/'||(scope->>'instance_id')),
    CHECK (first_observation->'scope'=scope AND last_observation->'scope'=scope),
    CHECK (COALESCE(first_observation->>'present','')='false' AND COALESCE(last_observation->>'present','')='false'),
    CHECK (COALESCE(first_observation->>'request_id','')<>'' AND COALESCE(last_observation->>'request_id','')<>''
        AND COALESCE(first_observation->>'identity_request_id','')<>'' AND COALESCE(last_observation->>'identity_request_id','')<>''),
    CHECK (last_observed_at>=first_observed_at),
    CHECK (proof_digest IS NULL OR (octet_length(proof_digest)=32
        AND last_observed_at-first_observed_at>=INTERVAL '30 seconds'
        AND first_observation->>'request_id'<>last_observation->>'request_id'
        AND first_observation->>'identity_request_id'<>last_observation->>'identity_request_id'))
);
CREATE TABLE manager.runtime_provider_absence_retirements (
    grant_id TEXT PRIMARY KEY REFERENCES manager.rootfs_writer_grants(grant_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL,
    proof_digest BYTEA NOT NULL REFERENCES manager.runtime_provider_absence(proof_digest),
    allocation_observation_digest BYTEA NOT NULL CHECK (octet_length(allocation_observation_digest)=32),
    terminal_digest BYTEA NOT NULL CHECK (octet_length(terminal_digest)=32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_provider_absence() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.cluster_id,NEW.node_id,NEW.node_uid,NEW.node_boot_id,NEW.scope)
        IS DISTINCT FROM (OLD.cluster_id,OLD.node_id,OLD.node_uid,OLD.node_boot_id,OLD.scope) THEN
        RAISE EXCEPTION 'provider absence identity is immutable';
    END IF;
    IF OLD.proof_digest IS NOT NULL AND (
        (NEW.proof_digest,NEW.first_observation,NEW.last_observation,NEW.first_observed_at,NEW.last_observed_at)
        IS DISTINCT FROM (OLD.proof_digest,OLD.first_observation,OLD.last_observation,OLD.first_observed_at,OLD.last_observed_at)
        OR (OLD.revoked AND NOT NEW.revoked)) THEN
        RAISE EXCEPTION 'confirmed provider absence proof is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_runtime_provider_absence BEFORE UPDATE ON manager.runtime_provider_absence
FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_provider_absence();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM manager.runtime_provider_absence_retirements) THEN
        RAISE EXCEPTION 'provider retirement evidence cannot be rolled back';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE manager.runtime_provider_absence_retirements;
DROP TABLE manager.runtime_provider_absence;
DROP FUNCTION manager.guard_runtime_provider_absence();
