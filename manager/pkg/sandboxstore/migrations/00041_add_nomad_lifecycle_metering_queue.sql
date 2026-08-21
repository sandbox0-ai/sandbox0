-- +goose Up

ALTER TABLE manager.sandboxes
    ADD COLUMN resource_millicpu BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN resource_memory_mib BIGINT NOT NULL DEFAULT 0;

ALTER TABLE manager.sandboxes
    ADD CONSTRAINT sandboxes_resource_millicpu_check CHECK (resource_millicpu >= 0),
    ADD CONSTRAINT sandboxes_resource_memory_mib_check CHECK (resource_memory_mib >= 0);

CREATE SEQUENCE manager.sandbox_metering_revision_seq;

CREATE TABLE manager.sandbox_metering_projection_queue (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    revision BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sandbox_metering_projection_queue_due
    ON manager.sandbox_metering_projection_queue(available_at, revision, sandbox_id);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.enqueue_nomad_sandbox_metering_projection(target_sandbox_id TEXT)
RETURNS VOID AS $$
DECLARE
    target_runtime_backend TEXT;
    next_revision BIGINT;
BEGIN
    SELECT runtime_backend
    INTO target_runtime_backend
    FROM manager.sandboxes
    WHERE sandbox_id = target_sandbox_id;

    IF target_runtime_backend IS DISTINCT FROM 'nomad' THEN
        RETURN;
    END IF;

    next_revision := nextval('manager.sandbox_metering_revision_seq');
    INSERT INTO manager.sandbox_metering_projection_queue (
        sandbox_id, revision, attempts, available_at, last_error, created_at, updated_at
    ) VALUES (
        target_sandbox_id, next_revision, 0, NOW(), '', NOW(), NOW()
    )
    ON CONFLICT (sandbox_id) DO UPDATE
    SET revision = EXCLUDED.revision,
        attempts = 0,
        available_at = NOW(),
        last_error = '',
        updated_at = NOW();

    PERFORM pg_notify('sandbox0_nomad_metering', target_sandbox_id);
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM manager.enqueue_nomad_sandbox_metering_projection(NEW.sandbox_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.enqueue_nomad_sandbox_metering_from_lifecycle()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.phase IN ('committed', 'aborted')
        AND (TG_OP = 'INSERT'
            OR OLD.phase IS DISTINCT FROM NEW.phase
            OR OLD.committed_at IS DISTINCT FROM NEW.committed_at
            OR OLD.aborted_at IS DISTINCT FROM NEW.aborted_at) THEN
        PERFORM manager.enqueue_nomad_sandbox_metering_projection(NEW.sandbox_id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_sandbox
    AFTER INSERT OR UPDATE OF
        team_id, user_id, template_id, cluster_id, runtime_backend,
        desired_state, current_pod_name, current_pod_namespace,
        runtime_generation, owner_kind, claimed_at, deleted_at,
        resource_millicpu, resource_memory_mib
    ON manager.sandboxes
    FOR EACH ROW
    EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox();

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_lifecycle
    AFTER INSERT OR UPDATE OF phase, committed_at, aborted_at
    ON manager.sandbox_lifecycle_txns
    FOR EACH ROW
    EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_lifecycle();

INSERT INTO manager.sandbox_metering_projection_queue (sandbox_id, revision)
SELECT sandbox_id, nextval('manager.sandbox_metering_revision_seq')
FROM manager.sandboxes
WHERE runtime_backend = 'nomad'
ON CONFLICT (sandbox_id) DO NOTHING;

-- +goose Down

DROP TRIGGER IF EXISTS enqueue_nomad_sandbox_metering_from_lifecycle ON manager.sandbox_lifecycle_txns;
DROP TRIGGER IF EXISTS enqueue_nomad_sandbox_metering_from_sandbox ON manager.sandboxes;
DROP FUNCTION IF EXISTS manager.enqueue_nomad_sandbox_metering_from_lifecycle();
DROP FUNCTION IF EXISTS manager.enqueue_nomad_sandbox_metering_from_sandbox();
DROP FUNCTION IF EXISTS manager.enqueue_nomad_sandbox_metering_projection(TEXT);
DROP INDEX IF EXISTS manager.idx_sandbox_metering_projection_queue_due;
DROP TABLE IF EXISTS manager.sandbox_metering_projection_queue;
DROP SEQUENCE IF EXISTS manager.sandbox_metering_revision_seq;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_resource_memory_mib_check,
    DROP CONSTRAINT IF EXISTS sandboxes_resource_millicpu_check,
    DROP COLUMN IF EXISTS resource_memory_mib,
    DROP COLUMN IF EXISTS resource_millicpu;
