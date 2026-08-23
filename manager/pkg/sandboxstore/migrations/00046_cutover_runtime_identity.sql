-- +goose Up

-- The final runtime cutover is intentionally fail-closed. Operators must
-- remove every Kubernetes-backed sandbox before applying this migration; the
-- migration never guesses how to translate a live Kubernetes workload.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM manager.sandboxes
        WHERE runtime_backend <> 'nomad'
          AND deleted_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Nomad-only cutover requires every sandbox runtime_backend to be nomad'
            USING ERRCODE = '55000';
    END IF;
END;
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS enqueue_nomad_sandbox_metering_from_sandbox
    ON manager.sandboxes;

ALTER TABLE manager.sandboxes
    RENAME COLUMN current_pod_namespace TO runtime_namespace;
ALTER TABLE manager.sandboxes
    RENAME COLUMN current_pod_name TO runtime_id;

ALTER INDEX IF EXISTS manager.idx_sandboxes_current_pod
    RENAME TO idx_sandboxes_runtime;

ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN from_pod_namespace TO from_runtime_namespace;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN from_pod_name TO from_runtime_id;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN to_pod_namespace TO to_runtime_namespace;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN to_pod_name TO to_runtime_id;

ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_pod_namespace TO runtime_namespace;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_pod_name TO runtime_id;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_pod_uid TO runtime_incarnation_id;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN consumer_ctld_pod_uid TO consumer_agent_uid;

ALTER TABLE manager.sandboxes
    DROP CONSTRAINT IF EXISTS sandboxes_runtime_backend_check,
    DROP COLUMN runtime_backend;

DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_hard_expiry;
DROP INDEX IF EXISTS manager.idx_sandboxes_nomad_soft_expiry;

CREATE INDEX idx_sandboxes_hard_expiry
    ON manager.sandboxes(hard_expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND desired_state IN ('active', 'paused')
        AND hard_expires_at IS NOT NULL;

CREATE INDEX idx_sandboxes_soft_expiry
    ON manager.sandboxes(expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND desired_state = 'active'
        AND expires_at IS NOT NULL;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.enqueue_nomad_sandbox_metering_projection(target_sandbox_id TEXT)
RETURNS VOID AS $$
DECLARE
    next_revision BIGINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM manager.sandboxes
        WHERE sandbox_id = target_sandbox_id
    ) THEN
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

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_sandbox
    AFTER INSERT OR UPDATE OF
        team_id, user_id, template_id, cluster_id,
        desired_state, runtime_id, runtime_namespace,
        runtime_generation, owner_kind, claimed_at, deleted_at,
        resource_millicpu, resource_memory_mib
    ON manager.sandboxes
    FOR EACH ROW
    EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox();

INSERT INTO manager.sandbox_metering_projection_queue (sandbox_id, revision)
SELECT sandbox_id, nextval('manager.sandbox_metering_revision_seq')
FROM manager.sandboxes
ON CONFLICT (sandbox_id) DO NOTHING;

-- +goose Down

DROP TRIGGER IF EXISTS enqueue_nomad_sandbox_metering_from_sandbox
    ON manager.sandboxes;

DROP INDEX IF EXISTS manager.idx_sandboxes_soft_expiry;
DROP INDEX IF EXISTS manager.idx_sandboxes_hard_expiry;

ALTER TABLE manager.sandboxes
    ADD COLUMN runtime_backend TEXT NOT NULL DEFAULT 'nomad',
    ADD CONSTRAINT sandboxes_runtime_backend_check
        CHECK (runtime_backend IN ('kubernetes', 'nomad'));

ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN consumer_agent_uid TO consumer_ctld_pod_uid;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_incarnation_id TO runtime_pod_uid;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_id TO runtime_pod_name;
ALTER TABLE manager.rootfs_writer_grants
    RENAME COLUMN runtime_namespace TO runtime_pod_namespace;

ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN to_runtime_id TO to_pod_name;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN to_runtime_namespace TO to_pod_namespace;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN from_runtime_id TO from_pod_name;
ALTER TABLE manager.sandbox_lifecycle_txns
    RENAME COLUMN from_runtime_namespace TO from_pod_namespace;

ALTER INDEX IF EXISTS manager.idx_sandboxes_runtime
    RENAME TO idx_sandboxes_current_pod;

ALTER TABLE manager.sandboxes
    RENAME COLUMN runtime_id TO current_pod_name;
ALTER TABLE manager.sandboxes
    RENAME COLUMN runtime_namespace TO current_pod_namespace;

CREATE INDEX idx_sandboxes_nomad_hard_expiry
    ON manager.sandboxes(hard_expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND runtime_backend = 'nomad'
        AND desired_state IN ('active', 'paused')
        AND hard_expires_at IS NOT NULL;

CREATE INDEX idx_sandboxes_nomad_soft_expiry
    ON manager.sandboxes(expires_at, sandbox_id)
    WHERE deleted_at IS NULL
        AND runtime_backend = 'nomad'
        AND desired_state = 'active'
        AND expires_at IS NOT NULL;

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

CREATE TRIGGER enqueue_nomad_sandbox_metering_from_sandbox
    AFTER INSERT OR UPDATE OF
        team_id, user_id, template_id, cluster_id, runtime_backend,
        desired_state, current_pod_name, current_pod_namespace,
        runtime_generation, owner_kind, claimed_at, deleted_at,
        resource_millicpu, resource_memory_mib
    ON manager.sandboxes
    FOR EACH ROW
    EXECUTE FUNCTION manager.enqueue_nomad_sandbox_metering_from_sandbox();
