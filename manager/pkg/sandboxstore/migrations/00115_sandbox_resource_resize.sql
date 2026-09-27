-- +goose Up
-- A resize replaces compute through the existing filesystem pause/resume
-- protocol. The old lease is never edited or released before physical cleanup.
CREATE TABLE manager.sandbox_resource_resizes (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 512),
    from_generation BIGINT NOT NULL CHECK (from_generation >= 0),
    was_active BOOLEAN NOT NULL,
    memory TEXT NOT NULL CHECK (octet_length(memory) BETWEEN 1 AND 128),
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_bytes BIGINT NOT NULL CHECK (memory_bytes > 0),
    phase TEXT NOT NULL CHECK (phase IN ('pausing', 'resuming', 'applied', 'canceled')),
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX sandbox_resource_resizes_pending ON manager.sandbox_resource_resizes(updated_at, sandbox_id)
    WHERE phase IN ('pausing', 'resuming');

-- Lifecycle admission shares the sandbox row lock with resize admission.
-- Only the resize's filesystem pause and cold resume may acquire that owner.
-- Deletion and crash cleanup still take precedence over resize reconciliation.
-- +goose StatementBegin
CREATE FUNCTION manager.guard_sandbox_resource_resize_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r manager.sandbox_resource_resizes;
BEGIN
    SELECT * INTO r FROM manager.sandbox_resource_resizes WHERE sandbox_id=NEW.sandbox_id
        AND phase IN ('pausing', 'resuming');
    IF FOUND AND NOT (
        NEW.source='resource_resize' AND NEW.from_generation=r.from_generation AND
        ((NEW.kind='pause' AND r.phase='pausing') OR (NEW.kind='resume' AND r.phase='resuming'))
    ) AND NEW.source NOT IN ('crash', 'health', 'lost') THEN
        RAISE EXCEPTION 'Sandbox resource resize owns lifecycle' USING ERRCODE='23514';
    END IF;
    IF NEW.target_sandbox_id<>'' AND EXISTS (SELECT 1 FROM manager.sandbox_resource_resizes
        WHERE sandbox_id=NEW.target_sandbox_id AND phase IN ('pausing','resuming')) THEN
        RAISE EXCEPTION 'Resource resize owns target sandbox' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER guard_sandbox_resource_resize_lifecycle BEFORE INSERT ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.guard_sandbox_resource_resize_lifecycle();

-- +goose StatementBegin
CREATE FUNCTION manager.guard_sandbox_resource_resize_network() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.phase='pending' AND EXISTS (SELECT 1 FROM manager.sandbox_resource_resizes
        WHERE sandbox_id=NEW.sandbox_id AND phase IN ('pausing', 'resuming')) THEN
        RAISE EXCEPTION 'Sandbox resource resize owns runtime mutation' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER guard_sandbox_resource_resize_network BEFORE INSERT OR UPDATE ON manager.sandbox_network_mutations
    FOR EACH ROW EXECUTE FUNCTION manager.guard_sandbox_resource_resize_network();

-- Preserve checkpoint custody except for the exact cold-resize discard.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_ref() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        -- An explicit process-restarting resize may drop only this paused
        -- owner's image after exact physical cleanup and config publication.
        IF EXISTS (
            SELECT 1 FROM manager.sandbox_resource_resizes r
            JOIN manager.sandboxes s ON s.sandbox_id=r.sandbox_id
            WHERE r.sandbox_id=OLD.sandbox_id AND r.phase='pausing'
                AND r.from_generation=OLD.runtime_generation AND s.runtime_generation=r.from_generation
                AND s.desired_state='paused' AND s.runtime_id='' AND s.runtime_namespace=''
                AND s.config->'resources'->>'memory'=r.memory
                AND NOT EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns l WHERE l.sandbox_id=s.sandbox_id
                    AND l.phase IN ('preparing','barriered','publishing','committing'))
                AND NOT EXISTS (SELECT 1 FROM manager.runtime_slots t
                    LEFT JOIN manager.runtime_resource_leases lease ON lease.lease_id=t.resource_lease_id
                    WHERE t.sandbox_id=s.sandbox_id AND (t.state<>'terminal' OR lease.lease_state='active'))
        ) THEN RETURN OLD; END IF;
        IF NOT EXISTS (SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=OLD.sandbox_id
            AND (s.desired_state='deleted' OR
                (s.desired_state='active' AND s.runtime_id<>'' AND s.runtime_generation>OLD.runtime_generation
                    AND NOT EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns l WHERE l.sandbox_id=s.sandbox_id
                        AND l.phase IN ('preparing','barriered','publishing','committing'))))) THEN
            RAISE EXCEPTION 'Checkpoint ownership requires completed restore or owner deletion' USING ERRCODE='23514';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.sandbox_id,NEW.checkpoint_id,NEW.generation_id,NEW.created_at)
        IS NOT DISTINCT FROM ROW(OLD.sandbox_id,OLD.checkpoint_id,OLD.generation_id,OLD.created_at)
        AND NEW.runtime_generation=OLD.runtime_generation+1 AND EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
            JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
            JOIN manager.runtime_slots t ON t.slot_id=r.evidence->'image'->'target'->>'slot_id'
            JOIN manager.runtime_resource_leases lease ON lease.lease_id=t.resource_lease_id
            WHERE r.checkpoint_id=NEW.checkpoint_id AND l.sandbox_id=NEW.sandbox_id
                AND l.phase='preparing' AND l.from_generation=OLD.runtime_generation AND l.to_generation=NEW.runtime_generation
                AND l.expected_generation_id=NEW.generation_id AND r.evidence ? 'failure_gc_ack'
                AND s.runtime_generation=NEW.runtime_generation AND s.runtime_id='' AND s.runtime_namespace=''
                AND s.desired_state IN ('paused','terminating') AND s.lifecycle_epoch=l.epoch
                AND t.state='terminal' AND t.terminal_reason='migration_failed' AND lease.lease_state='released'
        ) THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' THEN
        RAISE EXCEPTION 'Checkpoint ownership is immutable' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoints c
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
        WHERE c.operation_id=NEW.checkpoint_id AND l.sandbox_id=NEW.sandbox_id
            AND l.phase='publishing' AND c.evidence ? 'publication'
            AND l.prepared_generation_id=NEW.generation_id
            AND c.evidence->'publication'->'capture'->'rootfs'->'generation'->>'generation_id'=NEW.generation_id
            AND s.desired_state='active' AND s.runtime_generation=l.from_generation
            AND NEW.runtime_generation=s.runtime_generation AND s.lifecycle_epoch=l.epoch
    ) THEN RETURN NEW; END IF;
    IF EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_checkpoint_forks fork
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=fork.operation_id
        JOIN manager.sandbox_runtime_checkpoint_refs parent ON parent.sandbox_id=l.sandbox_id
        WHERE fork.target_sandbox_id=NEW.sandbox_id AND fork.checkpoint_id=NEW.checkpoint_id
            AND fork.generation_id=NEW.generation_id AND NEW.runtime_generation=0
            AND l.phase='publishing' AND l.kind='fork' AND l.target_sandbox_id=NEW.sandbox_id
            AND parent.checkpoint_id=NEW.checkpoint_id AND parent.generation_id=NEW.generation_id
            AND parent.runtime_generation=l.from_generation
    ) THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Checkpoint reference requires exact capture or fork custody' USING ERRCODE='23514';
END;
$$;
-- +goose StatementEnd

-- Remember initial and per-transition compute sizes so delayed metering can
-- reconstruct several resizes instead of charging old windows at today's size.
CREATE TABLE manager.sandbox_metering_initial_resources (
    sandbox_id TEXT PRIMARY KEY REFERENCES manager.sandboxes(sandbox_id) ON DELETE CASCADE,
    cpu_millicores BIGINT NOT NULL CHECK (cpu_millicores > 0),
    memory_mib BIGINT NOT NULL CHECK (memory_mib > 0)
);
INSERT INTO manager.sandbox_metering_initial_resources
    SELECT sandbox_id, resource_millicpu, resource_memory_mib FROM manager.sandboxes;
ALTER TABLE manager.sandbox_lifecycle_txns
    ADD COLUMN resource_millicpu BIGINT NOT NULL DEFAULT 0 CHECK (resource_millicpu >= 0),
    ADD COLUMN resource_memory_mib BIGINT NOT NULL DEFAULT 0 CHECK (resource_memory_mib >= 0);
UPDATE manager.sandbox_lifecycle_txns l SET resource_millicpu=s.resource_millicpu,
    resource_memory_mib=s.resource_memory_mib FROM manager.sandboxes s
    WHERE s.sandbox_id=l.sandbox_id AND l.phase='committed' AND l.kind IN ('pause', 'resume');
-- +goose StatementBegin
CREATE FUNCTION manager.retain_initial_sandbox_metering_resources() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO manager.sandbox_metering_initial_resources VALUES
        (NEW.sandbox_id, NEW.resource_millicpu, NEW.resource_memory_mib) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER retain_initial_sandbox_metering_resources AFTER INSERT ON manager.sandboxes
    FOR EACH ROW EXECUTE FUNCTION manager.retain_initial_sandbox_metering_resources();
-- Initial claim admission can precede its exact physical lease. Capture that
-- lease at the first ready boundary, before any later resume can change it.
-- +goose StatementBegin
CREATE FUNCTION manager.retain_completed_claim_metering_resources() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.phase='ready' AND (TG_OP='INSERT' OR OLD.phase IS DISTINCT FROM 'ready') THEN
        UPDATE manager.sandbox_metering_initial_resources i SET
            cpu_millicores=s.resource_millicpu, memory_mib=s.resource_memory_mib
            FROM manager.sandboxes s WHERE s.sandbox_id=NEW.sandbox_id AND i.sandbox_id=s.sandbox_id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER retain_completed_claim_metering_resources AFTER INSERT OR UPDATE OF phase ON manager.sandbox_runtime_claims
    FOR EACH ROW EXECUTE FUNCTION manager.retain_completed_claim_metering_resources();
-- +goose StatementBegin
CREATE FUNCTION manager.retain_lifecycle_metering_resources() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND OLD.phase='committed' THEN
        IF NEW.resource_millicpu<>OLD.resource_millicpu OR NEW.resource_memory_mib<>OLD.resource_memory_mib THEN
            RAISE EXCEPTION 'Committed lifecycle resource snapshot is immutable' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.phase='committed' AND NEW.kind IN ('pause', 'resume') THEN
        SELECT resource_millicpu, resource_memory_mib INTO NEW.resource_millicpu, NEW.resource_memory_mib
            FROM manager.sandboxes WHERE sandbox_id=NEW.sandbox_id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER retain_lifecycle_metering_resources BEFORE INSERT OR UPDATE ON manager.sandbox_lifecycle_txns
    FOR EACH ROW EXECUTE FUNCTION manager.retain_lifecycle_metering_resources();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Resource resize and metering history require explicit reconciliation before rollback' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
