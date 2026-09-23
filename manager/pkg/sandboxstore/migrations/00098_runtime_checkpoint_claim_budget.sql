-- +goose Up
-- The general claim path keeps its one-minute limit. A larger, nonrenewable
-- budget requires an already-admitted memory restore on the exact owner.
ALTER TABLE manager.runtime_slots DROP CONSTRAINT runtime_slots_claim_binding;
ALTER TABLE manager.runtime_slots ADD CONSTRAINT runtime_slots_claim_binding CHECK ((((state = ANY (ARRAY['registered'::text, 'fastpath_ready'::text])) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 360000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NOT NULL) AND (source_generation_id IS NOT NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 360000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL))));
ALTER TABLE manager.runtime_slots DROP CONSTRAINT runtime_slots_claim_ttl_milliseconds_check;
ALTER TABLE manager.runtime_slots ADD CONSTRAINT runtime_slots_claim_ttl_milliseconds_check
    CHECK (claim_ttl_milliseconds BETWEEN 0 AND 360000);

-- +goose StatementBegin
CREATE FUNCTION manager.guard_runtime_checkpoint_claim_budget() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND OLD.claim_ttl_milliseconds>60000 THEN
        IF ROW(NEW.claim_operation_id,NEW.claim_id,NEW.claimed_at,NEW.claim_ttl_milliseconds,NEW.claim_lease_expires_at)
            IS DISTINCT FROM ROW(OLD.claim_operation_id,OLD.claim_id,OLD.claimed_at,OLD.claim_ttl_milliseconds,OLD.claim_lease_expires_at) THEN
            RAISE EXCEPTION 'Memory restore claim deadline is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.claim_ttl_milliseconds<=60000 THEN RETURN NEW; END IF;
    IF TG_OP<>'UPDATE' THEN
        RAISE EXCEPTION 'Memory restore budget requires a ready carrier' USING ERRCODE='23514';
    END IF;
    IF OLD.state<>'fastpath_ready' OR OLD.claim_operation_id<>'' OR NEW.state<>'claiming'
        OR NEW.claim_lease_expires_at IS DISTINCT FROM NEW.claimed_at+NEW.claim_ttl_milliseconds*INTERVAL '1 millisecond'
        OR NOT EXISTS (
            SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores c
            JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
            WHERE c.operation_id=NEW.claim_operation_id AND c.compatibility_digest=NEW.compatibility_digest
                AND l.kind='resume' AND l.source='manual' AND NOT l.cancelable
                AND l.phase IN ('preparing','barriered','publishing','committing')
                AND l.sandbox_id=NEW.sandbox_id AND l.expected_generation_id=NEW.source_generation_id
                AND l.epoch=s.lifecycle_epoch AND s.deleted_at IS NULL AND s.desired_state='paused'
                AND l.from_generation=s.runtime_generation AND l.to_generation=l.from_generation+1
                AND (s.hard_expires_at IS NULL OR s.hard_expires_at>clock_timestamp())
                AND c.authority->'assignment'->'target'->>'sandbox_id'=s.sandbox_id
                AND c.authority->'assignment'->'target'->>'team_id'=s.team_id
        ) THEN
        RAISE EXCEPTION 'Extended claim budget requires exact memory restore authority' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_checkpoint_claim_budget_guard
    BEFORE INSERT OR UPDATE OF claim_operation_id,claim_id,claimed_at,claim_ttl_milliseconds,claim_lease_expires_at
    ON manager.runtime_slots FOR EACH ROW EXECUTE FUNCTION manager.guard_runtime_checkpoint_claim_budget();

-- +goose Down
-- Live/historical extended claims deliberately prevent an unsafe downgrade.
ALTER TABLE manager.runtime_slots DROP CONSTRAINT runtime_slots_claim_binding;
ALTER TABLE manager.runtime_slots ADD CONSTRAINT runtime_slots_claim_binding CHECK ((((state = ANY (ARRAY['registered'::text, 'fastpath_ready'::text])) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id = ''::text) AND (claim_id = ''::text) AND (claim_cluster_filter = ''::text) AND (claim_ttl_milliseconds = 0) AND (claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text) AND (sandbox_id IS NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NULL) AND (claimed_at IS NULL)) OR ((claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 60000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NOT NULL) AND (source_generation_id IS NOT NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL)) OR ((state = 'terminal'::text) AND (claim_operation_id <> ''::text) AND (claim_id <> ''::text) AND ((claim_ttl_milliseconds >= 1000) AND (claim_ttl_milliseconds <= 60000)) AND (((claim_runtime_assignment_revision <> ''::text) AND (claim_network_policy_digest <> ''::text)) OR ((claim_runtime_assignment_revision = ''::text) AND (claim_network_policy_digest = ''::text))) AND (sandbox_id IS NOT NULL) AND (filesystem_id IS NULL) AND (source_generation_id IS NULL) AND (writer_grant_id IS NULL) AND (claim_lease_expires_at IS NOT NULL) AND (claimed_at IS NOT NULL))));
ALTER TABLE manager.runtime_slots DROP CONSTRAINT runtime_slots_claim_ttl_milliseconds_check;
ALTER TABLE manager.runtime_slots ADD CONSTRAINT runtime_slots_claim_ttl_milliseconds_check
    CHECK (claim_ttl_milliseconds BETWEEN 0 AND 60000);
DROP TRIGGER runtime_checkpoint_claim_budget_guard ON manager.runtime_slots;
DROP FUNCTION manager.guard_runtime_checkpoint_claim_budget();
