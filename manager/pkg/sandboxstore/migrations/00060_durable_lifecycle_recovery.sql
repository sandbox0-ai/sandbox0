-- +goose Up
-- A lifecycle action observed in PostgreSQL remains recoverable after ESS no
-- longer lists its transient token. These fields fence one recovery owner and
-- retain bounded provider-absence evidence until cleanup reaches a proof.
ALTER TABLE manager.runtime_node_lifecycle_actions
    ADD COLUMN recovery_owner_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN recovery_epoch BIGINT NOT NULL DEFAULT 0
        CHECK (recovery_epoch >= 0),
    ADD COLUMN recovery_lease_expires_at TIMESTAMPTZ,
    ADD COLUMN recovery_deadline_at TIMESTAMPTZ,
    ADD COLUMN provider_action_last_observed_at TIMESTAMPTZ,
    ADD COLUMN provider_action_absent_since TIMESTAMPTZ,
    ADD COLUMN provider_instance_absent_since TIMESTAMPTZ,
    ADD COLUMN convergence_proof JSONB;

-- Existing terminal rows predate explicit proof objects. Preserve their
-- durable terminal state with a clearly labelled legacy receipt rather than
-- inventing provider responses.
UPDATE manager.runtime_node_lifecycle_actions
SET convergence_proof = JSONB_BUILD_OBJECT(
        'legacy', TRUE,
        'state', state,
        'completed_at', completed_at
    ),
    recovery_deadline_at = COALESCE(completed_at, first_observed_at)
WHERE state IN ('completed', 'abandoned');

UPDATE manager.runtime_node_lifecycle_actions
SET recovery_deadline_at = first_observed_at + INTERVAL '1 hour'
WHERE recovery_deadline_at IS NULL;

ALTER TABLE manager.runtime_node_lifecycle_actions
    ALTER COLUMN recovery_deadline_at SET NOT NULL,
    ADD CONSTRAINT runtime_node_lifecycle_owner_with_lease CHECK (
        (recovery_owner_id = '') = (recovery_lease_expires_at IS NULL)
    ),
    ADD CONSTRAINT runtime_node_lifecycle_terminal_proof CHECK (
        (state IN ('completed', 'abandoned')) = (convergence_proof IS NOT NULL)
    ),
    ADD CONSTRAINT runtime_node_lifecycle_absent_after_first_observation CHECK (
        provider_action_absent_since IS NULL
        OR provider_action_absent_since >= first_observed_at
    ),
    ADD CONSTRAINT runtime_node_lifecycle_instance_absent_after_action_absence CHECK (
        provider_instance_absent_since IS NULL
        OR (
            provider_action_absent_since IS NOT NULL
            AND provider_instance_absent_since >= provider_action_absent_since
        )
    );

CREATE INDEX idx_runtime_node_lifecycle_actions_recovery
    ON manager.runtime_node_lifecycle_actions(pool_id, recovery_deadline_at, lifecycle_action_token)
    WHERE state IN ('pending', 'draining');

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Durable lifecycle recovery receipts cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
