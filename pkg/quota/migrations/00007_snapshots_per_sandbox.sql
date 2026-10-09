-- +goose Up

ALTER TABLE quota.team_quota_limits
    DROP CONSTRAINT IF EXISTS team_quota_limits_dimension_supported,
    DROP CONSTRAINT IF EXISTS team_quota_limits_policy_shape;
ALTER TABLE quota.region_quota_limits
    DROP CONSTRAINT IF EXISTS region_quota_limits_dimension_supported,
    DROP CONSTRAINT IF EXISTS region_quota_limits_policy_shape;
ALTER TABLE quota.region_quota_bootstrap
    DROP CONSTRAINT IF EXISTS region_quota_bootstrap_dimension_supported;

ALTER TABLE quota.team_quota_limits
    ADD CONSTRAINT team_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'paused_sandboxes', 'snapshots_per_sandbox', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    ADD CONSTRAINT team_quota_limits_policy_shape CHECK (
        (dimension IN ('active_sandboxes', 'paused_sandboxes', 'snapshots_per_sandbox') AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    ),
    ADD CONSTRAINT team_quota_limits_snapshot_retention_positive CHECK (
        dimension <> 'snapshots_per_sandbox' OR limit_value > 0
    );
ALTER TABLE quota.region_quota_limits
    ADD CONSTRAINT region_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'paused_sandboxes', 'snapshots_per_sandbox', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    ADD CONSTRAINT region_quota_limits_policy_shape CHECK (
        (dimension IN ('active_sandboxes', 'paused_sandboxes', 'snapshots_per_sandbox') AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    ),
    ADD CONSTRAINT region_quota_limits_snapshot_retention_positive CHECK (
        dimension <> 'snapshots_per_sandbox' OR limit_value > 0
    );
ALTER TABLE quota.region_quota_bootstrap
    ADD CONSTRAINT region_quota_bootstrap_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'paused_sandboxes', 'snapshots_per_sandbox', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    );

-- +goose Down

-- Automatically removed snapshots cannot be reconstructed by a rollback.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'snapshot retention quota migration cannot be rolled back';
END; $$;
-- +goose StatementEnd
