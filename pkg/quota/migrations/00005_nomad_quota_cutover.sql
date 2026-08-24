-- +goose Up

ALTER TABLE quota.team_quota_limits
    DROP CONSTRAINT IF EXISTS team_quota_limits_limit_value_nonnegative,
    DROP CONSTRAINT IF EXISTS team_quota_limits_interval_ms_nonnegative,
    DROP CONSTRAINT IF EXISTS team_quota_limits_burst_value_nonnegative,
    DROP CONSTRAINT IF EXISTS team_quota_limits_dimension_supported,
    DROP CONSTRAINT IF EXISTS team_quota_limits_policy_shape;
ALTER TABLE quota.region_quota_limits
    DROP CONSTRAINT IF EXISTS region_quota_limits_limit_value_nonnegative,
    DROP CONSTRAINT IF EXISTS region_quota_limits_interval_ms_nonnegative,
    DROP CONSTRAINT IF EXISTS region_quota_limits_burst_value_nonnegative,
    DROP CONSTRAINT IF EXISTS region_quota_limits_dimension_supported,
    DROP CONSTRAINT IF EXISTS region_quota_limits_policy_shape;
ALTER TABLE quota.region_quota_bootstrap
    DROP CONSTRAINT IF EXISTS region_quota_bootstrap_dimension_supported;

DELETE FROM quota.team_quota_limits
WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes');
DELETE FROM quota.region_quota_limits
WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes');
DELETE FROM quota.region_quota_bootstrap
WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes');

ALTER TABLE quota.team_quota_limits
    ADD CONSTRAINT team_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT team_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT team_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    ADD CONSTRAINT team_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    ADD CONSTRAINT team_quota_limits_policy_shape CHECK (
        (dimension = 'active_sandboxes' AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    );
ALTER TABLE quota.region_quota_limits
    ADD CONSTRAINT region_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT region_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT region_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    ADD CONSTRAINT region_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    ADD CONSTRAINT region_quota_limits_policy_shape CHECK (
        (dimension = 'active_sandboxes' AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    );
ALTER TABLE quota.region_quota_bootstrap
    ADD CONSTRAINT region_quota_bootstrap_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    );

-- +goose Down

-- Removed quota rows cannot be reconstructed after the runtime cutover.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad quota cutover cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
