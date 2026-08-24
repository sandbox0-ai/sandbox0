-- +goose Up

CREATE TABLE quota.team_quota_limits (
    team_id TEXT NOT NULL,
    dimension TEXT NOT NULL,
    limit_value BIGINT NOT NULL,
    interval_ms BIGINT NOT NULL DEFAULT 0,
    burst_value BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, dimension),
    CONSTRAINT team_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    CONSTRAINT team_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    CONSTRAINT team_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    CONSTRAINT team_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    CONSTRAINT team_quota_limits_policy_shape CHECK (
        (dimension = 'active_sandboxes' AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    )
);

CREATE INDEX idx_team_quota_limits_dimension
    ON quota.team_quota_limits(dimension);

CREATE TABLE quota.region_quota_limits (
    dimension TEXT PRIMARY KEY,
    limit_value BIGINT NOT NULL,
    interval_ms BIGINT NOT NULL DEFAULT 0,
    burst_value BIGINT NOT NULL DEFAULT 0,
    managed_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT region_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    CONSTRAINT region_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    CONSTRAINT region_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    CONSTRAINT region_quota_limits_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    ),
    CONSTRAINT region_quota_limits_policy_shape CHECK (
        (dimension = 'active_sandboxes' AND interval_ms = 0 AND burst_value = 0)
        OR (
            dimension IN ('sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
            AND interval_ms > 0
            AND ((limit_value = 0 AND burst_value = 0) OR (limit_value > 0 AND burst_value > 0))
        )
    )
);

CREATE TABLE quota.region_quota_bootstrap (
    dimension TEXT PRIMARY KEY,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT region_quota_bootstrap_dimension_supported CHECK (
        dimension IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes')
    )
);

-- +goose StatementBegin
CREATE FUNCTION quota.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER update_team_quota_limits_updated_at
    BEFORE UPDATE ON quota.team_quota_limits
    FOR EACH ROW EXECUTE FUNCTION quota.update_updated_at_column();
CREATE TRIGGER update_region_quota_limits_updated_at
    BEFORE UPDATE ON quota.region_quota_limits
    FOR EACH ROW EXECUTE FUNCTION quota.update_updated_at_column();

-- +goose StatementBegin
CREATE FUNCTION quota.notify_quota_policy_changed()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('quota_policy_changed', TG_TABLE_NAME);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER notify_team_quota_policy_changed
    AFTER INSERT OR UPDATE OR DELETE ON quota.team_quota_limits
    FOR EACH STATEMENT EXECUTE FUNCTION quota.notify_quota_policy_changed();
CREATE TRIGGER notify_region_quota_policy_changed
    AFTER INSERT OR UPDATE OR DELETE ON quota.region_quota_limits
    FOR EACH STATEMENT EXECUTE FUNCTION quota.notify_quota_policy_changed();

-- +goose Down

-- The final quota contract is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad quota baseline cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
