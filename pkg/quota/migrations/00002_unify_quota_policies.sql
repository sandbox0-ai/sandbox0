-- +goose Up

ALTER TABLE team_quota_limits
    ADD COLUMN IF NOT EXISTS interval_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS burst_value BIGINT NOT NULL DEFAULT 0;

ALTER TABLE team_quota_limits
    ADD CONSTRAINT team_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT team_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0);

DELETE FROM team_quota_limits
WHERE dimension NOT IN (
    'active_sandboxes',
    'cpu_millicpu',
    'memory_mib',
    'volume_storage_gb',
    'snapshot_storage_gb'
);

ALTER TABLE team_quota_limits
    ADD CONSTRAINT team_quota_limits_dimension_supported CHECK (
        dimension IN (
            'active_sandboxes',
            'cpu_millicpu',
            'memory_mib',
            'volume_storage_gb',
            'snapshot_storage_gb',
            'api_requests',
            'network_egress_bytes',
            'network_ingress_bytes'
        )
    ),
    ADD CONSTRAINT team_quota_limits_policy_shape CHECK (
        (
            dimension IN (
                'active_sandboxes',
                'cpu_millicpu',
                'memory_mib',
                'volume_storage_gb',
                'snapshot_storage_gb'
            )
            AND interval_ms = 0
            AND burst_value = 0
        )
        OR
        (
            dimension IN (
                'api_requests',
                'network_egress_bytes',
                'network_ingress_bytes'
            )
            AND interval_ms > 0
            AND (
                (limit_value = 0 AND burst_value = 0)
                OR (limit_value > 0 AND burst_value > 0)
            )
        )
    );

CREATE TABLE IF NOT EXISTS region_quota_limits (
    dimension TEXT PRIMARY KEY,
    limit_value BIGINT NOT NULL,
    interval_ms BIGINT NOT NULL DEFAULT 0,
    burst_value BIGINT NOT NULL DEFAULT 0,
    managed_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (limit_value >= 0),
    CHECK (interval_ms >= 0),
    CHECK (burst_value >= 0),
    CHECK (
        dimension IN (
            'active_sandboxes',
            'cpu_millicpu',
            'memory_mib',
            'volume_storage_gb',
            'snapshot_storage_gb',
            'api_requests',
            'network_egress_bytes',
            'network_ingress_bytes'
        )
    ),
    CHECK (
        (
            dimension IN (
                'active_sandboxes',
                'cpu_millicpu',
                'memory_mib',
                'volume_storage_gb',
                'snapshot_storage_gb'
            )
            AND interval_ms = 0
            AND burst_value = 0
        )
        OR
        (
            dimension IN (
                'api_requests',
                'network_egress_bytes',
                'network_ingress_bytes'
            )
            AND interval_ms > 0
            AND (
                (limit_value = 0 AND burst_value = 0)
                OR (limit_value > 0 AND burst_value > 0)
            )
        )
    )
);

CREATE TABLE IF NOT EXISTS region_quota_bootstrap (
    dimension TEXT PRIMARY KEY,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        dimension IN (
            'active_sandboxes',
            'cpu_millicpu',
            'memory_mib',
            'volume_storage_gb',
            'snapshot_storage_gb',
            'api_requests',
            'network_egress_bytes',
            'network_ingress_bytes'
        )
    )
);

DROP TRIGGER IF EXISTS update_region_quota_limits_updated_at ON region_quota_limits;
CREATE TRIGGER update_region_quota_limits_updated_at
    BEFORE UPDATE ON region_quota_limits
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_quota_policy_changed()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('quota_policy_changed', TG_TABLE_NAME);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS notify_team_quota_policy_changed ON team_quota_limits;
CREATE TRIGGER notify_team_quota_policy_changed
    AFTER INSERT OR UPDATE OR DELETE ON team_quota_limits
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_quota_policy_changed();

DROP TRIGGER IF EXISTS notify_region_quota_policy_changed ON region_quota_limits;
CREATE TRIGGER notify_region_quota_policy_changed
    AFTER INSERT OR UPDATE OR DELETE ON region_quota_limits
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_quota_policy_changed();

-- +goose Down

DROP TRIGGER IF EXISTS notify_region_quota_policy_changed ON region_quota_limits;
DROP TRIGGER IF EXISTS notify_team_quota_policy_changed ON team_quota_limits;
DROP FUNCTION IF EXISTS notify_quota_policy_changed();
DROP TRIGGER IF EXISTS update_region_quota_limits_updated_at ON region_quota_limits;
DROP TABLE IF EXISTS region_quota_bootstrap;
DROP TABLE IF EXISTS region_quota_limits;
DELETE FROM team_quota_limits
WHERE dimension IN (
    'api_requests',
    'network_egress_bytes',
    'network_ingress_bytes'
);
ALTER TABLE team_quota_limits
    DROP CONSTRAINT IF EXISTS team_quota_limits_policy_shape,
    DROP CONSTRAINT IF EXISTS team_quota_limits_dimension_supported,
    DROP CONSTRAINT IF EXISTS team_quota_limits_burst_value_nonnegative,
    DROP CONSTRAINT IF EXISTS team_quota_limits_interval_ms_nonnegative,
    DROP COLUMN IF EXISTS burst_value,
    DROP COLUMN IF EXISTS interval_ms;
