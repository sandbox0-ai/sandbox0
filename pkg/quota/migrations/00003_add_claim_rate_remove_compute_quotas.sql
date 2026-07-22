-- +goose Up

-- +goose StatementBegin
DO $$
DECLARE
    constraint_record RECORD;
BEGIN
    FOR constraint_record IN
        SELECT conrelid::regclass::text AS table_name, conname
        FROM pg_constraint
        WHERE conrelid IN (
            'team_quota_limits'::regclass,
            'region_quota_limits'::regclass,
            'region_quota_bootstrap'::regclass
        )
        AND contype = 'c'
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_record.table_name,
            constraint_record.conname
        );
    END LOOP;
END;
$$;
-- +goose StatementEnd

DELETE FROM team_quota_limits
WHERE dimension IN ('cpu_millicpu', 'memory_mib');

DELETE FROM region_quota_limits
WHERE dimension IN ('cpu_millicpu', 'memory_mib');

DELETE FROM region_quota_bootstrap
WHERE dimension IN ('cpu_millicpu', 'memory_mib');

ALTER TABLE team_quota_limits
    ADD CONSTRAINT team_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT team_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT team_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    ADD CONSTRAINT team_quota_limits_dimension_supported CHECK (
        dimension IN (
            'active_sandboxes',
            'sandbox_claims',
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
                'volume_storage_gb',
                'snapshot_storage_gb'
            )
            AND interval_ms = 0
            AND burst_value = 0
        )
        OR
        (
            dimension IN (
                'sandbox_claims',
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

ALTER TABLE region_quota_limits
    ADD CONSTRAINT region_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT region_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT region_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    ADD CONSTRAINT region_quota_limits_dimension_supported CHECK (
        dimension IN (
            'active_sandboxes',
            'sandbox_claims',
            'volume_storage_gb',
            'snapshot_storage_gb',
            'api_requests',
            'network_egress_bytes',
            'network_ingress_bytes'
        )
    ),
    ADD CONSTRAINT region_quota_limits_policy_shape CHECK (
        (
            dimension IN (
                'active_sandboxes',
                'volume_storage_gb',
                'snapshot_storage_gb'
            )
            AND interval_ms = 0
            AND burst_value = 0
        )
        OR
        (
            dimension IN (
                'sandbox_claims',
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

ALTER TABLE region_quota_bootstrap
    ADD CONSTRAINT region_quota_bootstrap_dimension_supported CHECK (
        dimension IN (
            'active_sandboxes',
            'sandbox_claims',
            'volume_storage_gb',
            'snapshot_storage_gb',
            'api_requests',
            'network_egress_bytes',
            'network_ingress_bytes'
        )
    );

-- +goose Down

-- +goose StatementBegin
DO $$
DECLARE
    constraint_record RECORD;
BEGIN
    FOR constraint_record IN
        SELECT conrelid::regclass::text AS table_name, conname
        FROM pg_constraint
        WHERE conrelid IN (
            'team_quota_limits'::regclass,
            'region_quota_limits'::regclass,
            'region_quota_bootstrap'::regclass
        )
        AND contype = 'c'
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_record.table_name,
            constraint_record.conname
        );
    END LOOP;
END;
$$;
-- +goose StatementEnd

DELETE FROM team_quota_limits WHERE dimension = 'sandbox_claims';
DELETE FROM region_quota_limits WHERE dimension = 'sandbox_claims';
DELETE FROM region_quota_bootstrap WHERE dimension = 'sandbox_claims';

ALTER TABLE team_quota_limits
    ADD CONSTRAINT team_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT team_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT team_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
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

ALTER TABLE region_quota_limits
    ADD CONSTRAINT region_quota_limits_limit_value_nonnegative CHECK (limit_value >= 0),
    ADD CONSTRAINT region_quota_limits_interval_ms_nonnegative CHECK (interval_ms >= 0),
    ADD CONSTRAINT region_quota_limits_burst_value_nonnegative CHECK (burst_value >= 0),
    ADD CONSTRAINT region_quota_limits_dimension_supported CHECK (
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
    ADD CONSTRAINT region_quota_limits_policy_shape CHECK (
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

ALTER TABLE region_quota_bootstrap
    ADD CONSTRAINT region_quota_bootstrap_dimension_supported CHECK (
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
    );
