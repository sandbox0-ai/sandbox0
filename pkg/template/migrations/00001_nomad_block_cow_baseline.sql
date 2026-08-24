-- +goose Up

-- Final region-authoritative template schema. Warm capacity is managed by
-- resource-neutral Nomad slots, and template captures retain block-COW state.

CREATE TABLE scheduler_clusters (
    cluster_id VARCHAR(255) PRIMARY KEY,
    cluster_name VARCHAR(255) NOT NULL,
    cluster_gateway_url VARCHAR(1024) NOT NULL,
    weight INTEGER NOT NULL DEFAULT 100,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE scheduler_templates (
    template_id VARCHAR(255) NOT NULL,
    scope VARCHAR(32) NOT NULL DEFAULT 'public',
    team_id VARCHAR(255) NOT NULL DEFAULT '',
    user_id VARCHAR(255) NOT NULL DEFAULT '',
    spec JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    creation_build_id UUID,
    creation_idempotency_key VARCHAR(255),
    creation_request_hash VARCHAR(64),
    creation_state VARCHAR(32) NOT NULL DEFAULT 'ready',
    creation_stage VARCHAR(32),
    creation_started_at TIMESTAMPTZ,
    creation_captured_at TIMESTAMPTZ,
    creation_completed_at TIMESTAMPTZ,
    creation_reason VARCHAR(128),
    creation_message TEXT,
    rootfs_storage_format TEXT,
    rootfs_snapshot_id TEXT,
    rootfs_generation_id TEXT,
    rootfs_source_oci_digest TEXT,
    rootfs_base_artifact_digest TEXT,
    rootfs_format_generation INTEGER,
    rootfs_platform_os TEXT,
    rootfs_platform_architecture TEXT,
    rootfs_platform_variant TEXT,
    PRIMARY KEY (scope, team_id, template_id),
    CONSTRAINT scheduler_templates_scope_check
        CHECK (scope IN ('public', 'team')),
    CONSTRAINT scheduler_templates_creation_state_check
        CHECK (creation_state IN ('creating', 'ready', 'failed')),
    CONSTRAINT scheduler_templates_creation_stage_check
        CHECK (creation_stage IS NULL OR creation_stage IN ('capturing', 'publishing')),
    CONSTRAINT scheduler_templates_image_digest_check CHECK (
        (spec #>> '{mainContainer,image}') ~ '^[^[:space:]@]+@sha256:[0-9a-f]{64}$'
    ),
    CONSTRAINT scheduler_templates_rootfs_source_shape_check CHECK (
        (
            rootfs_storage_format IS NULL
            AND rootfs_snapshot_id IS NULL
            AND rootfs_generation_id IS NULL
            AND rootfs_source_oci_digest IS NULL
            AND rootfs_base_artifact_digest IS NULL
            AND rootfs_format_generation IS NULL
            AND rootfs_platform_os IS NULL
            AND rootfs_platform_architecture IS NULL
            AND rootfs_platform_variant IS NULL
        )
        OR
        (
            rootfs_storage_format = 'block-cow-v1'
            AND rootfs_snapshot_id IS NOT NULL AND rootfs_snapshot_id <> ''
            AND rootfs_generation_id IS NOT NULL AND rootfs_generation_id <> ''
            AND rootfs_source_oci_digest IS NOT NULL AND rootfs_source_oci_digest <> ''
            AND rootfs_base_artifact_digest IS NOT NULL AND rootfs_base_artifact_digest <> ''
            AND rootfs_format_generation > 0
            AND rootfs_platform_os IS NOT NULL AND rootfs_platform_os <> ''
            AND rootfs_platform_architecture IS NOT NULL AND rootfs_platform_architecture <> ''
            AND rootfs_platform_variant IS NOT NULL
        )
    )
);

CREATE TABLE scheduler_template_builds (
    build_id UUID PRIMARY KEY,
    template_id VARCHAR(255) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    team_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL DEFAULT '',
    source_sandbox_id VARCHAR(255) NOT NULL,
    target_cluster_id VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(255),
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    stage VARCHAR(32) NOT NULL DEFAULT 'capturing',
    snapshot_id VARCHAR(255),
    capture_metadata JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduler_template_builds_scope_check
        CHECK (scope IN ('public', 'team')),
    CONSTRAINT scheduler_template_builds_status_check
        CHECK (status IN ('queued', 'running', 'cancelled')),
    CONSTRAINT scheduler_template_builds_stage_check
        CHECK (stage IN ('capturing', 'publishing')),
    CONSTRAINT scheduler_template_builds_capture_version_check
        CHECK (capture_metadata IS NULL OR capture_metadata->>'version' = '2')
);

-- Cleanup rows outlive the visible template until a manager removes the
-- retained regional snapshot.
CREATE TABLE scheduler_template_rootfs_deletions (
    snapshot_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scheduler_clusters_enabled ON scheduler_clusters(enabled);
CREATE UNIQUE INDEX idx_scheduler_clusters_name ON scheduler_clusters(cluster_name);
CREATE INDEX idx_scheduler_templates_template_id ON scheduler_templates(template_id);
CREATE INDEX idx_scheduler_templates_team_id ON scheduler_templates(team_id);
CREATE UNIQUE INDEX scheduler_templates_creation_idempotency_key
    ON scheduler_templates(scope, team_id, creation_idempotency_key)
    WHERE creation_idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX scheduler_templates_rootfs_snapshot
    ON scheduler_templates(rootfs_snapshot_id)
    WHERE rootfs_snapshot_id IS NOT NULL;
CREATE INDEX scheduler_template_builds_capture_claim
    ON scheduler_template_builds(target_cluster_id, next_attempt_at, created_at)
    WHERE status IN ('queued', 'running')
      AND stage = 'capturing'
      AND cancel_requested_at IS NULL;
CREATE INDEX scheduler_template_builds_takeover_claim
    ON scheduler_template_builds(next_attempt_at, created_at)
    WHERE status IN ('queued', 'running')
      AND stage = 'publishing'
      AND cancel_requested_at IS NULL;
CREATE INDEX scheduler_template_builds_cleanup
    ON scheduler_template_builds(next_attempt_at, created_at)
    WHERE cancel_requested_at IS NOT NULL;
CREATE INDEX scheduler_template_builds_template
    ON scheduler_template_builds(scope, team_id, template_id);
CREATE INDEX scheduler_template_rootfs_deletions_due
    ON scheduler_template_rootfs_deletions(next_attempt_at, created_at);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER update_scheduler_clusters_updated_at
    BEFORE UPDATE ON scheduler_clusters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_scheduler_templates_updated_at
    BEFORE UPDATE ON scheduler_templates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_scheduler_template_builds_updated_at
    BEFORE UPDATE ON scheduler_template_builds
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_scheduler_template_rootfs_deletions_updated_at
    BEFORE UPDATE ON scheduler_template_rootfs_deletions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- +goose Down

-- The final template authority baseline is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'Nomad block-COW template baseline cannot be rolled back'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
