-- +goose Up
-- A successful warmup belongs to one physical boot and one immutable template
-- revision. A short lease lets another manager recover after a crash.
CREATE TABLE manager.runtime_node_cache_prewarms (
    window_name TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    template_digest TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
    state TEXT NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'complete', 'failed')),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_name, node_uid, node_boot_id, template_digest),
    CHECK ((state = 'complete') = (completed_at IS NOT NULL))
);

CREATE INDEX runtime_node_cache_prewarms_incomplete
    ON manager.runtime_node_cache_prewarms (lease_expires_at)
    WHERE state <> 'complete';

-- +goose Down
DROP TABLE manager.runtime_node_cache_prewarms;
