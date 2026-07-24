-- +goose Up

CREATE TABLE IF NOT EXISTS hot_claim_reservations (
    sandbox_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    pod_namespace TEXT NOT NULL,
    pod_name TEXT NOT NULL,
    pod_uid TEXT NOT NULL,
    desired_labels JSONB,
    desired_annotations JSONB,
    desired_finalizers JSONB,
    committed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cluster_id, pod_namespace, pod_name)
);

CREATE INDEX IF NOT EXISTS idx_hot_claim_reservations_reconcile
    ON hot_claim_reservations(cluster_id, created_at);

-- +goose Down

DROP INDEX IF EXISTS idx_hot_claim_reservations_reconcile;
DROP TABLE IF EXISTS hot_claim_reservations;
