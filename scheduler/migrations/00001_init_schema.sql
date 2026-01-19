-- Scheduler database schema
-- This migration creates the tables needed for the scheduler service

-- Clusters table: stores registered data-plane clusters
CREATE TABLE IF NOT EXISTS scheduler_clusters (
    cluster_id VARCHAR(255) PRIMARY KEY,
    internal_gateway_url VARCHAR(1024) NOT NULL,
    weight INTEGER NOT NULL DEFAULT 100,
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_seen_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Templates table: stores SandboxTemplate definitions (source of truth)
CREATE TABLE IF NOT EXISTS scheduler_templates (
    template_id VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'sandbox0',
    spec JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (template_id, namespace)
);

-- Template allocations table: tracks how templates are distributed across clusters
CREATE TABLE IF NOT EXISTS scheduler_template_allocations (
    template_id VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'sandbox0',
    cluster_id VARCHAR(255) NOT NULL REFERENCES scheduler_clusters(cluster_id) ON DELETE CASCADE,
    min_idle INTEGER NOT NULL DEFAULT 0,
    max_idle INTEGER NOT NULL DEFAULT 0,
    last_synced_at TIMESTAMP WITH TIME ZONE,
    sync_status VARCHAR(50) DEFAULT 'pending', -- pending, synced, error
    sync_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (template_id, namespace, cluster_id),
    FOREIGN KEY (template_id, namespace) REFERENCES scheduler_templates(template_id, namespace) ON DELETE CASCADE
);

-- Index for efficient queries
CREATE INDEX IF NOT EXISTS idx_scheduler_clusters_enabled ON scheduler_clusters(enabled);
CREATE INDEX IF NOT EXISTS idx_scheduler_templates_namespace ON scheduler_templates(namespace);
CREATE INDEX IF NOT EXISTS idx_scheduler_allocations_cluster ON scheduler_template_allocations(cluster_id);
CREATE INDEX IF NOT EXISTS idx_scheduler_allocations_sync_status ON scheduler_template_allocations(sync_status);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
DROP TRIGGER IF EXISTS update_scheduler_clusters_updated_at ON scheduler_clusters;
CREATE TRIGGER update_scheduler_clusters_updated_at
    BEFORE UPDATE ON scheduler_clusters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_scheduler_templates_updated_at ON scheduler_templates;
CREATE TRIGGER update_scheduler_templates_updated_at
    BEFORE UPDATE ON scheduler_templates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_scheduler_allocations_updated_at ON scheduler_template_allocations;
CREATE TRIGGER update_scheduler_allocations_updated_at
    BEFORE UPDATE ON scheduler_template_allocations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
