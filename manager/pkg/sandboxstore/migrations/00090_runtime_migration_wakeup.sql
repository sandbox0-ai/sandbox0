-- +goose Up
-- Notifications only request a fresh authoritative scan. They contain no
-- workload data and cannot authorize migration or replace periodic recovery.
-- +goose StatementBegin
CREATE FUNCTION manager.notify_runtime_migration_drain() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'draining' THEN RETURN NULL; END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'draining' THEN RETURN NULL; END IF;
    ELSE
        IF (NEW.state <> 'draining' AND OLD.state <> 'draining')
            OR (NEW.state, NEW.reason) IS NOT DISTINCT FROM (OLD.state, OLD.reason) THEN
            RETURN NULL;
        END IF;
    END IF;
    PERFORM pg_notify('sandbox0_runtime_migration_drain', '');
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER runtime_migration_drain_wakeup
    AFTER INSERT OR UPDATE OR DELETE ON manager.runtime_node_fences
    FOR EACH ROW EXECUTE FUNCTION manager.notify_runtime_migration_drain();

-- +goose Down
DROP TRIGGER runtime_migration_drain_wakeup ON manager.runtime_node_fences;
DROP FUNCTION manager.notify_runtime_migration_drain();
