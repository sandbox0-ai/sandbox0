-- +goose Up

-- Zero keeps legacy writers and rows on physical-capacity admission. Physical
-- capacity remains the cgroup boundary; these budgets only bound summed limits.
ALTER TABLE manager.runtime_node_capacities
    ADD COLUMN admission_cpu_millicores BIGINT NOT NULL DEFAULT 0
        CHECK (admission_cpu_millicores = 0 OR
            admission_cpu_millicores BETWEEN cpu_millicores AND cpu_millicores * 16),
    ADD COLUMN admission_memory_bytes BIGINT NOT NULL DEFAULT 0
        CHECK (admission_memory_bytes = 0 OR
            admission_memory_bytes BETWEEN memory_bytes AND memory_bytes * 2);

-- +goose Down

LOCK TABLE manager.runtime_node_capacities, manager.runtime_resource_leases IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM manager.runtime_node_capacities AS capacity
        JOIN manager.runtime_resource_leases AS lease
            USING (cluster_id, node_id, node_uid, node_boot_id)
        WHERE lease.lease_state = 'active'
            AND (capacity.admission_cpu_millicores > capacity.cpu_millicores
                OR capacity.admission_memory_bytes > capacity.memory_bytes)
    ) THEN
        RAISE EXCEPTION 'Drain overcommitted nodes before removing admission budgets';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE manager.runtime_node_capacities
    DROP COLUMN admission_cpu_millicores,
    DROP COLUMN admission_memory_bytes;
