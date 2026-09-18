-- +goose Up
-- A resize is durable desired placement, not a second resource ledger. Claims
-- exclude allocations not retained by the intent until placement converges.
CREATE SEQUENCE manager.runtime_carrier_revision;
CREATE TABLE manager.runtime_carrier_controllers (
    cluster_id TEXT PRIMARY KEY,
    heartbeat_expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE manager.runtime_carrier_resizes (
    cluster_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    node_boot_id TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT nextval('manager.runtime_carrier_revision') CHECK (revision > 0),
    pending BOOLEAN NOT NULL DEFAULT true,
    allowed_groups TEXT[] NOT NULL,
    retained_allocations TEXT[] NOT NULL,
    max_carriers INTEGER NOT NULL CHECK (max_carriers BETWEEN 8 AND 576),
    compatibility_capacity JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    surplus_since TIMESTAMPTZ,
    PRIMARY KEY (cluster_id, node_id)
);
-- A cluster-wide pipeline lets each shard retain just one monotonic epoch,
-- rather than an unbounded map of every disposable node ever provisioned.
CREATE UNIQUE INDEX runtime_carrier_one_pending ON manager.runtime_carrier_resizes(cluster_id) WHERE pending;
ALTER TABLE manager.runtime_node_pool_demands ADD COLUMN compatibility_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE manager.runtime_slots ADD COLUMN carrier_retired BOOLEAN NOT NULL DEFAULT false;

-- +goose StatementBegin
CREATE FUNCTION manager.retain_carrier_retirement() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.carrier_retired OR NEW.carrier_retired THEN
        NEW.carrier_retired := true;
        NEW.heartbeat_expires_at := LEAST(OLD.heartbeat_expires_at,NEW.heartbeat_expires_at,NOW());
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER retain_carrier_retirement BEFORE UPDATE ON manager.runtime_slots
    FOR EACH ROW EXECUTE FUNCTION manager.retain_carrier_retirement();

-- Claims already hold the exact capacity row before inserting their lease.
-- Resize preparation takes that same lock before creating/updating its fence,
-- including the first resize (where no fence row existed yet).
-- +goose StatementBegin
CREATE FUNCTION manager.guard_carrier_resize_claim() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM manager.runtime_slots WHERE slot_id=NEW.slot_id AND carrier_retired) THEN
        RAISE EXCEPTION 'Carrier allocation is retired' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM manager.runtime_carrier_resizes
        WHERE cluster_id = NEW.cluster_id AND node_id = NEW.node_id AND pending
            AND NOT EXISTS (SELECT 1 FROM manager.runtime_slots s WHERE s.slot_id=NEW.slot_id
                AND s.allocation_id=ANY(retained_allocations))) THEN
        RAISE EXCEPTION 'Carrier resize admission is fenced' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER carrier_resize_claim_guard BEFORE INSERT ON manager.runtime_resource_leases
    FOR EACH ROW EXECUTE FUNCTION manager.guard_carrier_resize_claim();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Carrier resize fences require explicit reconciliation before rollback' USING ERRCODE = '55000';
END $$;
-- +goose StatementEnd
