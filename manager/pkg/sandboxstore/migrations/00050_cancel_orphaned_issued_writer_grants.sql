-- +goose Up

-- Older claim planners issued writer authority and bound the runtime slot in
-- separate transactions. A concurrent resume abort could therefore leave an
-- unconsumed grant live after its slot became terminal. Such a grant has never
-- held runtime write authority and is safe to cancel once no matching live slot
-- carries it.
UPDATE manager.rootfs_writer_grants AS writer_grant
SET state = 'canceled',
    canceled_at = COALESCE(canceled_at, NOW()),
    updated_at = NOW()
WHERE writer_grant.state = 'issued'
  AND NOT EXISTS (
      SELECT 1
      FROM manager.runtime_slots AS slot
      WHERE slot.slot_id = writer_grant.slot_id
        AND slot.writer_grant_id = writer_grant.grant_id
        AND slot.claim_id = writer_grant.claim_id
        AND slot.sandbox_id = writer_grant.sandbox_id
        AND slot.state IN ('claiming', 'starting', 'active')
  );

-- +goose Down

-- Canceled grants cannot be made live again because their bearer tokens may
-- have escaped before the repair ran.
