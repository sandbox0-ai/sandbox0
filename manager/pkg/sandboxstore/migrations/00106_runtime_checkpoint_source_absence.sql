-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE l manager.sandbox_lifecycle_txns%ROWTYPE;
BEGIN
    IF NEW.evidence ? 'cancel_authorized' THEN
        SELECT * INTO l FROM manager.sandbox_lifecycle_txns WHERE txn_id=NEW.operation_id;
        IF NEW.evidence->>'cancel_authorized' IS DISTINCT FROM 'true'
            OR NEW.evidence ? 'capture_authorized'
            OR l.phase NOT IN ('preparing','barriered','aborted') THEN
            RAISE EXCEPTION 'Checkpoint cancellation cannot coexist with execution authority' USING ERRCODE='23514';
        END IF;
        IF TG_OP='INSERT' THEN
            RAISE EXCEPTION 'Checkpoint cancellation requires existing source custody' USING ERRCODE='23514';
        END IF;
        IF (NEW.evidence - ARRAY['cancel_authorized','canceled','source_absent_proof','staging_released'])
            IS DISTINCT FROM (OLD.evidence - ARRAY['cancel_authorized','canceled','source_absent_proof','staging_released']) THEN
            RAISE EXCEPTION 'Canceled checkpoint cannot accept new capture evidence' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.evidence ? 'source_absent_proof' AND (
        NOT NEW.evidence ? 'cancel_authorized' OR NOT NEW.evidence ? 'preparation'
        OR jsonb_typeof(NEW.evidence->'source_absent_proof') <> 'string')
        OR NEW.evidence ? 'canceled' AND (NOT NEW.evidence ? 'cancel_authorized' OR NOT NEW.evidence ? 'preparation')
        OR NEW.evidence ? 'staging_released' AND (NOT NEW.evidence ? 'cancel_authorized' OR NOT NEW.evidence ? 'staging'
            OR NEW.evidence ? 'preparation' AND NOT (NEW.evidence ? 'canceled' OR NEW.evidence ? 'source_absent_proof')) THEN
        RAISE EXCEPTION 'Checkpoint cancellation receipt lacks regional authority' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION manager.guard_runtime_checkpoint_canceled_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c manager.sandbox_runtime_checkpoints%ROWTYPE;
BEGIN
    SELECT * INTO c FROM manager.sandbox_runtime_checkpoints WHERE operation_id=NEW.txn_id;
    IF NOT FOUND OR c.evidence ? 'capture_authorized' THEN RETURN NEW; END IF;
    IF c.evidence ? 'cancel_authorized' AND NEW.phase IS DISTINCT FROM OLD.phase AND NEW.phase<>'aborted' THEN
        RAISE EXCEPTION 'Canceled checkpoint cannot advance capture' USING ERRCODE='23514';
    END IF;
    IF NEW.phase<>'aborted' OR OLD.phase='aborted' THEN RETURN NEW; END IF;
    IF NOT c.evidence ? 'cancel_authorized'
        OR c.evidence ? 'preparation' AND NOT (c.evidence ? 'canceled' OR c.evidence ? 'source_absent_proof')
        OR c.evidence ? 'staging' AND NOT c.evidence ? 'staging_released' THEN
        RAISE EXCEPTION 'Checkpoint abort requires source cancellation or absence and staging release' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION 'Checkpoint source-absence fences must be retained' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
