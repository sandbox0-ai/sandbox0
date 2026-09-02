-- +goose Up

-- Public templates installed before the Nomad cutover retained provenance for
-- the removed Kubernetes infra-operator. Keep the existing template specs and
-- ownership intact while correcting only that stale public description suffix.
UPDATE scheduler_templates
SET spec = jsonb_set(
        spec,
        '{description}',
        to_jsonb(regexp_replace(
            spec->>'description',
            ' installed by infra-operator\.$',
            ' provided by Sandbox0.'
        )),
        false
    )
WHERE scope = 'public'
  AND team_id = ''
  AND spec->>'description' ~ ' installed by infra-operator\.$';

-- +goose Down

-- The old description advertises a component that no longer exists and must
-- not be restored on rollback.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'retired infra-operator template descriptions cannot be restored'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
