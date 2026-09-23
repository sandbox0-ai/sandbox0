-- +goose Up

-- Template rows describe future claims. Existing sandbox rows own an immutable
-- copy of their original spec and keep the standard class for resume.
UPDATE scheduler_templates
SET spec = jsonb_set(spec, '{mainContainer,securityClass}', '"privileged"'::jsonb, true)
WHERE jsonb_typeof(spec->'mainContainer') = 'object'
  AND spec #>> '{mainContainer,securityClass}' IS DISTINCT FROM 'privileged';

-- +goose Down

-- Upgrading a template's guest privilege is intentional and cannot be
-- reversed without knowing each owner's original choice.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'privileged template cutover is irreversible'
        USING ERRCODE = '55000';
END;
$$;
-- +goose StatementEnd
