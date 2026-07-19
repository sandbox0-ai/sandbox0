-- +goose Up

-- Version 1 is deliberately an empty placeholder. The former dimension-based
-- quota schema used the same goose version in this PostgreSQL schema. Team
-- Quota starts at version 2 and drops that obsolete table without translating
-- its state.

-- +goose Down
