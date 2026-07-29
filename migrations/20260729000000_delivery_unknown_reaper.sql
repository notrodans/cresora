-- +goose Up

-- Keep the enum addition in its own Goose migration. PostgreSQL must commit a
-- new enum value before a later migration can use it in a constraint.
ALTER TYPE mailing_delivery_status_type ADD VALUE IF NOT EXISTS 'unknown';

-- +goose Down
-- PostgreSQL enum values cannot be removed safely.
-- This migration is intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'delivery unknown status migration is irreversible';
END
$$;
-- +goose StatementEnd
