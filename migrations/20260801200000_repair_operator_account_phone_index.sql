-- +goose Up

CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_accounts_operator_phone
    ON operator_accounts (operator_id, phone)
    WHERE phone IS NOT NULL;

-- +goose Down

-- The current schema migration owns this index, so rollback is intentionally a no-op.
