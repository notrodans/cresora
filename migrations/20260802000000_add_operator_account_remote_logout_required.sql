-- +goose Up

ALTER TABLE operator_accounts
    ADD COLUMN remote_logout_required boolean NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT ck_operator_accounts_remote_logout_required CHECK (
        remote_logout_required = FALSE OR status = 'disconnecting'
    );

-- +goose Down

ALTER TABLE operator_accounts
    DROP CONSTRAINT IF EXISTS ck_operator_accounts_remote_logout_required,
    DROP COLUMN IF EXISTS remote_logout_required;
