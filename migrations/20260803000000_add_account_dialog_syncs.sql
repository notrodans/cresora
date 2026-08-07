-- +goose Up

CREATE TYPE account_dialog_sync_status_type AS ENUM (
    'pending',
    'running',
    'done',
    'failed'
);

CREATE TABLE account_dialog_syncs (
    account_id uuid PRIMARY KEY,
    status account_dialog_sync_status_type NOT NULL DEFAULT 'pending',
    needs_sync_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    next_retry_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    last_error text,
    lease_token uuid,
    lease_until timestamptz,
    lease_generation bigint,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_account_dialog_syncs_account
        FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE CASCADE,
    CONSTRAINT ck_account_dialog_syncs_attempts CHECK (attempt_count >= 0 AND max_attempts >= 1),
    CONSTRAINT ck_account_dialog_syncs_lease CHECK (
        (lease_token IS NULL AND lease_until IS NULL AND lease_generation IS NULL)
        OR (
            lease_token IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_generation IS NOT NULL
            AND lease_generation >= 1
        )
    ),
    CONSTRAINT ck_account_dialog_syncs_running_lease CHECK (
        status <> 'running'
        OR (
            lease_token IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_generation IS NOT NULL
        )
    ),
    CONSTRAINT ck_account_dialog_syncs_terminal_lease CHECK (
        status NOT IN ('done', 'failed')
        OR (
            lease_token IS NULL
            AND lease_until IS NULL
            AND lease_generation IS NULL
        )
    ),
    CONSTRAINT ck_account_dialog_syncs_failed_evidence CHECK (
        status <> 'failed'
        OR (
            last_error IS NOT NULL
            AND btrim(last_error) <> ''
        )
    )
);

CREATE INDEX ix_account_dialog_syncs_claim
    ON account_dialog_syncs (next_retry_at, needs_sync_at, account_id)
    WHERE status = 'pending';

-- +goose Down

DROP TABLE account_dialog_syncs;
DROP TYPE account_dialog_sync_status_type;