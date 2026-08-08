-- +goose Up

CREATE TABLE operator_account_force_forget_events (
    event_id uuid PRIMARY KEY DEFAULT uuidv7(),
    event_type varchar(64) NOT NULL,
    operator_id uuid NOT NULL,
    account_id uuid NOT NULL,
    previous_version bigint NOT NULL,
    resulting_version bigint NOT NULL,
    reason varchar(64) NOT NULL,
    idempotency_key uuid NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_operator_account_force_forget_events_operator
        FOREIGN KEY (operator_id) REFERENCES operators (id),
    CONSTRAINT fk_operator_account_force_forget_events_account
        FOREIGN KEY (account_id) REFERENCES operator_accounts (id),
    CONSTRAINT uq_operator_account_force_forget_events_idempotency
        UNIQUE (operator_id, account_id, idempotency_key),
    CONSTRAINT ck_operator_account_force_forget_events_type
        CHECK (event_type = 'operator_account_force_forgotten'),
    CONSTRAINT ck_operator_account_force_forget_events_versions
        CHECK (previous_version > 0 AND resulting_version = previous_version + 1),
    CONSTRAINT ck_operator_account_force_forget_events_reason
        CHECK (reason = 'remote_logout_unverified_operator_override')
);

CREATE INDEX ix_operator_account_force_forget_events_account
    ON operator_account_force_forget_events (account_id, occurred_at);

-- +goose Down

DROP TABLE IF EXISTS operator_account_force_forget_events;
