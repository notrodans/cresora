-- +goose Up

ALTER TABLE mailing_deliveries
    DROP CONSTRAINT ck_mailing_deliveries_terminal_lease,
    ADD CONSTRAINT ck_mailing_deliveries_terminal_lease CHECK (
        status NOT IN ('sent', 'skipped', 'failed', 'unknown')
        OR (
            lease_token IS NULL
            AND lease_until IS NULL
            AND lease_execution_generation IS NULL
        )
    ),
    ADD CONSTRAINT ck_mailing_deliveries_unknown_evidence CHECK (
        status <> 'unknown'
        OR (
            attempt_count >= 1
            AND started_at IS NOT NULL
            AND error_message IS NOT NULL
            AND btrim(error_message) <> ''
        )
    );

-- A time-relative expired predicate cannot be part of a PostgreSQL partial
-- index. Keep the stable part of the predicate here so reapers do not scan
-- terminal and pending deliveries.
CREATE INDEX ix_mailing_deliveries_expired_sending_reaper
    ON mailing_deliveries (lease_until, mailing_id, run_id, recipient_id)
    WHERE status = 'sending'
      AND lease_until IS NOT NULL;

-- +goose Down
-- The enum and its state-machine constraints are intentionally irreversible.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'delivery reaper constraints migration is irreversible';
END
$$;
-- +goose StatementEnd
