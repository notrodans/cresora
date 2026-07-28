-- +goose Up

-- This is a destructive cutover.  The preceding gate migration creates
-- delivery_execution_v2_cutover_ack, but intentionally does not acknowledge
-- it.  The operator must stop every legacy sender and insert the documented
-- acknowledgement before rerunning Goose.  A PostgreSQL lock cannot protect
-- against a sender that already read a row before the lock was acquired.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM delivery_execution_v2_cutover_ack
        WHERE acknowledgement_id
    ) THEN
        RAISE EXCEPTION
            'delivery execution v2 cutover requires operator acknowledgement after all legacy senders are stopped';
    END IF;
END
$$;
-- +goose StatementEnd

-- Serialize the DDL and history deletion after the operational prerequisite
-- has been met.  This lock is not the sender-safety mechanism; the explicit
-- acknowledgement above is.
LOCK TABLE mailings, mailing_runs, mailing_deliveries, telegram_mailing_deliveries
    IN ACCESS EXCLUSIVE MODE;

-- Keep drafts and their editable recipient graphs, but discard all execution
-- history.  The foreign keys from deliveries cascade from mailing_runs.
DELETE FROM mailing_runs;

UPDATE mailings
SET status = 'stopped',
    updated_at = CURRENT_TIMESTAMP
WHERE status <> 'draft';

ALTER TABLE mailing_runs
    ADD COLUMN execution_generation bigint NOT NULL DEFAULT 1,
    ADD CONSTRAINT ck_mailing_runs_execution_generation_positive
        CHECK (execution_generation >= 1);

-- A cancelled queued run has no started_at, but still needs a durable
-- finished_at timestamp recording when cancellation took effect.
ALTER TABLE mailing_runs
    DROP CONSTRAINT ck_mailing_runs_finished,
    ADD CONSTRAINT ck_mailing_runs_finished CHECK (
        finished_at IS NULL
        OR (
            status = 'cancelled'
            AND started_at IS NULL
            AND finished_at >= queued_at
        )
        OR (
            started_at IS NOT NULL
            AND finished_at >= started_at
        )
    ),
    ADD CONSTRAINT ck_mailing_runs_cancelled_before_start CHECK (
        status <> 'cancelled'
        OR started_at IS NOT NULL
        OR finished_at IS NOT NULL
    );

ALTER TABLE mailing_deliveries
    ADD COLUMN lease_execution_generation bigint;

ALTER TABLE mailing_deliveries
    DROP CONSTRAINT ck_mailing_deliveries_lease,
    ADD CONSTRAINT ck_mailing_deliveries_lease CHECK (
        (
            lease_token IS NULL
            AND lease_until IS NULL
            AND lease_execution_generation IS NULL
        )
        OR (
            lease_token IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_execution_generation IS NOT NULL
            AND lease_execution_generation >= 1
        )
    );

ALTER TABLE mailing_deliveries
    ADD CONSTRAINT ck_mailing_deliveries_sending_lease CHECK (
        status <> 'sending'
        OR (
            lease_token IS NOT NULL
            AND lease_until IS NOT NULL
            AND lease_execution_generation IS NOT NULL
        )
    ),
    ADD CONSTRAINT ck_mailing_deliveries_terminal_lease CHECK (
        status NOT IN ('sent', 'skipped', 'failed')
        OR (
            lease_token IS NULL
            AND lease_until IS NULL
            AND lease_execution_generation IS NULL
        )
    );

DROP INDEX IF EXISTS ix_mailing_deliveries_claim;
CREATE INDEX ix_mailing_deliveries_claim
    ON mailing_deliveries (ready_at, mailing_id, run_id, recipient_id)
    WHERE status = 'pending';

CREATE UNIQUE INDEX uq_mailing_runs_active
    ON mailing_runs (mailing_id)
    WHERE status IN ('queued', 'running');

-- +goose Down
-- This development cutover is intentionally irreversible.  Restoring the
-- pre-cutover execution graph would risk sending with an obsolete fence.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'delivery execution v2 migration is irreversible';
END
$$;
-- +goose StatementEnd
