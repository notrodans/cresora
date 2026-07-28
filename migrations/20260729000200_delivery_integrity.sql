-- +goose Up

-- The Telegram random ID is the durable idempotency key for a logical
-- delivery. It may be read by retries, but it must never be replaced in
-- place.
-- +goose StatementBegin
CREATE FUNCTION prevent_telegram_mailing_delivery_random_id_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.random_id IS DISTINCT FROM OLD.random_id THEN
        RAISE EXCEPTION 'telegram_mailing_deliveries.random_id is immutable'
            USING ERRCODE = 'check_violation',
                  CONSTRAINT = 'ck_telegram_mailing_deliveries_random_id_immutable';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_telegram_mailing_deliveries_random_id_immutable
    BEFORE UPDATE OF random_id ON telegram_mailing_deliveries
    FOR EACH ROW
    EXECUTE FUNCTION prevent_telegram_mailing_delivery_random_id_update();

ALTER TABLE mailing_deliveries
    ADD CONSTRAINT ck_mailing_deliveries_sending_evidence CHECK (
        status <> 'sending'
        OR (
            attempt_count >= 1
            AND started_at IS NOT NULL
        )
    );

-- +goose Down
-- Removing these guards would reopen the delivery state machine and make the
-- persisted random ID mutable again. Keep the integrity cutover forward-only.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION 'delivery integrity migration is irreversible';
END
$$;
-- +goose StatementEnd
