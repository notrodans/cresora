-- +goose Up
CREATE SEQUENCE mailing_delivery_random_id_seq
    AS bigint
    MINVALUE 1
    START WITH 1
    NO CYCLE;

SELECT setval(
    'mailing_delivery_random_id_seq',
    GREATEST(COALESCE((SELECT MAX(random_id) FROM telegram_mailing_deliveries), 0) + 1, 1),
    false
);

-- +goose Down
DROP SEQUENCE mailing_delivery_random_id_seq;
