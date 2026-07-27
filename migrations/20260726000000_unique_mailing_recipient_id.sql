-- +goose Up
ALTER TABLE mailing_recipients
    ADD CONSTRAINT uq_mailing_recipients_id UNIQUE (id);

-- +goose Down
ALTER TABLE mailing_recipients
    DROP CONSTRAINT uq_mailing_recipients_id;
