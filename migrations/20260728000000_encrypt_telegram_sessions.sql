-- +goose Up

-- The old sessions.session column contained plaintext gotd session strings.
-- Do not transform or retain them: the application has no plaintext fallback.
LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sessions) THEN
        RAISE EXCEPTION 'cannot migrate telegram sessions: legacy plaintext session rows exist; remove or re-encrypt them out of band before applying this migration';
    END IF;
END
$$;
-- +goose StatementEnd

ALTER TABLE sessions
    DROP COLUMN session,
    ADD COLUMN format_version integer NOT NULL,
    ADD COLUMN key_id varchar(128) NOT NULL,
    ADD COLUMN nonce bytea NOT NULL,
    ADD COLUMN ciphertext bytea NOT NULL,
    ADD CONSTRAINT ck_sessions_format_version CHECK (format_version = 1),
    ADD CONSTRAINT ck_sessions_key_id CHECK (btrim(key_id) = key_id AND length(key_id) BETWEEN 1 AND 128),
    ADD CONSTRAINT ck_sessions_nonce CHECK (octet_length(nonce) = 12),
    ADD CONSTRAINT ck_sessions_ciphertext CHECK (octet_length(ciphertext) BETWEEN 16 AND 1048592);

-- +goose Down

-- This rollback never decrypts data and is intentionally usable only for an
-- empty table. Encrypted rows cannot be represented by the legacy plaintext
-- column and make this migration irreversible in practice.
LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sessions) THEN
        RAISE EXCEPTION 'cannot roll back encrypted telegram sessions while rows exist: migration is irreversible for nonempty sessions';
    END IF;
END
$$;
-- +goose StatementEnd

ALTER TABLE sessions
    DROP CONSTRAINT ck_sessions_format_version,
    DROP CONSTRAINT ck_sessions_key_id,
    DROP CONSTRAINT ck_sessions_nonce,
    DROP CONSTRAINT ck_sessions_ciphertext,
    DROP COLUMN format_version,
    DROP COLUMN key_id,
    DROP COLUMN nonce,
    DROP COLUMN ciphertext,
    ADD COLUMN session varchar(255) NOT NULL;
