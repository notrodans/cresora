-- +goose Up

-- Browser authentication is opaque. Only a fixed-width SHA-256 digest is
-- persisted; bearer and request metadata are intentionally not part of this
-- schema.
-- The PostgreSQL adapter enforces a maximum of five live rows per operator
-- while holding the operator row lock; storing a caller-controlled limit would
-- make that security bound mutable.
ALTER TABLE operators
    ADD COLUMN enabled boolean NOT NULL DEFAULT TRUE;

CREATE TABLE operator_web_sessions (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    operator_id uuid NOT NULL,
    token_hash bytea NOT NULL,
    created_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    idle_expires_at timestamptz NOT NULL,
    absolute_expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    CONSTRAINT fk_operator_web_sessions_operator
        FOREIGN KEY (operator_id) REFERENCES operators (id) ON DELETE CASCADE,
    CONSTRAINT uq_operator_web_sessions_token_hash UNIQUE (token_hash),
    CONSTRAINT ck_operator_web_sessions_token_hash_length
        CHECK (octet_length(token_hash) = 32),
    CONSTRAINT ck_operator_web_sessions_expiry_order
        CHECK (created_at <= last_seen_at
            AND last_seen_at <= idle_expires_at
            AND idle_expires_at <= absolute_expires_at)
);

CREATE INDEX ix_operator_web_sessions_operator_live
    ON operator_web_sessions (operator_id, last_seen_at DESC, created_at DESC)
    WHERE revoked_at IS NULL;

CREATE INDEX ix_operator_web_sessions_expiry
    ON operator_web_sessions (idle_expires_at, absolute_expires_at)
    WHERE revoked_at IS NULL;

-- Keep the physical revocation marker aligned with the credential boundary as
-- well as the validation predicate. This trigger is installed after the
-- credential cutover migration, so its first invocation cannot refer to a
-- missing session table.
-- +goose StatementBegin
CREATE FUNCTION revoke_operator_web_sessions_after_credential_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tokens_invalid_before > OLD.tokens_invalid_before THEN
        UPDATE operator_web_sessions
        SET revoked_at = clock_timestamp()
        WHERE operator_id = NEW.id
          AND revoked_at IS NULL;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER operators_revoke_web_sessions_after_credential_change
AFTER UPDATE OF tokens_invalid_before ON operators
FOR EACH ROW
EXECUTE FUNCTION revoke_operator_web_sessions_after_credential_change();
