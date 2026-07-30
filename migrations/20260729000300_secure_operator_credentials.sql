-- +goose Up

-- Operator passwords are deliberately not migrated. Existing operators are
-- left unprovisioned and cannot authenticate until the local bootstrap tool
-- writes a password_hash. Goose runs this forward-only migration in one
-- transaction, so the credential cutover and legacy bearer-token revocation
-- are atomic.
ALTER TABLE operators
    DROP COLUMN password,
    ADD COLUMN password_hash text,
    ADD COLUMN password_changed_at timestamptz,
    ADD CONSTRAINT ck_operators_password_hash_state CHECK (
        (password_hash IS NULL AND password_changed_at IS NULL)
        OR (password_hash IS NOT NULL AND password_changed_at IS NOT NULL)
    ),
    ADD CONSTRAINT ck_operators_password_hash_format CHECK (
        password_hash IS NULL OR (
            length(password_hash) BETWEEN 64 AND 512
            AND password_hash ~ $phc$^\$argon2id\$v=19\$m=[1-9][0-9]*,t=[1-9][0-9]*,p=[1-9][0-9]*\$[A-Za-z0-9+/]{11,86}\$[A-Za-z0-9+/]{22,86}$$phc$
        )
    );

-- clock_timestamp() is evaluated by PostgreSQL for each row update. The
-- update therefore waits for the row lock acquired by the destructive
-- cutover and advances every existing operator's bearer-token boundary while
-- retaining the operator and its related rows.
UPDATE operators
SET tokens_invalid_before = GREATEST(tokens_invalid_before, clock_timestamp());
