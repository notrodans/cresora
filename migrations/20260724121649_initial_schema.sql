-- +goose Up
CREATE TYPE telegram_peer_type AS ENUM (
    'user',
    'chat',
    'channel'
);

CREATE TYPE shared_dialog_kind AS ENUM (
    'supergroup',
    'broadcast_channel'
);

CREATE TYPE membership_status_type AS ENUM (
    'unknown',
    'not_joined',
    'joined',
    'left',
    'banned'
);

CREATE TYPE proxy_protocol AS ENUM (
    'socks5',
    'http'
);

CREATE TABLE proxy_credentials (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    username varchar(255) NOT NULL,
    password varchar(255) NOT NULL,
    CONSTRAINT uq_proxy_credentials_unique_auth UNIQUE (username, PASSWORD)
);

CREATE TABLE proxies (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    protocol proxy_protocol NOT NULL,
    ip inet NOT NULL,
    port integer NOT NULL,
    credential_id uuid NOT NULL,
    CONSTRAINT fk_proxies_credential FOREIGN KEY (credential_id) REFERENCES proxy_credentials (id) ON DELETE CASCADE,
    CONSTRAINT ck_proxies_port CHECK (port BETWEEN 1 AND 65535),
    CONSTRAINT uq_unique_proxy UNIQUE (ip, port)
);

CREATE TABLE operators (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    username varchar(255) NOT NULL,
    password varchar(255) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tokens_invalid_before timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_operators_username UNIQUE (username)
);

CREATE TABLE operator_accounts (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    operator_id uuid NOT NULL,
    -- Оставляй NOT NULL + UNIQUE, только если это внутренний
    -- username оператора, а не изменяемый Telegram username.
    phone varchar(20) NOT NULL,
    telegram_username varchar(32) NOT NULL,
    telegram_first_name varchar(64) NOT NULL,
    telegram_last_name varchar(64),
    api_id integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used timestamptz DEFAULT NULL,
    CONSTRAINT fk_operator_accounts_operator FOREIGN KEY (operator_id) REFERENCES operators (id) ON DELETE CASCADE,
    CONSTRAINT ck_operator_accounts_first_name CHECK (length(telegram_first_name) >= 1),
    CONSTRAINT ck_operator_accounts_phone CHECK (phone ~ '^\+[1-9]\d{1,14}$')
);

CREATE TABLE operator_accounts_proxies (
    account_id uuid NOT NULL,
    proxy_id uuid NOT NULL,
    CONSTRAINT pk_operator_accounts_proxies PRIMARY KEY (account_id, proxy_id),
    CONSTRAINT fk_operator_accounts_proxies_account FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE CASCADE,
    CONSTRAINT fk_operator_accounts_proxies_proxy FOREIGN KEY (proxy_id) REFERENCES proxies (id) ON DELETE CASCADE
);

CREATE TABLE sessions (
    account_id uuid NOT NULL,
    session varchar(255) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_sessions PRIMARY KEY (account_id),
    CONSTRAINT fk_sessions_account FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE CASCADE
);

CREATE TABLE telegram_shared_dialogs (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    -- Все shared-диалоги являются Telegram Channel peer:
    -- либо supergroup, либо broadcast channel.
    telegram_peer_id bigint NOT NULL,
    dialog_kind shared_dialog_kind NOT NULL,
    title varchar(255) NOT NULL,
    canonical_username varchar(32),
    participants_count integer,
    metadata_synced_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_telegram_shared_dialogs_peer UNIQUE (telegram_peer_id),
    CONSTRAINT ck_telegram_shared_dialog_participants_nonnegative CHECK (participants_count IS NULL OR participants_count >= 0)
);

CREATE TABLE operator_accounts_shared_dialogs (
    account_id uuid NOT NULL,
    shared_dialog_id uuid NOT NULL,
    -- Может быть ещё не получен при частичной синхронизации.
    access_hash bigint,
    membership_status membership_status_type NOT NULL DEFAULT 'unknown',
    last_joined_at timestamptz,
    can_send boolean NOT NULL DEFAULT FALSE,
    last_synced_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_operator_accounts_shared_dialogs PRIMARY KEY (account_id, shared_dialog_id),
    CONSTRAINT fk_operator_accounts_shared_dialogs_account FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE CASCADE,
    CONSTRAINT fk_operator_accounts_shared_dialogs_dialog FOREIGN KEY (shared_dialog_id) REFERENCES telegram_shared_dialogs (id) ON DELETE CASCADE,
    CONSTRAINT ck_operator_account_shared_dialog_joined_at CHECK (membership_status <> 'joined' OR last_joined_at IS NOT NULL)
);

-- PK начинается с account_id, поэтому для операций по shared_dialog_id
-- нужен отдельный индекс.
CREATE INDEX ix_operator_accounts_shared_dialogs_shared_dialog ON operator_accounts_shared_dialogs (shared_dialog_id);

CREATE TABLE operator_accounts_private_dialogs (
    account_id uuid NOT NULL,
    -- Нужен, потому что user/chat/channel имеют разные пространства ID.
    peer_type telegram_peer_type NOT NULL,
    telegram_peer_id bigint NOT NULL,
    title varchar(255) NOT NULL,
    username varchar(32),
    access_hash bigint,
    -- Для user peer эти поля могут быть неприменимы.
    membership_status membership_status_type,
    last_joined_at timestamptz,
    can_send boolean NOT NULL DEFAULT FALSE,
    last_synced_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_operator_accounts_private_dialogs PRIMARY KEY (account_id, peer_type, telegram_peer_id),
    CONSTRAINT fk_operator_accounts_private_dialogs_account FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE CASCADE
);

CREATE TYPE mailing_status_type AS ENUM (
    'draft',
    'queued',
    'running',
    'paused',
    'stopped',
    'completed',
    'failed'
);

CREATE TYPE mailing_send_mode_type AS ENUM (
    'sequential',
    'parallel'
);

CREATE TYPE mailing_repeat_mode_type AS ENUM (
    'once',
    'rounds'
);

CREATE TABLE mailings (
    id uuid PRIMARY KEY DEFAULT uuidv7 (),
    operator_id uuid NOT NULL,
    name varchar(255) NOT NULL,
    message_text text NOT NULL,
    status mailing_status_type NOT NULL DEFAULT 'draft',
    send_mode mailing_send_mode_type NOT NULL DEFAULT 'parallel',
    repeat_mode mailing_repeat_mode_type NOT NULL DEFAULT 'once',
    round_delay interval NOT NULL DEFAULT INTERVAL '1 second',
    recipient_delay interval NOT NULL DEFAULT INTERVAL '30 seconds',
    post_join_delay interval NOT NULL DEFAULT INTERVAL '2 seconds',
    preflight_check boolean NOT NULL DEFAULT TRUE,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_mailings_operator FOREIGN KEY (operator_id) REFERENCES operators (id) ON DELETE CASCADE,
    CONSTRAINT ck_mailings_name_not_empty CHECK (btrim(name) <> ''),
    CONSTRAINT ck_mailings_message_not_empty CHECK (btrim(message_text) <> ''),
    CONSTRAINT ck_mailings_round_delay_nonnegative CHECK (round_delay >= interval '0 seconds'),
    CONSTRAINT ck_mailings_recipient_delay_nonnegative CHECK (recipient_delay >= interval '0 seconds'),
    CONSTRAINT ck_mailings_post_join_delay_nonnegative CHECK (post_join_delay >= interval '0 seconds')
);

CREATE INDEX ix_mailings_operator_status ON mailings (operator_id, status);

CREATE TYPE mailing_schedule_mode_type AS ENUM (
    'always',
    'window'
);

CREATE TABLE mailing_schedules (
    mailing_id uuid PRIMARY KEY,
    mode mailing_schedule_mode_type NOT NULL DEFAULT 'always',
    starts_at timestamptz,
    ends_at timestamptz,
    daily_until time,
    timezone varchar(64) NOT NULL DEFAULT 'UTC',
    CONSTRAINT fk_mailing_schedules_mailing FOREIGN KEY (mailing_id) REFERENCES mailings (id) ON DELETE CASCADE,
    CONSTRAINT ck_mailing_schedules_range CHECK (starts_at IS NULL OR ends_at IS NULL OR starts_at < ends_at),
    CONSTRAINT ck_mailing_schedules_always CHECK (mode <> 'always' OR (starts_at IS NULL AND ends_at IS NULL AND daily_until IS NULL))
);

CREATE TABLE telegram_mailing_routes (
    mailing_id uuid PRIMARY KEY,
    account_id uuid NOT NULL,
    CONSTRAINT fk_telegram_mailing_routes_mailing FOREIGN KEY (mailing_id) REFERENCES mailings (id) ON DELETE CASCADE,
    CONSTRAINT fk_telegram_mailing_routes_account FOREIGN KEY (account_id) REFERENCES operator_accounts (id) ON DELETE RESTRICT
);

CREATE INDEX ix_telegram_mailing_routes_account ON telegram_mailing_routes (account_id);

CREATE TABLE mailing_recipients (
    mailing_id uuid NOT NULL,
    id uuid NOT NULL DEFAULT uuidv7 (),
    position integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mailing_recipients PRIMARY KEY (mailing_id, id),
    CONSTRAINT fk_mailing_recipients_mailing FOREIGN KEY (mailing_id) REFERENCES mailings (id) ON DELETE CASCADE,
    CONSTRAINT uq_mailing_recipients_position UNIQUE (mailing_id, position),
    CONSTRAINT ck_mailing_recipients_position_positive CHECK (position >= 0)
);

CREATE TABLE telegram_mailing_recipients (
    mailing_id uuid NOT NULL,
    recipient_id uuid NOT NULL,
    shared_dialog_id uuid,
    private_account_id uuid,
    private_peer_type telegram_peer_type,
    private_peer_id bigint,
    CONSTRAINT pk_telegram_mailing_recipients PRIMARY KEY (mailing_id, recipient_id),
    CONSTRAINT fk_telegram_mailing_recipients_recipient FOREIGN KEY (mailing_id, recipient_id) REFERENCES mailing_recipients (mailing_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_telegram_mailing_recipients_shared FOREIGN KEY (shared_dialog_id) REFERENCES telegram_shared_dialogs (id) ON DELETE CASCADE,
    CONSTRAINT fk_telegram_mailing_recipients_private FOREIGN KEY (private_account_id, private_peer_type, private_peer_id) REFERENCES operator_accounts_private_dialogs (account_id, peer_type, telegram_peer_id) ON DELETE CASCADE,
    CONSTRAINT ck_telegram_mailing_recipient_target CHECK ((shared_dialog_id IS NOT NULL AND private_account_id IS NULL AND private_peer_type IS NULL AND private_peer_id IS NULL) OR (shared_dialog_id IS NULL AND private_account_id IS NOT NULL AND private_peer_type IS NOT NULL AND private_peer_id IS NOT NULL))
);

CREATE INDEX ix_telegram_mailing_recipients_shared ON telegram_mailing_recipients (shared_dialog_id)
WHERE
    shared_dialog_id IS NOT NULL;

CREATE INDEX ix_telegram_mailing_recipients_private ON telegram_mailing_recipients (private_account_id, private_peer_type, private_peer_id)
WHERE
    private_account_id IS NOT NULL;

CREATE TYPE mailing_run_status_type AS ENUM (
    'queued',
    'running',
    'completed',
    'cancelled',
    'failed'
);

CREATE TABLE mailing_runs (
    mailing_id uuid NOT NULL,
    id uuid NOT NULL DEFAULT uuidv7 (),
    number integer NOT NULL,
    status mailing_run_status_type NOT NULL DEFAULT 'queued',
    queued_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mailing_runs PRIMARY KEY (id),
    CONSTRAINT uq_mailing_runs UNIQUE (mailing_id, id),
    CONSTRAINT fk_mailing_runs_mailing FOREIGN KEY (mailing_id) REFERENCES mailings (id) ON DELETE CASCADE,
    CONSTRAINT uq_mailing_runs_number UNIQUE (mailing_id, number),
    CONSTRAINT ck_mailing_runs_number_positive CHECK (number >= 1),
    CONSTRAINT ck_mailing_runs_started CHECK (started_at IS NULL OR started_at >= queued_at),
    CONSTRAINT ck_mailing_runs_finished CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND finished_at >= started_at))
);

CREATE INDEX ix_mailing_runs_status ON mailing_runs (status, queued_at);

CREATE TYPE mailing_delivery_status_type AS ENUM (
    'pending',
    'sending',
    'sent',
    'skipped',
    'failed'
);

CREATE TABLE mailing_deliveries (
    mailing_id uuid NOT NULL,
    run_id uuid NOT NULL,
    recipient_id uuid NOT NULL,
    status mailing_delivery_status_type NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    ready_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at timestamptz,
    sent_at timestamptz,
    skip_reason varchar(255),
    error_message text,
    lease_token uuid,
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mailing_deliveries PRIMARY KEY (mailing_id, run_id, recipient_id),
    CONSTRAINT fk_mailing_deliveries_run FOREIGN KEY (mailing_id, run_id) REFERENCES mailing_runs (mailing_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_mailing_deliveries_recipient FOREIGN KEY (mailing_id, recipient_id) REFERENCES mailing_recipients (mailing_id, id) ON DELETE CASCADE,
    CONSTRAINT ck_mailing_deliveries_attempt_nonnegative CHECK (attempt_count >= 0),
    CONSTRAINT ck_mailing_deliveries_sent CHECK (status <> 'sent' OR sent_at IS NOT NULL),
    CONSTRAINT ck_mailing_deliveries_skipped CHECK (status <> 'skipped' OR skip_reason IS NOT NULL),
    CONSTRAINT ck_mailing_deliveries_attempts CHECK (attempt_count >= 0 AND max_attempts >= 1),
    CONSTRAINT ck_mailing_deliveries_lease CHECK ((lease_until IS NULL) = (lease_token IS NULL))
);

CREATE INDEX ix_mailing_deliveries_pending ON mailing_deliveries (ready_at, mailing_id, run_id)
WHERE
    status = 'pending';

CREATE INDEX ix_mailing_deliveries_recipient ON mailing_deliveries (mailing_id, recipient_id);

CREATE INDEX ix_mailing_deliveries_claim ON mailing_deliveries (ready_at, mailing_id, run_id, recipient_id)
WHERE
    status IN ('pending', 'sending');

CREATE TABLE telegram_mailing_deliveries (
    mailing_id uuid NOT NULL,
    run_id uuid NOT NULL,
    recipient_id uuid NOT NULL,
    random_id bigint NOT NULL,
    telegram_message_id bigint,
    CONSTRAINT pk_telegram_mailing_deliveries PRIMARY KEY (mailing_id, run_id, recipient_id),
    CONSTRAINT fk_telegram_mailing_deliveries_delivery FOREIGN KEY (mailing_id, run_id, recipient_id) REFERENCES mailing_deliveries (mailing_id, run_id, recipient_id) ON DELETE CASCADE,
    CONSTRAINT uq_telegram_mailing_deliveries_random UNIQUE (random_id),
    CONSTRAINT ck_telegram_mailing_deliveries_random_nonzero CHECK (random_id <> 0)
);

-- Telegram usernames регистронезависимы.
CREATE UNIQUE INDEX uq_telegram_shared_dialogs_canonical_username ON telegram_shared_dialogs (lower(canonical_username))
WHERE
    canonical_username IS NOT NULL;

CREATE TABLE telegram_subscription_requirements (
    target_dialog_id uuid NOT NULL,
    required_dialog_id uuid NOT NULL,
    CONSTRAINT pk_telegram_subscription_requirements PRIMARY KEY (target_dialog_id, required_dialog_id),
    CONSTRAINT fk_subscription_requirements_target FOREIGN KEY (target_dialog_id) REFERENCES telegram_shared_dialogs (id) ON DELETE CASCADE,
    CONSTRAINT fk_subscription_requirements_required FOREIGN KEY (required_dialog_id) REFERENCES telegram_shared_dialogs (id) ON DELETE CASCADE,
    CONSTRAINT ck_subscription_requirement_not_self CHECK (target_dialog_id <> required_dialog_id)
);

-- PK индекс начинается с target_dialog_id.
-- Для каскадного удаления по required_dialog_id нужен отдельный индекс.
CREATE INDEX ix_subscription_requirements_required_dialog ON telegram_subscription_requirements (required_dialog_id);
