// Package dialogsync provides the PostgreSQL-backed durable account dialog
// synchronization queue and the dialog projection writes for shared and
// private dialogs.
package dialogsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/dialogsync"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// Store implements dialogsync.Store over PostgreSQL.
type Store struct {
	database *pgxpool.Pool
}

var (
	_ dialogsync.Store = (*Store)(nil)
)

// New constructs a PostgreSQL-backed dialog sync store.
func New(database *pgxpool.Pool) *Store {
	return &Store{database: database}
}

// Claim leases at most one claimable account dialog sync using FOR UPDATE
// SKIP LOCKED and returns a live task handle.
func (store *Store) Claim(
	ctx context.Context,
	lease time.Duration,
) (dialogsync.Task, error) {
	if lease <= 0 {
		return nil, errors.New("claim account dialog sync with invalid lease")
	}
	token := uuid.New()
	seconds := lease.Seconds()

	var (
		accountID  uuid.UUID
		operatorID uuid.UUID
		version    int64
		leaseToken uuid.UUID
		generation int64
	)
	failure := store.database.QueryRow(
		ctx,
		`WITH candidate AS (
		    SELECT sync.account_id,
		           account.operator_id,
		           account.status_version
		    FROM account_dialog_syncs AS sync
		    JOIN operator_accounts AS account
		      ON account.id = sync.account_id
		    WHERE sync.status = 'pending'
		      AND (sync.next_retry_at IS NULL OR sync.next_retry_at <= CURRENT_TIMESTAMP)
		      AND sync.attempt_count < sync.max_attempts
		      AND (sync.lease_until IS NULL OR sync.lease_until < CURRENT_TIMESTAMP)
		      AND account.status = 'active'
		    ORDER BY sync.needs_sync_at, sync.account_id
		    FOR UPDATE OF sync SKIP LOCKED
		    LIMIT 1
		), claimed AS (
		    UPDATE account_dialog_syncs AS sync
		    SET status = 'running',
		        lease_until = CURRENT_TIMESTAMP + ($1::double precision * INTERVAL '1 second'),
		        lease_token = $2,
		        lease_generation = COALESCE(sync.lease_generation, 0) + 1,
		        attempt_count = sync.attempt_count + 1,
		        next_retry_at = NULL,
		        updated_at = CURRENT_TIMESTAMP
		    FROM candidate
		    WHERE sync.account_id = candidate.account_id
		    RETURNING sync.account_id,
		              sync.lease_token,
		              sync.lease_generation
		)
		SELECT claimed.account_id,
		       candidate.operator_id,
		       candidate.status_version,
		       claimed.lease_token,
		       claimed.lease_generation
		  FROM claimed
		  JOIN candidate ON candidate.account_id = claimed.account_id`,
		seconds,
		token,
	).Scan(&accountID, &operatorID, &version, &leaseToken, &generation)
	if errors.Is(failure, pgx.ErrNoRows) {
		return nil, dialogsync.ErrEmpty
	}
	if failure != nil {
		return nil, fmt.Errorf("claim account dialog sync: %w", failure)
	}
	return &task{
		store:      store,
		accountID:  accountID,
		operatorID: operatorID,
		version:    version,
		leaseToken: leaseToken,
		generation: generation,
	}, nil
}

// Backfill creates a pending sync row for every active account that lacks one.
func (store *Store) Backfill(ctx context.Context) (int, error) {
	tag, failure := store.database.Exec(
		ctx,
		`INSERT INTO account_dialog_syncs (account_id)
		 SELECT account.id
		   FROM operator_accounts AS account
		  WHERE account.status = 'active'
		    AND NOT EXISTS (
		        SELECT 1 FROM account_dialog_syncs s WHERE s.account_id = account.id
		    )
		 ON CONFLICT (account_id) DO NOTHING`,
	)
	if failure != nil {
		return 0, fmt.Errorf("backfill account dialog syncs: %w", failure)
	}
	return int(tag.RowsAffected()), nil
}

// task is one leased account dialog synchronization.
type task struct {
	store *Store

	accountID  uuid.UUID
	operatorID uuid.UUID
	version    int64
	leaseToken uuid.UUID
	generation int64
}

var (
	_ dialogsync.Task = (*task)(nil)
)

func (t *task) Key() dialogsync.TaskKey {
	return dialogsync.TaskKey{
		AccountID:  t.accountID,
		OperatorID: t.operatorID,
		Version:    t.version,
	}
}

func (t *task) Revalidate(
	ctx context.Context,
) (operatoraccounts.RuntimeTarget, error) {
	var (
		operatorID uuid.UUID
		status     string
		version    int64
	)
	failure := t.store.database.QueryRow(
		ctx,
		`SELECT account.operator_id,
		        account.status::text,
		        account.status_version
		 FROM operator_accounts AS account
		 JOIN account_dialog_syncs AS sync
		   ON sync.account_id = account.id
		  AND sync.lease_token = $2
		 WHERE account.id = $1
		   AND account.status = 'active'
		   AND sync.status = 'running'
		   AND sync.lease_generation = $3`,
		t.accountID,
		t.leaseToken,
		t.generation,
	).Scan(&operatorID, &status, &version)
	if errors.Is(failure, pgx.ErrNoRows) {
		return operatoraccounts.RuntimeTarget{}, operatoraccounts.ErrAccountNotFound
	}
	if failure != nil {
		return operatoraccounts.RuntimeTarget{}, fmt.Errorf("revalidate account dialog sync: %w", failure)
	}
	if status != string(operatoraccount.StatusActive) || operatorID != t.operatorID || version <= 0 {
		return operatoraccounts.RuntimeTarget{}, operatoraccounts.ErrAccountNotFound
	}
	return operatoraccounts.RuntimeTarget{
		Actor:     application.Actor{OperatorID: operatorID},
		AccountID: operatoraccount.Identity(t.accountID),
		Status:    operatoraccount.StatusActive,
		Version:   operatoraccount.Version(version),
	}, nil
}

func (t *task) Renew(ctx context.Context, lease time.Duration) error {
	tag, failure := t.store.database.Exec(
		ctx,
		`UPDATE account_dialog_syncs
		 SET lease_until = CURRENT_TIMESTAMP + ($1::double precision * INTERVAL '1 second'),
		     updated_at = CURRENT_TIMESTAMP
		 WHERE account_id = $2
		   AND status = 'running'
		   AND lease_token = $3
		   AND lease_generation = $4
		   AND lease_until > CURRENT_TIMESTAMP`,
		lease.Seconds(),
		t.accountID,
		t.leaseToken,
		t.generation,
	)
	if failure != nil {
		return fmt.Errorf("renew account dialog sync lease: %w", failure)
	}
	if tag.RowsAffected() == 0 {
		return dialogsync.ErrLeaseLost
	}
	return nil
}

// Complete persists the fetched shared and private dialogs and atomically
// marks the sync done.
func (t *task) Complete(
	ctx context.Context,
	shared []dialogsync.SharedDialog,
	private []dialogsync.PrivateDialog,
) error {
	transaction, failure := t.store.database.Begin(ctx)
	if failure != nil {
		return fmt.Errorf("begin account dialog sync completion: %w", failure)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	for _, dialog := range shared {
		var sharedID uuid.UUID
		failure = transaction.QueryRow(
			ctx,
			`INSERT INTO telegram_shared_dialogs (
			     telegram_peer_id, dialog_kind, title, canonical_username,
			     participants_count, metadata_synced_at
			 )
			 VALUES ($1, $2::shared_dialog_kind, $3, $4, $5, CURRENT_TIMESTAMP)
			 ON CONFLICT (telegram_peer_id) DO UPDATE
			 SET title = EXCLUDED.title,
			     canonical_username = EXCLUDED.canonical_username,
			     participants_count = EXCLUDED.participants_count,
			     metadata_synced_at = CURRENT_TIMESTAMP,
			     updated_at = CURRENT_TIMESTAMP
			 RETURNING id`,
			dialog.PeerID,
			string(dialog.Kind),
			dialog.Title,
			nullableText(dialog.Username),
			optionalIntValue(dialog.Participants),
		).Scan(&sharedID)
		if failure != nil {
			return fmt.Errorf("upsert shared telegram dialog: %w", failure)
		}
		if _, failure = transaction.Exec(
			ctx,
			`INSERT INTO operator_accounts_shared_dialogs (
			     account_id, shared_dialog_id, access_hash, membership_status,
			     last_joined_at, can_send, last_synced_at
			 )
			 VALUES ($1, $2, $3, 'joined', CURRENT_TIMESTAMP, FALSE, CURRENT_TIMESTAMP)
			 ON CONFLICT (account_id, shared_dialog_id) DO UPDATE
			 SET access_hash = COALESCE(EXCLUDED.access_hash, operator_accounts_shared_dialogs.access_hash),
			     membership_status = 'joined',
			     last_joined_at = COALESCE(operator_accounts_shared_dialogs.last_joined_at, CURRENT_TIMESTAMP),
			     can_send = FALSE,
			     last_synced_at = CURRENT_TIMESTAMP,
			     updated_at = CURRENT_TIMESTAMP`,
			t.accountID,
			sharedID,
			optionalInt64Value(dialog.AccessHash),
		); failure != nil {
			return fmt.Errorf("upsert shared dialog membership: %w", failure)
		}
	}

	for _, dialog := range private {
		if _, failure = transaction.Exec(
			ctx,
			`INSERT INTO operator_accounts_private_dialogs (
			     account_id, peer_type, telegram_peer_id, title, username,
			     access_hash, membership_status, last_joined_at, can_send, last_synced_at
			 )
			 VALUES ($1, $2::telegram_peer_type, $3, $4, $5, $6, 'joined', CURRENT_TIMESTAMP, FALSE, CURRENT_TIMESTAMP)
			 ON CONFLICT (account_id, peer_type, telegram_peer_id) DO UPDATE
			 SET title = EXCLUDED.title,
			     username = EXCLUDED.username,
			     access_hash = COALESCE(EXCLUDED.access_hash, operator_accounts_private_dialogs.access_hash),
			     membership_status = 'joined',
			     last_joined_at = COALESCE(operator_accounts_private_dialogs.last_joined_at, CURRENT_TIMESTAMP),
			     can_send = FALSE,
			     last_synced_at = CURRENT_TIMESTAMP,
			     updated_at = CURRENT_TIMESTAMP`,
			t.accountID,
			string(dialog.PeerType),
			dialog.PeerID,
			dialog.Title,
			nullableText(dialog.Username),
			optionalInt64Value(dialog.AccessHash),
		); failure != nil {
			return fmt.Errorf("upsert private telegram dialog: %w", failure)
		}
	}

	if _, failure = transaction.Exec(
		ctx,
		`UPDATE account_dialog_syncs
		 SET status = 'done',
		     lease_token = NULL,
		     lease_until = NULL,
		     lease_generation = NULL,
		     next_retry_at = NULL,
		     last_error = NULL,
		     finished_at = CURRENT_TIMESTAMP,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE account_id = $1
		   AND lease_token = $2
		   AND lease_generation = $3
		   AND status = 'running'`,
		t.accountID,
		t.leaseToken,
		t.generation,
	); failure != nil {
		return fmt.Errorf("complete account dialog sync record: %w", failure)
	}
	if failure = transaction.Commit(ctx); failure != nil {
		return fmt.Errorf("commit account dialog sync completion: %w", failure)
	}
	return nil
}

func (t *task) Retry(ctx context.Context, cause error, delay time.Duration) error {
	return t.retryOrFail(ctx, cause, delay)
}

func (t *task) Fail(ctx context.Context, cause error) error {
	return t.retryOrFail(ctx, cause, 0)
}

func (t *task) retryOrFail(ctx context.Context, cause error, delay time.Duration) error {
	// A server-issued flood wait is never terminal: the remote explicitly says
	// when to retry, so the attempt budget does not apply to it. Only hard Fail
	// calls (zero delay) or an exhausted transient budget become terminal.
	var flood *dialogsync.FloodWaitError
	isFlood := errors.As(cause, &flood) && flood != nil && flood.RetryAfter() > 0
	terminal := delay <= 0 && !isFlood

	transaction, failure := t.store.database.Begin(ctx)
	if failure != nil {
		return fmt.Errorf("begin account dialog sync retry: %w", failure)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var (
		attempts int
		max      int
	)
	failure = transaction.QueryRow(
		ctx,
		`SELECT attempt_count, max_attempts
		 FROM account_dialog_syncs
		 WHERE account_id = $1
		   AND lease_token = $2
		   AND lease_generation = $3
		   AND status = 'running'
		  FOR UPDATE`,
		t.accountID,
		t.leaseToken,
		t.generation,
	).Scan(&attempts, &max)
	if errors.Is(failure, pgx.ErrNoRows) {
		return dialogsync.ErrLeaseLost
	}
	if failure != nil {
		return fmt.Errorf("lock account dialog sync retry: %w", failure)
	}
	if !terminal && (isFlood || attempts < max) {
		// A flood wait is not a real failure: it never consumes the attempt
		// budget, so a throttled account stays claimable instead of being
		// silently excluded by the attempt cap. Genuine transient attempts keep
		// their already-incremented budget and dead-letter at the cap.
		resetAttempts := isFlood
		if _, failure = transaction.Exec(
			ctx,
			`UPDATE account_dialog_syncs
			 SET status = 'pending',
			     next_retry_at = CURRENT_TIMESTAMP + ($4::double precision * INTERVAL '1 second'),
			     attempt_count = CASE WHEN $5 THEN 0 ELSE attempt_count END,
			     lease_token = NULL,
			     lease_until = NULL,
			     lease_generation = NULL,
			     last_error = $6,
			     updated_at = CURRENT_TIMESTAMP
			 WHERE account_id = $1
			   AND lease_token = $2
			   AND lease_generation = $3
			   AND status = 'running'`,
			t.accountID,
			t.leaseToken,
			t.generation,
			delay.Seconds(),
			resetAttempts,
			dialogsync.BoundedErrorMessage(cause),
		); failure != nil {
			return fmt.Errorf("requeue account dialog sync: %w", failure)
		}
	} else {
		if _, failure = transaction.Exec(
			ctx,
			`UPDATE account_dialog_syncs
			 SET status = 'failed',
			     lease_token = NULL,
			     lease_until = NULL,
			     lease_generation = NULL,
			     last_error = $4,
			     finished_at = CURRENT_TIMESTAMP,
			     updated_at = CURRENT_TIMESTAMP
			 WHERE account_id = $1
			   AND lease_token = $2
			   AND lease_generation = $3
			   AND status = 'running'`,
			t.accountID,
			t.leaseToken,
			t.generation,
			dialogsync.BoundedErrorMessage(cause),
		); failure != nil {
			return fmt.Errorf("fail account dialog sync: %w", failure)
		}
	}
	if failure = transaction.Commit(ctx); failure != nil {
		return fmt.Errorf("commit account dialog sync retry: %w", failure)
	}
	return nil
}

func (t *task) Release(ctx context.Context, cause error) error {
	_, failure := t.store.database.Exec(
		ctx,
		`UPDATE account_dialog_syncs
		 SET status = 'pending',
		     next_retry_at = CURRENT_TIMESTAMP + INTERVAL '5 seconds',
		     lease_token = NULL,
		     lease_until = NULL,
		     lease_generation = NULL,
		     last_error = $4,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE account_id = $1
		   AND lease_token = $2
		   AND lease_generation = $3
		   AND status = 'running'`,
		t.accountID,
		t.leaseToken,
		t.generation,
		dialogsync.BoundedErrorMessage(cause),
	)
	if failure != nil {
		return fmt.Errorf("release account dialog sync: %w", failure)
	}
	return nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func optionalIntValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func optionalInt64Value(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
