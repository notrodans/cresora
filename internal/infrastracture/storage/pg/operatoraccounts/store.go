package operatoraccounts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

type database interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

var (
	_ applicationoperatoraccounts.AccountLifecycleRepository = (*Store)(nil)
	_ applicationoperatoraccounts.RemoteLogoutIntentLister   = (*Store)(nil)
	_ applicationoperatoraccounts.SessionDeleter             = (*Store)(nil)
	_ applicationoperatoraccounts.ForceForgetPersistence     = (*Store)(nil)
)

// Store persists operator account lifecycle state and its associated Telegram
// session ownership operations. Lifecycle changes are optimistic-concurrency
// updates; PostgreSQL owns the transition and version check so concurrent
// application replicas cannot both advance the same snapshot.
type Store struct {
	database database
}

// New creates a PostgreSQL-backed operator account store.
func New(database *pgxpool.Pool) *Store {
	return &Store{database: database}
}

// LoadAccount loads one account only when the actor owns it. Unknown and
// foreign account IDs intentionally return the same application error.
func (store *Store) LoadAccount(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
) (operatoraccount.Account, error) {
	if !validOwnership(actor, accountID) {
		return operatoraccount.Account{}, applicationoperatoraccounts.ErrAccountNotFound
	}

	var (
		id                   uuid.UUID
		status               string
		version              int64
		failureCode          pgtype.Text
		telegramUserID       pgtype.Int8
		authExpiresAt        pgtype.Timestamptz
		remoteLogoutRequired bool
	)
	failure := store.database.QueryRow(
		context,
		`SELECT account.id,
		        account.status::text,
		        account.status_version,
		        account.failure_code,
		        account.telegram_user_id,
		        account.auth_expires_at,
		        account.remote_logout_required
		 FROM operator_accounts AS account
		 WHERE account.operator_id = $1
		   AND account.id = $2`,
		actor.OperatorID,
		accountID.UUID(),
	).Scan(
		&id,
		&status,
		&version,
		&failureCode,
		&telegramUserID,
		&authExpiresAt,
		&remoteLogoutRequired,
	)
	if errors.Is(failure, pgx.ErrNoRows) {
		return operatoraccount.Account{}, applicationoperatoraccounts.ErrAccountNotFound
	}
	if failure != nil {
		return operatoraccount.Account{}, fmt.Errorf("load operator account: %w", failure)
	}
	if version <= 0 {
		return operatoraccount.Account{}, fmt.Errorf("load operator account: %w", operatoraccount.ErrInvalidState)
	}
	if id == uuid.Nil {
		return operatoraccount.Account{}, fmt.Errorf("load operator account: %w", operatoraccount.ErrInvalidState)
	}

	var failureValue operatoraccount.FailureCode
	if failureCode.Valid {
		failureValue = operatoraccount.FailureCode(failureCode.String)
	}
	var identity int64
	if telegramUserID.Valid {
		identity = telegramUserID.Int64
	}
	var expiry time.Time
	if authExpiresAt.Valid {
		expiry = authExpiresAt.Time
	}
	restored, failure := operatoraccount.Restore(
		operatoraccount.Identity(id),
		operatoraccount.Status(status),
		operatoraccount.Version(version),
		failureValue,
		identity,
		expiry,
		remoteLogoutRequired,
	)
	if failure != nil {
		return operatoraccount.Account{}, fmt.Errorf("load operator account: %w", failure)
	}
	return restored, nil
}

// ListRemoteLogoutIntents returns only actor-scoped snapshots whose durable
// remote logout intent still requires runtime reconciliation. Authentication
// abort candidates are intentionally not included here.
func (store *Store) ListRemoteLogoutIntents(
	context context.Context,
) ([]applicationoperatoraccounts.RuntimeTarget, error) {
	rows, failure := store.database.Query(
		context,
		`SELECT operator_id, id, status::text, status_version
		 FROM operator_accounts
		 WHERE status = 'disconnecting'
		   AND remote_logout_required = TRUE
		 ORDER BY created_at, id`,
	)
	if failure != nil {
		return nil, fmt.Errorf("list operator account remote logout intents: %w", failure)
	}
	defer rows.Close()

	targets := make([]applicationoperatoraccounts.RuntimeTarget, 0)
	for rows.Next() {
		var (
			operatorID uuid.UUID
			accountID  uuid.UUID
			status     string
			version    int64
		)
		if failure = rows.Scan(&operatorID, &accountID, &status, &version); failure != nil {
			return nil, fmt.Errorf("scan operator account remote logout intent: %w", failure)
		}
		if operatorID == uuid.Nil || accountID == uuid.Nil || version <= 0 || status != string(operatoraccount.StatusDisconnecting) {
			return nil, fmt.Errorf("scan operator account remote logout intent: %w", operatoraccount.ErrInvalidState)
		}
		targets = append(targets, applicationoperatoraccounts.RuntimeTarget{
			Actor:     application.Actor{OperatorID: operatorID},
			AccountID: operatoraccount.Identity(accountID),
			Status:    operatoraccount.Status(status),
			Version:   operatoraccount.Version(version),
		})
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("list operator account remote logout intents: %w", failure)
	}
	return targets, nil
}

// PersistLifecycle atomically advances one valid domain transition. The
// expected status is derived from the requested transition because the
// application port supplies the expected version and the next domain
// snapshot. Every update is scoped by operator, account, expected status, and
// expected version. The database expression advances the version exactly once.
func (store *Store) PersistLifecycle(
	context context.Context,
	actor application.Actor,
	account operatoraccount.Account,
	expectedVersion operatoraccount.Version,
) error {
	if !validOwnership(actor, account.ID()) {
		return applicationoperatoraccounts.ErrAccountNotFound
	}
	if expectedVersion >= operatoraccount.Version(math.MaxInt64) || account.Version() != expectedVersion+1 {
		return applicationoperatoraccounts.ErrAccountVersionConflict
	}
	previousStatuses, ok := lifecyclePredecessors(account.Status())
	if !ok {
		return applicationoperatoraccounts.ErrAccountVersionConflict
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin operator account lifecycle transition: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var updatedID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`UPDATE operator_accounts AS account
		 SET status = $3::operator_account_status_type,
		     status_version = account.status_version + 1,
		     telegram_user_id = $4,
		     auth_expires_at = $5,
		     failure_code = $6,
		     remote_logout_required = $7,
		     updated_at = clock_timestamp()
		 WHERE account.operator_id = $1
		   AND account.id = $2
		   AND account.status IN (`+previousStatuses+`)
		   AND account.status_version = $8
		   AND (
		       $3::operator_account_status_type <> 'active'::operator_account_status_type
		       OR EXISTS (
		           SELECT 1
		           FROM sessions
		           WHERE sessions.account_id = account.id
		       )
		   )
		 RETURNING account.id`,
		actor.OperatorID,
		account.ID().UUID(),
		string(account.Status()),
		optionalTelegramUserID(account.TelegramUserID()),
		optionalAuthenticationExpiry(account.AuthExpiresAt()),
		optionalFailureCode(account.FailureCode()),
		account.RemoteLogoutRequired(),
		int64(expectedVersion),
	).Scan(&updatedID)
	if failure == nil {
		if account.Status() == operatoraccount.StatusDisconnected {
			if _, failure = transaction.Exec(
				context,
				`DELETE FROM sessions WHERE account_id = $1`,
				account.ID().UUID(),
			); failure != nil {
				return fmt.Errorf("delete operator account session during disconnect: %w", failure)
			}
		}
		if failure = transaction.Commit(context); failure != nil {
			return fmt.Errorf("commit operator account lifecycle transition: %w", failure)
		}
		return nil
	}
	if !errors.Is(failure, pgx.ErrNoRows) {
		return fmt.Errorf("persist operator account lifecycle: %w", failure)
	}

	var (
		currentStatus  string
		currentVersion int64
		sessionExists  bool
	)
	failure = transaction.QueryRow(
		context,
		`SELECT account.status::text,
		        account.status_version,
		        EXISTS (
		            SELECT 1
		            FROM sessions
		            WHERE sessions.account_id = account.id
		        )
		 FROM operator_accounts AS account
		 WHERE account.operator_id = $1
		   AND account.id = $2
		 FOR UPDATE`,
		actor.OperatorID,
		account.ID().UUID(),
	).Scan(&currentStatus, &currentVersion, &sessionExists)
	if errors.Is(failure, pgx.ErrNoRows) {
		return applicationoperatoraccounts.ErrAccountNotFound
	}
	if failure != nil {
		return fmt.Errorf("inspect operator account lifecycle conflict: %w", failure)
	}
	if account.Status() == operatoraccount.StatusActive &&
		currentStatus == string(operatoraccount.StatusAuthenticating) &&
		currentVersion == int64(expectedVersion) &&
		!sessionExists {
		return applicationoperatoraccounts.ErrSessionNotFound
	}
	return applicationoperatoraccounts.ErrAccountVersionConflict
}

// ForceForgetAlreadyApplied reports whether one actor/account idempotency key
// already has a durable force-forget event. It is a read-only lookup used to
// make retries return an already-applied result before another runtime stop.
func (store *Store) ForceForgetAlreadyApplied(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
	idempotencyKey uuid.UUID,
) (bool, error) {
	if !validForceForgetInput(actor, accountID, idempotencyKey) {
		return false, applicationoperatoraccounts.ErrAccountNotFound
	}

	var applied bool
	failure := store.database.QueryRow(
		context,
		`SELECT EXISTS (
			SELECT 1
			FROM operator_account_force_forget_events
			WHERE operator_id = $1
			  AND account_id = $2
			  AND idempotency_key = $3
		)`,
		actor.OperatorID,
		accountID.UUID(),
		idempotencyKey,
	).Scan(&applied)
	if failure != nil {
		return false, fmt.Errorf("check operator account force forget event: %w", failure)
	}
	return applied, nil
}

// PersistForceForget atomically applies the local override. The update is
// fenced by ownership, disconnecting status, remote logout intent, and the
// expected version. Session deletion and the fixed audit event are in the
// same transaction, so an audit failure rolls back both local effects.
func (store *Store) PersistForceForget(
	context context.Context,
	actor application.Actor,
	account operatoraccount.Account,
	expectedVersion operatoraccount.Version,
	idempotencyKey uuid.UUID,
) (bool, error) {
	if !validForceForgetInput(actor, account.ID(), idempotencyKey) {
		return false, applicationoperatoraccounts.ErrAccountNotFound
	}
	if applied, failure := store.ForceForgetAlreadyApplied(context, actor, account.ID(), idempotencyKey); failure != nil {
		return false, fmt.Errorf("check operator account force forget idempotency: %w", failure)
	} else if applied {
		return true, nil
	}
	if expectedVersion == 0 || expectedVersion >= operatoraccount.Version(math.MaxInt64) ||
		account.Status() != operatoraccount.StatusDisconnected ||
		account.Version() != expectedVersion+1 {
		return false, applicationoperatoraccounts.ErrAccountVersionConflict
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return false, fmt.Errorf("begin operator account force forget: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	applied, failure := forceForgetEventExists(context, transaction, actor, account.ID(), idempotencyKey)
	if failure != nil {
		return false, fmt.Errorf("check operator account force forget event in transaction: %w", failure)
	}
	if applied {
		if failure = transaction.Commit(context); failure != nil {
			return false, fmt.Errorf("commit duplicate operator account force forget: %w", failure)
		}
		return true, nil
	}

	var disconnectedID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`UPDATE operator_accounts AS account
		 SET status = 'disconnected',
		     status_version = account.status_version + 1,
		     auth_expires_at = NULL,
		     failure_code = NULL,
		     remote_logout_required = FALSE,
		     updated_at = clock_timestamp()
		 WHERE account.operator_id = $1
		   AND account.id = $2
		   AND account.status = 'disconnecting'
		   AND account.remote_logout_required = TRUE
		   AND account.status_version = $3
		 RETURNING account.id`,
		actor.OperatorID,
		account.ID().UUID(),
		int64(expectedVersion),
	).Scan(&disconnectedID)
	if errors.Is(failure, pgx.ErrNoRows) {
		applied, checkFailure := forceForgetEventExists(context, transaction, actor, account.ID(), idempotencyKey)
		if checkFailure != nil {
			return false, fmt.Errorf("recheck operator account force forget event: %w", checkFailure)
		}
		if applied {
			if failure = transaction.Commit(context); failure != nil {
				return false, fmt.Errorf("commit duplicate operator account force forget: %w", failure)
			}
			return true, nil
		}

		var exists bool
		failure = transaction.QueryRow(
			context,
			`SELECT EXISTS (
				SELECT 1
				FROM operator_accounts
				WHERE operator_id = $1
				  AND id = $2
			)`,
			actor.OperatorID,
			account.ID().UUID(),
		).Scan(&exists)
		if failure != nil {
			return false, fmt.Errorf("inspect operator account force forget target: %w", failure)
		}
		if !exists {
			return false, applicationoperatoraccounts.ErrAccountNotFound
		}
		return false, applicationoperatoraccounts.ErrAccountVersionConflict
	}
	if failure != nil {
		return false, fmt.Errorf("transition operator account for force forget: %w", failure)
	}

	if _, failure = transaction.Exec(
		context,
		`DELETE FROM sessions WHERE account_id = $1`,
		disconnectedID,
	); failure != nil {
		return false, fmt.Errorf("delete operator account session during force forget: %w", failure)
	}
	if _, failure = transaction.Exec(
		context,
		`INSERT INTO operator_account_force_forget_events (
			 event_type,
			 operator_id,
			 account_id,
			 previous_version,
			 resulting_version,
			 reason,
			 idempotency_key
		 ) VALUES (
			 'operator_account_force_forgotten',
			 $1,
			 $2,
			 $3,
			 $4,
			 'remote_logout_unverified_operator_override',
			 $5
		 )`,
		actor.OperatorID,
		account.ID().UUID(),
		int64(expectedVersion),
		int64(expectedVersion+1),
		idempotencyKey,
	); failure != nil {
		return false, fmt.Errorf("insert operator account force forget audit event: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return false, fmt.Errorf("commit operator account force forget: %w", failure)
	}
	return false, nil
}

func forceForgetEventExists(
	context context.Context,
	transaction pgx.Tx,
	actor application.Actor,
	accountID operatoraccount.ID,
	idempotencyKey uuid.UUID,
) (bool, error) {
	var applied bool
	failure := transaction.QueryRow(
		context,
		`SELECT EXISTS (
			SELECT 1
			FROM operator_account_force_forget_events
			WHERE operator_id = $1
			  AND account_id = $2
			  AND idempotency_key = $3
		)`,
		actor.OperatorID,
		accountID.UUID(),
		idempotencyKey,
	).Scan(&applied)
	return applied, failure
}

// DeleteSession removes an owned account's Telegram session. The account row
// is locked before deletion so this operation cannot race a lifecycle update.
// Deleting an already-missing session is intentionally successful.
func (store *Store) DeleteSession(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
) error {
	if !validOwnership(actor, accountID) {
		return applicationoperatoraccounts.ErrAccountNotFound
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin operator account session deletion: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var status string
	failure = transaction.QueryRow(
		context,
		`SELECT status::text
		 FROM operator_accounts
		 WHERE operator_id = $1
		   AND id = $2
		 FOR UPDATE`,
		actor.OperatorID,
		accountID.UUID(),
	).Scan(&status)
	if errors.Is(failure, pgx.ErrNoRows) {
		return applicationoperatoraccounts.ErrAccountNotFound
	}
	if failure != nil {
		return fmt.Errorf("check operator account session ownership: %w", failure)
	}
	if status == string(operatoraccount.StatusActive) {
		return applicationoperatoraccounts.ErrAccountStateConflict
	}
	if _, failure = transaction.Exec(
		context,
		`DELETE FROM sessions WHERE account_id = $1`,
		accountID.UUID(),
	); failure != nil {
		return fmt.Errorf("delete operator account session: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit operator account session deletion: %w", failure)
	}
	return nil
}

func validOwnership(actor application.Actor, accountID operatoraccount.ID) bool {
	return actor.OperatorID != uuid.Nil && !accountID.IsZero()
}

func validForceForgetInput(actor application.Actor, accountID operatoraccount.ID, idempotencyKey uuid.UUID) bool {
	return validOwnership(actor, accountID) && idempotencyKey != uuid.Nil
}

func lifecyclePredecessors(next operatoraccount.Status) (string, bool) {
	switch next {
	case operatoraccount.StatusAuthenticating:
		return `'disconnected', 'reauth_required'`, true
	case operatoraccount.StatusActive:
		return `'authenticating'`, true
	case operatoraccount.StatusReauthRequired:
		return `'active'`, true
	case operatoraccount.StatusDisconnected:
		return `'disconnecting'`, true
	case operatoraccount.StatusDisconnecting:
		return `'authenticating', 'active', 'reauth_required'`, true
	default:
		return "", false
	}
}

func optionalTelegramUserID(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}

func optionalAuthenticationExpiry(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func optionalFailureCode(value operatoraccount.FailureCode) any {
	if value == operatoraccount.NoFailure {
		return nil
	}
	return string(value)
}
