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
	_ applicationoperatoraccounts.SessionDeleter             = (*Store)(nil)
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
		id             uuid.UUID
		status         string
		version        int64
		failureCode    pgtype.Text
		telegramUserID pgtype.Int8
		authExpiresAt  pgtype.Timestamptz
	)
	failure := store.database.QueryRow(
		context,
		`SELECT account.id,
		        account.status::text,
		        account.status_version,
		        account.failure_code,
		        account.telegram_user_id,
		        account.auth_expires_at
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
	)
	if failure != nil {
		return operatoraccount.Account{}, fmt.Errorf("load operator account: %w", failure)
	}
	return restored, nil
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
		     updated_at = clock_timestamp()
		 WHERE account.operator_id = $1
		   AND account.id = $2
		   AND account.status IN (`+previousStatuses+`)
		   AND account.status_version = $7
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
