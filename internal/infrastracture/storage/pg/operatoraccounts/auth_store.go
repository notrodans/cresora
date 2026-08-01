package operatoraccounts

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

var _ applicationoperatoraccountauth.AuthenticationPersistence = (*Store)(nil)

const (
	operatorAccountIdentityUniqueIndex = "uq_operator_accounts_telegram_user_id"
)

// BeginOrResume durably admits a phone authentication attempt before any
// Telegram call. Phone uniqueness and concurrent admission are owned by
// PostgreSQL; no challenge or provider state is retained by this store.
func (store *Store) BeginOrResume(
	context context.Context,
	actor application.Actor,
	phone string,
	expiresAt time.Time,
) (applicationoperatoraccountauth.BeginResult, error) {
	normalized, failure := normalizePhone(phone)
	if failure != nil {
		return applicationoperatoraccountauth.BeginResult{}, failure
	}
	if actor.OperatorID == uuid.Nil {
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("%w: actor identity is required", applicationoperatoraccountauth.ErrInvalidInput)
	}
	if expiresAt.IsZero() {
		return applicationoperatoraccountauth.BeginResult{}, operatoraccount.ErrInvalidAuthenticationExpiry
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("begin operator account authentication: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var inserted accountRow
	failure = scanAccountRow(transaction.QueryRow(
		context,
		`INSERT INTO operator_accounts (
			operator_id,
			phone,
			status,
			status_version,
			auth_expires_at
		)
		VALUES ($1, $2, 'authenticating', 2, $3)
		ON CONFLICT (operator_id, phone) WHERE phone IS NOT NULL DO NOTHING
		RETURNING id, phone, telegram_username, telegram_first_name,
		          telegram_last_name, telegram_user_id, status::text,
		          status_version, auth_expires_at, failure_code`,
		actor.OperatorID,
		normalized,
		expiresAt,
	), &inserted)
	if failure == nil {
		if failure = transaction.Commit(context); failure != nil {
			return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("commit operator account authentication admission: %w", failure)
		}
		return applicationoperatoraccountauth.BeginResult{
			Account:       inserted.account(),
			Outcome:       applicationoperatoraccountauth.BeginStarted,
			AuthExpiresAt: inserted.authExpiry(),
		}, nil
	}
	if !errors.Is(failure, pgx.ErrNoRows) {
		if isForeignKeyViolation(failure) {
			return applicationoperatoraccountauth.BeginResult{}, applicationoperatoraccountauth.ErrAccountNotFound
		}
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("admit operator account authentication: %w", failure)
	}

	var current accountRow
	failure = scanAccountRow(transaction.QueryRow(
		context,
		`SELECT id, phone, telegram_username, telegram_first_name,
		        telegram_last_name, telegram_user_id, status::text,
		        status_version, auth_expires_at, failure_code
		 FROM operator_accounts
		 WHERE operator_id = $1
		   AND phone = $2
		 FOR UPDATE`,
		actor.OperatorID,
		normalized,
	), &current)
	if errors.Is(failure, pgx.ErrNoRows) {
		return applicationoperatoraccountauth.BeginResult{}, applicationoperatoraccountauth.ErrAccountNotFound
	}
	if failure != nil {
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("load operator account authentication admission: %w", failure)
	}

	outcome := applicationoperatoraccountauth.BeginInProgress
	switch current.status {
	case string(operatoraccount.StatusAuthenticating):
		// An existing authentication attempt is deliberately returned unchanged.
	case string(operatoraccount.StatusActive):
		outcome = applicationoperatoraccountauth.BeginAlreadyActive
	case string(operatoraccount.StatusDisconnected), string(operatoraccount.StatusReauthRequired):
		if current.version >= math.MaxInt64 {
			return applicationoperatoraccountauth.BeginResult{}, applicationoperatoraccountauth.ErrAccountVersionConflict
		}
		var resumed accountRow
		failure = scanAccountRow(transaction.QueryRow(
			context,
			`UPDATE operator_accounts
			 SET status = 'authenticating',
			     status_version = status_version + 1,
			     auth_expires_at = $3,
			     failure_code = NULL,
			     updated_at = clock_timestamp()
			 WHERE operator_id = $1
			   AND id = $2
			RETURNING id, phone, telegram_username, telegram_first_name,
			          telegram_last_name, telegram_user_id, status::text,
			          status_version, auth_expires_at, failure_code`,
			actor.OperatorID,
			current.id,
			expiresAt,
		), &resumed)
		if failure != nil {
			return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("resume operator account authentication: %w", failure)
		}
		current = resumed
		outcome = applicationoperatoraccountauth.BeginResumed
	case string(operatoraccount.StatusDisconnecting):
		return applicationoperatoraccountauth.BeginResult{}, applicationoperatoraccountauth.ErrAccountStateConflict
	default:
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("load operator account authentication admission: %w", operatoraccount.ErrInvalidState)
	}

	if failure = transaction.Commit(context); failure != nil {
		return applicationoperatoraccountauth.BeginResult{}, fmt.Errorf("commit operator account authentication admission: %w", failure)
	}
	return applicationoperatoraccountauth.BeginResult{
		Account:       current.account(),
		Outcome:       outcome,
		AuthExpiresAt: current.authExpiry(),
	}, nil
}

// Finalize atomically records the Telegram identity and profile and activates
// an authenticating account. The session row is checked in the same statement
// and transaction as the lifecycle update, so a session can never be observed
// as active without a persisted encrypted session.
func (store *Store) Finalize(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
	expectedVersion operatoraccount.Version,
	profile applicationoperatoraccountauth.Profile,
) (applicationoperatoraccountauth.Account, error) {
	if actor.OperatorID == uuid.Nil || accountID.IsZero() {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountNotFound
	}
	if expectedVersion == 0 || uint64(expectedVersion) >= math.MaxInt64 {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountVersionConflict
	}
	if failure := validateProfile(profile); failure != nil {
		return applicationoperatoraccountauth.Account{}, failure
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return applicationoperatoraccountauth.Account{}, fmt.Errorf("begin operator account authentication finalization: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var current accountRow
	failure = scanAccountRow(transaction.QueryRow(
		context,
		`SELECT id, phone, telegram_username, telegram_first_name,
		        telegram_last_name, telegram_user_id, status::text,
		        status_version, auth_expires_at, failure_code
		 FROM operator_accounts
		 WHERE operator_id = $1
		   AND id = $2
		 FOR UPDATE`,
		actor.OperatorID,
		accountID.UUID(),
	), &current)
	if errors.Is(failure, pgx.ErrNoRows) {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountNotFound
	}
	if failure != nil {
		return applicationoperatoraccountauth.Account{}, fmt.Errorf("inspect operator account authentication finalization: %w", failure)
	}
	if current.status == string(operatoraccount.StatusActive) &&
		current.version == int64(expectedVersion)+1 &&
		current.telegramUserID.Valid &&
		current.telegramUserID.Int64 == profile.UserID {
		if failure = transaction.Commit(context); failure != nil {
			return applicationoperatoraccountauth.Account{}, fmt.Errorf("commit duplicate operator account authentication finalization: %w", failure)
		}
		return current.account(), nil
	}
	if current.status != string(operatoraccount.StatusAuthenticating) || current.version != int64(expectedVersion) {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountVersionConflict
	}
	exists, checkFailure := sessionExists(transaction, context, current.id)
	if checkFailure != nil {
		return applicationoperatoraccountauth.Account{}, fmt.Errorf("check operator account authentication session: %w", checkFailure)
	}
	if !exists {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountStateConflict
	}

	var finalized accountRow
	failure = scanAccountRow(transaction.QueryRow(
		context,
		`UPDATE operator_accounts AS account
		 SET telegram_user_id = $4,
		     telegram_username = $5,
		     telegram_first_name = $6,
		     telegram_last_name = $7,
		     status = 'active',
		     status_version = account.status_version + 1,
		     auth_expires_at = NULL,
		     failure_code = NULL,
		     updated_at = clock_timestamp()
		 WHERE account.operator_id = $1
		   AND account.id = $2
		   AND account.status = 'authenticating'
		   AND account.status_version = $3
		   AND account.auth_expires_at > clock_timestamp()
		   AND EXISTS (
		       SELECT 1
		       FROM sessions
		       WHERE sessions.account_id = account.id
		   )
		 RETURNING account.id, account.phone, account.telegram_username,
		           account.telegram_first_name, account.telegram_last_name,
		           account.telegram_user_id, account.status::text,
		           account.status_version, account.auth_expires_at,
		           account.failure_code`,
		actor.OperatorID,
		accountID.UUID(),
		int64(expectedVersion),
		profile.UserID,
		nullableText(profile.Username),
		nullableText(profile.FirstName),
		nullableText(profile.LastName),
	), &finalized)
	if errors.Is(failure, pgx.ErrNoRows) {
		return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountVersionConflict
	}
	if failure != nil {
		if isConstraintViolation(failure, operatorAccountIdentityUniqueIndex) {
			return applicationoperatoraccountauth.Account{}, applicationoperatoraccountauth.ErrAccountStateConflict
		}
		return applicationoperatoraccountauth.Account{}, fmt.Errorf("finalize operator account authentication: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return applicationoperatoraccountauth.Account{}, fmt.Errorf("commit operator account authentication finalization: %w", failure)
	}
	return finalized.account(), nil
}

// BeginAbort fences an authenticating runtime before its process-local owner
// is stopped. It intentionally leaves the session row untouched until
// CompleteAbort, because the runtime may still need it while stopping.
func (store *Store) BeginAbort(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
	expectedVersion operatoraccount.Version,
) (operatoraccount.Version, error) {
	if actor.OperatorID == uuid.Nil || accountID.IsZero() {
		return 0, applicationoperatoraccountauth.ErrAccountNotFound
	}
	if expectedVersion == 0 || uint64(expectedVersion) >= math.MaxInt64 {
		return 0, applicationoperatoraccountauth.ErrAccountVersionConflict
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return 0, fmt.Errorf("begin operator account authentication abort: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var nextVersion int64
	failure = transaction.QueryRow(
		context,
		`UPDATE operator_accounts
		 SET status = 'disconnecting',
		     status_version = status_version + 1,
		     auth_expires_at = NULL,
		     failure_code = NULL,
		     updated_at = clock_timestamp()
		 WHERE operator_id = $1
		   AND id = $2
		   AND status = 'authenticating'
		   AND status_version = $3
		 RETURNING status_version`,
		actor.OperatorID,
		accountID.UUID(),
		int64(expectedVersion),
	).Scan(&nextVersion)
	if failure == nil {
		if failure = transaction.Commit(context); failure != nil {
			return 0, fmt.Errorf("commit operator account authentication abort: %w", failure)
		}
		return operatoraccount.Version(nextVersion), nil
	}
	if !errors.Is(failure, pgx.ErrNoRows) {
		return 0, fmt.Errorf("begin operator account authentication abort: %w", failure)
	}
	var (
		currentStatus  string
		currentVersion int64
	)
	failure = transaction.QueryRow(
		context,
		`SELECT status::text, status_version
		 FROM operator_accounts
		 WHERE operator_id = $1
		   AND id = $2
		 FOR UPDATE`,
		actor.OperatorID,
		accountID.UUID(),
	).Scan(&currentStatus, &currentVersion)
	if errors.Is(failure, pgx.ErrNoRows) {
		return 0, applicationoperatoraccountauth.ErrAccountNotFound
	}
	if failure != nil {
		return 0, fmt.Errorf("inspect operator account authentication abort: %w", failure)
	}
	if currentStatus == string(operatoraccount.StatusDisconnecting) &&
		currentVersion == int64(expectedVersion)+1 {
		if failure = transaction.Commit(context); failure != nil {
			return 0, fmt.Errorf("commit duplicate operator account authentication abort: %w", failure)
		}
		return operatoraccount.Version(currentVersion), nil
	}
	if currentStatus == string(operatoraccount.StatusDisconnected) &&
		currentVersion == int64(expectedVersion)+2 {
		if failure = transaction.Commit(context); failure != nil {
			return 0, fmt.Errorf("commit ambiguous duplicate operator account authentication abort: %w", failure)
		}
		return operatoraccount.Version(expectedVersion + 1), nil
	}
	return 0, applicationoperatoraccountauth.ErrAccountVersionConflict
}

// CompleteAbort performs the final durable half of abort. The state change
// and session deletion share one transaction, and the exact disconnecting
// version is required so a late completion cannot erase a newer run.
func (store *Store) CompleteAbort(
	context context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
	expectedVersion operatoraccount.Version,
) error {
	if actor.OperatorID == uuid.Nil || accountID.IsZero() {
		return applicationoperatoraccountauth.ErrAccountNotFound
	}
	if expectedVersion == 0 || uint64(expectedVersion) >= math.MaxInt64 {
		return applicationoperatoraccountauth.ErrAccountVersionConflict
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin operator account authentication completion: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var disconnectedID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`UPDATE operator_accounts
		 SET status = 'disconnected',
		     status_version = status_version + 1,
		     auth_expires_at = NULL,
		     failure_code = NULL,
		     updated_at = clock_timestamp()
		 WHERE operator_id = $1
		   AND id = $2
		   AND status = 'disconnecting'
		   AND status_version = $3
		 RETURNING id`,
		actor.OperatorID,
		accountID.UUID(),
		int64(expectedVersion),
	).Scan(&disconnectedID)
	if errors.Is(failure, pgx.ErrNoRows) {
		var (
			currentStatus  string
			currentVersion int64
			hasSession     bool
		)
		failure = transaction.QueryRow(
			context,
			`SELECT status::text,
			        status_version,
			        EXISTS (SELECT 1 FROM sessions WHERE account_id = operator_accounts.id)
			 FROM operator_accounts
			 WHERE operator_id = $1
			   AND id = $2
			 FOR UPDATE`,
			actor.OperatorID,
			accountID.UUID(),
		).Scan(&currentStatus, &currentVersion, &hasSession)
		if errors.Is(failure, pgx.ErrNoRows) {
			return applicationoperatoraccountauth.ErrAccountNotFound
		}
		if failure != nil {
			return fmt.Errorf("inspect operator account authentication completion: %w", failure)
		}
		if currentStatus == string(operatoraccount.StatusDisconnected) &&
			currentVersion == int64(expectedVersion)+1 &&
			!hasSession {
			if failure = transaction.Commit(context); failure != nil {
				return fmt.Errorf("commit duplicate operator account authentication completion: %w", failure)
			}
			return nil
		}
		return applicationoperatoraccountauth.ErrAccountVersionConflict
	}
	if failure != nil {
		return fmt.Errorf("complete operator account authentication abort: %w", failure)
	}
	if _, failure = transaction.Exec(context, `DELETE FROM sessions WHERE account_id = $1`, disconnectedID); failure != nil {
		return fmt.Errorf("delete aborted operator account session: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit operator account authentication completion: %w", failure)
	}
	return nil
}

// ListAccounts is actor-scoped and returns only durable account state. In
// particular, it does not expose process-local phone-code challenges.
func (store *Store) ListAccounts(
	context context.Context,
	actor application.Actor,
) ([]applicationoperatoraccountauth.Account, error) {
	if actor.OperatorID == uuid.Nil {
		return nil, fmt.Errorf("%w: actor identity is required", applicationoperatoraccountauth.ErrInvalidInput)
	}
	rows, failure := store.database.Query(
		context,
		`SELECT id, phone, telegram_username, telegram_first_name,
		        telegram_last_name, telegram_user_id, status::text,
		        status_version, auth_expires_at, failure_code
		 FROM operator_accounts
		 WHERE operator_id = $1
		 ORDER BY created_at, id`,
		actor.OperatorID,
	)
	if failure != nil {
		return nil, fmt.Errorf("list operator accounts: %w", failure)
	}
	defer rows.Close()

	accounts := make([]applicationoperatoraccountauth.Account, 0)
	for rows.Next() {
		var row accountRow
		if failure = scanAccountRow(rows, &row); failure != nil {
			return nil, fmt.Errorf("scan operator account: %w", failure)
		}
		accounts = append(accounts, row.account())
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("list operator accounts: %w", failure)
	}
	return accounts, nil
}

// ListOrphanAuthenticationLifecycles returns durable authentication
// candidates. Runtime ownership is deliberately not represented in PostgreSQL,
// so every current
// authenticating or disconnecting row is a candidate after a process restart.
// The returned status and version are the authoritative persisted lifecycle
// snapshot; disconnecting candidates are completed without another BeginAbort.
func (store *Store) ListOrphanAuthenticationLifecycles(
	context context.Context,
) ([]applicationoperatoraccountauth.AuthTarget, error) {
	rows, failure := store.database.Query(
		context,
		`SELECT operator_id, id, status::text, status_version
		 FROM operator_accounts
		 WHERE status IN ('authenticating', 'disconnecting')
		 ORDER BY created_at, id`,
	)
	if failure != nil {
		return nil, fmt.Errorf("list orphan operator account authentications: %w", failure)
	}
	defer rows.Close()

	targets := make([]applicationoperatoraccountauth.AuthTarget, 0)
	for rows.Next() {
		var (
			operatorID uuid.UUID
			accountID  uuid.UUID
			status     string
			version    int64
		)
		if failure = rows.Scan(&operatorID, &accountID, &status, &version); failure != nil {
			return nil, fmt.Errorf("scan orphan operator account authentication: %w", failure)
		}
		if version <= 0 {
			return nil, fmt.Errorf("scan orphan operator account authentication: %w", operatoraccount.ErrInvalidState)
		}
		targets = append(targets, applicationoperatoraccountauth.AuthTarget{
			Actor:     application.Actor{OperatorID: operatorID},
			AccountID: operatoraccount.Identity(accountID),
			Status:    operatoraccount.Status(status),
			Version:   operatoraccount.Version(version),
		})
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("list orphan operator account authentications: %w", failure)
	}
	return targets, nil
}

type accountRow struct {
	id             uuid.UUID
	phone          pgtype.Text
	username       pgtype.Text
	firstName      pgtype.Text
	lastName       pgtype.Text
	telegramUserID pgtype.Int8
	status         string
	version        int64
	authExpiresAt  pgtype.Timestamptz
	failureCode    pgtype.Text
}

func (row accountRow) authExpiry() time.Time {
	if !row.authExpiresAt.Valid {
		return time.Time{}
	}
	return row.authExpiresAt.Time
}

func scanAccountRow(scanner interface{ Scan(...any) error }, row *accountRow) error {
	return scanner.Scan(
		&row.id,
		&row.phone,
		&row.username,
		&row.firstName,
		&row.lastName,
		&row.telegramUserID,
		&row.status,
		&row.version,
		&row.authExpiresAt,
		&row.failureCode,
	)
}

func (row accountRow) account() applicationoperatoraccountauth.Account {
	return applicationoperatoraccountauth.Account{
		ID:                row.id,
		Phone:             nullableString(row.phone),
		Profile:           applicationoperatoraccountauth.Profile{UserID: nullableInt64(row.telegramUserID), Username: nullableString(row.username), FirstName: nullableString(row.firstName), LastName: nullableString(row.lastName)},
		Status:            operatoraccount.Status(row.status),
		Version:           operatoraccount.Version(row.version),
		TelegramUsername:  nullableString(row.username),
		TelegramFirstName: nullableString(row.firstName),
		TelegramLastName:  nullableString(row.lastName),
	}
}

func sessionExists(transaction pgx.Tx, context context.Context, accountID uuid.UUID) (bool, error) {
	var exists bool
	if failure := transaction.QueryRow(
		context,
		`SELECT EXISTS (SELECT 1 FROM sessions WHERE account_id = $1)`,
		accountID,
	).Scan(&exists); failure != nil {
		return false, failure
	}
	return exists, nil
}

func validateProfile(profile applicationoperatoraccountauth.Profile) error {
	if profile.UserID <= 0 {
		return fmt.Errorf("%w: telegram identity is required", applicationoperatoraccountauth.ErrInvalidInput)
	}
	for name, field := range map[string]struct {
		value string
		limit int
	}{
		"telegram username":   {value: profile.Username, limit: 32},
		"telegram first name": {value: profile.FirstName, limit: 64},
		"telegram last name":  {value: profile.LastName, limit: 64},
	} {
		if !utf8.ValidString(field.value) || utf8.RuneCountInString(field.value) > field.limit {
			return fmt.Errorf("%w: %s is too long or invalid", applicationoperatoraccountauth.ErrInvalidInput, name)
		}
	}
	return nil
}

func normalizePhone(phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", fmt.Errorf("%w: phone is required", applicationoperatoraccountauth.ErrInvalidInput)
	}
	var normalized strings.Builder
	for index, character := range phone {
		switch {
		case character == '+' && index == 0:
			normalized.WriteRune(character)
		case character >= '0' && character <= '9':
			normalized.WriteRune(character)
		case character == ' ' || character == '-' || character == '(' || character == ')' || character == '.':
			// Ignore common display formatting.
		case unicode.IsDigit(character):
			return "", fmt.Errorf("%w: phone contains unsupported digit", applicationoperatoraccountauth.ErrInvalidInput)
		default:
			return "", fmt.Errorf("%w: phone contains unsupported character", applicationoperatoraccountauth.ErrInvalidInput)
		}
	}
	value := normalized.String()
	if after, ok := strings.CutPrefix(value, "00"); ok {
		value = "+" + after
	}
	if !strings.HasPrefix(value, "+") {
		value = "+" + value
	}
	digits := strings.TrimPrefix(value, "+")
	if len(digits) < 7 || len(digits) > 15 || digits[0] == '0' {
		return "", fmt.Errorf("%w: phone must contain 7 to 15 international digits", applicationoperatoraccountauth.ErrInvalidInput)
	}
	return value, nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableString(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullableInt64(value pgtype.Int8) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}

func isForeignKeyViolation(failure error) bool {
	var pgFailure *pgconn.PgError
	return errors.As(failure, &pgFailure) && pgFailure.Code == "23503"
}

func isConstraintViolation(failure error, constraint string) bool {
	var pgFailure *pgconn.PgError
	return errors.As(failure, &pgFailure) && pgFailure.ConstraintName == constraint
}
