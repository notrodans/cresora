package operatoraccount

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidTransition indicates that an explicit lifecycle operation is
	// not valid from the account's current status.
	ErrInvalidTransition = errors.New("operator account lifecycle transition is invalid")
	// ErrInvalidState indicates that persisted account state cannot satisfy the
	// account invariants.
	ErrInvalidState = errors.New("operator account state is invalid")
	// ErrInvalidFailureCode indicates a missing or unsupported failure code.
	ErrInvalidFailureCode = errors.New("operator account failure code is invalid")
	// ErrInvalidTelegramIdentity indicates that a canonical Telegram user ID is
	// missing or outside the valid positive Telegram user ID range.
	ErrInvalidTelegramIdentity = errors.New("operator account Telegram identity is invalid")
	// ErrInvalidAuthenticationExpiry indicates that an authentication attempt
	// was created without an expiry.
	ErrInvalidAuthenticationExpiry = errors.New("operator account authentication expiry is invalid")
	// ErrVersionExhausted indicates that a lifecycle transition cannot safely
	// advance the optimistic-concurrency version any further.
	ErrVersionExhausted = errors.New("operator account lifecycle version is exhausted")
)

// Account is one operator account's current lifecycle state. New accounts are
// disconnected at version one.
//
// State is changed only by the explicit operations below. In particular,
// callers cannot set Status, Version, FailureCode, identity, or authentication
// expiry independently.
type Account struct {
	id             ID
	status         Status
	version        Version
	failureCode    FailureCode
	telegramUserID int64
	authExpiresAt  time.Time
}

// New creates an account in the disconnected state at the initial version.
func New(id ID) Account {
	if id.IsZero() {
		panic("create operator account from zero identity")
	}
	return Account{
		id:      id,
		status:  StatusDisconnected,
		version: InitialVersion,
	}
}

// Restore reconstructs the current account state read from persistence. It
// validates the complete state instead of exposing a general-purpose setter.
func Restore(
	id ID,
	status Status,
	version Version,
	failureCode FailureCode,
	telegramUserID int64,
	authExpiresAt time.Time,
) (Account, error) {
	account := Account{
		id:             id,
		status:         status,
		version:        version,
		failureCode:    failureCode,
		telegramUserID: telegramUserID,
		authExpiresAt:  authExpiresAt,
	}
	if failure := account.validate(); failure != nil {
		return Account{}, failure
	}
	return account, nil
}

// ID returns the account identity.
func (account Account) ID() ID {
	return account.id
}

// Status returns the account's current lifecycle status.
func (account Account) Status() Status {
	return account.status
}

// Version returns the account's current optimistic-concurrency version.
func (account Account) Version() Version {
	return account.version
}

// FailureCode returns the current stable failure code. It is NoFailure unless
// the account is in StatusReauthRequired.
func (account Account) FailureCode() FailureCode {
	return account.failureCode
}

// TelegramUserID returns the canonical Telegram user ID, or zero when the
// account has not acquired one yet.
func (account Account) TelegramUserID() int64 {
	return account.telegramUserID
}

// AuthExpiresAt returns the expiry of the current authentication attempt. It
// is zero unless the account is in StatusAuthenticating.
func (account Account) AuthExpiresAt() time.Time {
	return account.authExpiresAt
}

// BeginAuthentication moves a disconnected or reauthentication-required
// account into authentication. An authentication expiry is mandatory.
func (account *Account) BeginAuthentication(expiresAt time.Time) error {
	if expiresAt.IsZero() {
		return ErrInvalidAuthenticationExpiry
	}
	if failure := account.move(StatusAuthenticating, StatusDisconnected, StatusReauthRequired); failure != nil {
		return failure
	}
	account.authExpiresAt = expiresAt
	account.failureCode = NoFailure
	return nil
}

// Activate records the canonical Telegram identity after authentication has
// succeeded. A valid positive Telegram user ID is mandatory.
func (account *Account) Activate(telegramUserID int64) error {
	if telegramUserID <= 0 {
		return ErrInvalidTelegramIdentity
	}
	if failure := account.move(StatusActive, StatusAuthenticating); failure != nil {
		return failure
	}
	account.telegramUserID = telegramUserID
	account.authExpiresAt = time.Time{}
	account.failureCode = NoFailure
	return nil
}

// RequireReauthentication moves an active account into the reauthentication
// state and records one bounded stable reason.
func (account *Account) RequireReauthentication(code FailureCode) error {
	if !validFailureCode(code) {
		return ErrInvalidFailureCode
	}
	if account.telegramUserID <= 0 {
		return ErrInvalidTelegramIdentity
	}
	if failure := account.move(StatusReauthRequired, StatusActive); failure != nil {
		return failure
	}
	account.authExpiresAt = time.Time{}
	account.failureCode = code
	return nil
}

// BeginDisconnect requests shutdown from any state that may own an active or
// in-progress authentication session.
func (account *Account) BeginDisconnect() error {
	if failure := account.move(StatusDisconnecting, StatusAuthenticating, StatusActive, StatusReauthRequired); failure != nil {
		return failure
	}
	account.authExpiresAt = time.Time{}
	account.failureCode = NoFailure
	return nil
}

// MarkDisconnected records completion of a requested disconnect. The known
// Telegram identity is retained as account metadata; it is not required while
// disconnected and may be replaced by a later Activate operation.
func (account *Account) MarkDisconnected() error {
	if failure := account.move(StatusDisconnected, StatusDisconnecting); failure != nil {
		return failure
	}
	account.authExpiresAt = time.Time{}
	account.failureCode = NoFailure
	return nil
}

func (account *Account) move(next Status, allowed ...Status) error {
	for _, current := range allowed {
		if account.status == current {
			if failure := account.advanceVersion(); failure != nil {
				return failure
			}
			account.status = next
			return nil
		}
	}
	return account.invalidTransition(next)
}

func (account *Account) advanceVersion() error {
	if account.version == ^Version(0) {
		return ErrVersionExhausted
	}
	account.version++
	return nil
}

func (account Account) invalidTransition(next Status) error {
	return fmt.Errorf("%w: cannot move from %q to %q", ErrInvalidTransition, account.status, next)
}

func (account Account) validate() error {
	if account.id.IsZero() {
		return fmt.Errorf("%w: account identity is required", ErrInvalidState)
	}
	if !account.status.valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidState, account.status)
	}
	if account.version == 0 {
		return fmt.Errorf("%w: version must be positive", ErrInvalidState)
	}
	if account.telegramUserID < 0 {
		return fmt.Errorf("%w: Telegram user ID must not be negative", ErrInvalidState)
	}
	if account.status == StatusActive || account.status == StatusReauthRequired {
		if account.telegramUserID <= 0 {
			return fmt.Errorf("%w: %q requires a canonical Telegram identity", ErrInvalidState, account.status)
		}
	}
	if account.status == StatusAuthenticating {
		if account.authExpiresAt.IsZero() {
			return fmt.Errorf("%w: authenticating account requires an expiry", ErrInvalidState)
		}
	} else if !account.authExpiresAt.IsZero() {
		return fmt.Errorf("%w: authentication expiry is only valid while authenticating", ErrInvalidState)
	}
	if account.status == StatusReauthRequired {
		if !validFailureCode(account.failureCode) {
			return fmt.Errorf("%w: reauthentication-required account needs a stable code", ErrInvalidState)
		}
	} else if account.failureCode != NoFailure {
		return fmt.Errorf("%w: failure code is only valid while reauthentication is required", ErrInvalidState)
	}
	return nil
}
