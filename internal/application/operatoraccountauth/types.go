// Package operatoraccountauth contains transport-neutral data for operator
// account authentication.
package operatoraccountauth

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// Stage is the next action required by a phone-auth challenge.
type Stage string

const (
	// StageCode means that Telegram is waiting for the phone code.
	StageCode Stage = "code"
	// StagePassword means that Telegram requested the account's 2FA password.
	StagePassword Stage = "password"
)

// Profile is the safe, transport-neutral Telegram identity returned by
// Self. It deliberately contains only the fields needed by the account
// lifecycle projection.
type Profile struct {
	UserID    int64
	Username  string
	FirstName string
	LastName  string
}

// Account is the display projection of an operator-owned Telegram account.
// Its fields mirror the account data exposed by the operator_accounts table.
// Status and Version use the domain lifecycle types. Profile is embedded so a
// finalized account contains the identity returned by Self. The flattened
// Telegram fields remain part of the existing display projection until the
// disabled HTTP projection is migrated in a later lane.
type Account struct {
	ID    uuid.UUID
	Phone string
	Profile
	Status            operatoraccount.Status
	Version           operatoraccount.Version
	TelegramUsername  string
	TelegramFirstName string
	TelegramLastName  string
}

// AuthTarget is the compatibility name for the canonical application-owned
// runtime admission value.
type AuthTarget = operatoraccounts.RuntimeTarget

// Challenge is the safe projection of one in-memory phone-auth attempt. The
// Telegram phone-code hash is intentionally absent: it is retained only by
// the process-local coordinator and passed back to PhoneProvider.SignIn. The
// embedded target keeps actor, account, and lifecycle version together.
type Challenge struct {
	RequestID uuid.UUID
	AuthTarget
	Phone     string
	Delivery  string
	Stage     Stage
	ExpiresAt time.Time
}

// PhoneCodeHash is an opaque in-memory Telegram phone-code hash. Exactly
// one of Account and Challenge is set: an Account means authentication is
// complete, while a Challenge means another phone-auth operation is needed.
type Result struct {
	Account   *Account
	Challenge *Challenge
}

// Validate enforces the result invariant: exactly one of Account and
// Challenge must be present.
func (result Result) Validate() error {
	if (result.Account == nil) == (result.Challenge == nil) {
		return ErrInvalidResult
	}
	return nil
}

// BeginOutcome is the unambiguous durable admission outcome returned before a
// provider operation.
type BeginOutcome string

const (
	// BeginStarted means a new account was durably moved to authenticating.
	BeginStarted BeginOutcome = "started"
	// BeginResumed means a disconnected or reauthentication-required account was
	// durably moved to authenticating.
	BeginResumed BeginOutcome = "resumed"
	// BeginInProgress means an existing authenticating account was returned
	// unchanged.
	BeginInProgress BeginOutcome = "in_progress"
	// BeginAlreadyActive means the account is already active. It is a normal
	// result and is never returned as an error.
	BeginAlreadyActive BeginOutcome = "already_active"
)

// BeginResult describes the durable account admission decision made before a
// provider operation. Implementations must persist authenticating before a
// caller invokes SendCode and return the authoritative stored auth_expires_at
// in AuthExpiresAt for started, resumed, and in-progress outcomes. Active
// outcomes have no authentication expiry.
type BeginResult struct {
	Account       Account
	Outcome       BeginOutcome
	AuthExpiresAt time.Time
}

// Validate enforces the durable lifecycle and expiry contract. Every outcome
// that creates or retains an authenticating lifecycle must expose the
// authoritative stored auth_expires_at value. An already-active account has
// no authentication expiry.
func (result BeginResult) Validate() error {
	switch result.Outcome {
	case BeginStarted, BeginResumed, BeginInProgress:
		if result.Account.Status != operatoraccount.StatusAuthenticating {
			return ErrInvalidBeginResult
		}
		if result.AuthExpiresAt.IsZero() {
			return ErrInvalidAuthenticationExpiry
		}
	case BeginAlreadyActive:
		if result.Account.Status != operatoraccount.StatusActive {
			return ErrInvalidBeginResult
		}
		if !result.AuthExpiresAt.IsZero() {
			return ErrInvalidAuthenticationExpiry
		}
	default:
		return ErrInvalidBeginResult
	}
	return nil
}

// PhoneCodeHash is an opaque in-memory Telegram phone-code hash. Its only
// value access is the explicitly named transport hand-off method, and String
// never returns the secret, which prevents accidental inclusion in logs or
// browser projections.
type PhoneCodeHash struct {
	value string
}

// NewPhoneCodeHash wraps a provider hash for the in-memory auth protocol.
// Empty hashes are invalid provider output and must be rejected by the
// transport adapter before they reach the coordinator.
func NewPhoneCodeHash(value string) PhoneCodeHash {
	return PhoneCodeHash{value: value}
}

// IsZero reports whether no provider hash was supplied.
func (hash PhoneCodeHash) IsZero() bool {
	return hash.value == ""
}

// Value returns the transient hash for the transport adapter's SignIn call.
// Callers must not persist, serialize, or log this value; String is the safe
// representation for diagnostics.
func (hash PhoneCodeHash) Value() string {
	return hash.value
}

// String intentionally redacts the hash.
func (hash PhoneCodeHash) String() string {
	if hash.IsZero() {
		return "<empty phone code hash>"
	}
	return "<redacted phone code hash>"
}

// GoString implements fmt.GoStringer so %#v cannot reveal the unexported
// backing value.
func (hash PhoneCodeHash) GoString() string {
	return "operatoraccountauth.PhoneCodeHash(<redacted>)"
}

// LogValue implements slog.LogValuer with the same redacted representation as
// String. The raw hash is never a structured-log attribute value.
func (hash PhoneCodeHash) LogValue() slog.Value {
	return slog.StringValue(hash.String())
}

// SendCodeResult is the provider result after SendCode. PhoneCodeHash is
// transient coordinator state; Delivery and ExpiresAt are safe challenge
// metadata.
type SendCodeResult struct {
	PhoneCodeHash PhoneCodeHash
	Delivery      string
	ExpiresAt     time.Time
}

// Status is the operator account authentication dashboard projection. A nil
// challenge means that no authentication flow is in progress.
type Status struct {
	Accounts  []Account
	Challenge *Challenge
}
