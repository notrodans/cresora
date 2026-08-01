package operatoraccountauth

import (
	"errors"
	"time"
)

var (
	// ErrInvalidInput identifies malformed command input or an invalid scope.
	ErrInvalidInput = errors.New("invalid operator account authentication input")
	// ErrInvalidResult identifies a result with neither or both outcomes set.
	ErrInvalidResult = errors.New("operator account authentication result is invalid")
	// ErrInvalidBeginResult identifies an unknown durable admission outcome.
	ErrInvalidBeginResult = errors.New("operator account authentication begin result is invalid")
	// ErrInvalidAuthenticationExpiry identifies a missing or contradictory
	// durable authentication expiry.
	ErrInvalidAuthenticationExpiry = errors.New("operator account authentication expiry is invalid")
	// ErrInvalidRetryAfter identifies a non-positive retry duration.
	ErrInvalidRetryAfter = errors.New("operator account authentication retry-after is invalid")
	// ErrAccountNotFound covers unknown accounts and accounts outside the
	// actor's ownership scope.
	ErrAccountNotFound = errors.New("operator account not found")
	// ErrAccountVersionConflict identifies a stale conditional lifecycle write.
	ErrAccountVersionConflict = errors.New("operator account authentication version conflict")
	// ErrAccountStateConflict identifies a lifecycle state that cannot satisfy
	// the requested authentication operation.
	ErrAccountStateConflict = errors.New("operator account authentication state conflict")

	// ErrChallengeNotFound intentionally also covers a foreign request ID.
	ErrChallengeNotFound = errors.New("authentication challenge not found")
	// ErrChallengeExpired identifies a challenge that can no longer accept an
	// operation.
	ErrChallengeExpired = errors.New("authentication challenge expired")
	// ErrInvalidCode identifies a code rejected by Telegram. The challenge is
	// retained for a bounded retry budget.
	ErrInvalidCode = errors.New("authentication code rejected")
	// ErrPasswordRequired identifies the transition from code to password
	// stage. It is a control result, not a terminal failure.
	ErrPasswordRequired = errors.New("authentication password required")
	// ErrInvalidPassword identifies a password rejected by Telegram. The
	// challenge remains available for another password attempt.
	ErrInvalidPassword = errors.New("authentication password rejected")

	// ErrProviderUnavailable identifies an absent or unusable runtime provider.
	ErrProviderUnavailable = errors.New("telegram authentication provider unavailable")
	// ErrProviderTransient identifies a bounded provider failure that must not
	// delete the persisted session or authentication challenge.
	ErrProviderTransient = errors.New("telegram authentication provider temporarily unavailable")
	// ErrFloodWait identifies a safe retry-after response from Telegram.
	ErrFloodWait = errors.New("telegram authentication temporarily rate limited")
	// ErrUnauthorized identifies an authorization/session failure that requires
	// a conditional lifecycle abort.
	ErrUnauthorized = errors.New("telegram authentication authorization failed")
	// ErrSessionUnavailable identifies a provider session that cannot be used.
	ErrSessionUnavailable = errors.New("telegram authentication session unavailable")

	// ErrAuthenticationAborted identifies a cancelled or recovered challenge.
	ErrAuthenticationAborted = errors.New("telegram authentication aborted")
	// ErrStartupRecovery identifies a startup recovery failure.
	ErrStartupRecovery = errors.New("telegram authentication startup recovery failed")
)

// ProviderFailureKind is the bounded, transport-neutral diagnostic taxonomy
// for a provider failure. ProviderFailureUnknown is intentionally invalid for
// ProviderFailureError values.
type ProviderFailureKind string

const (
	ProviderFailureUnknown               ProviderFailureKind = ""
	ProviderFailureConfigurationRejected ProviderFailureKind = "configuration_rejected"
	ProviderFailurePhoneRejected         ProviderFailureKind = "phone_rejected"
	ProviderFailureRuntimeCapacity       ProviderFailureKind = "runtime_capacity"
	ProviderFailureProtocol              ProviderFailureKind = "protocol"
	ProviderFailureRemoteRejected        ProviderFailureKind = "remote_rejected"
	ProviderFailureRemoteFailure         ProviderFailureKind = "remote_failure"
	ProviderFailureTransportUnknown      ProviderFailureKind = "transport_unknown"
)

// These aliases keep the enum readable at call sites that prefer the type
// name as a prefix without adding another representation or an unsafe value.
const (
	ProviderFailureKindUnknown               = ProviderFailureUnknown
	ProviderFailureKindConfigurationRejected = ProviderFailureConfigurationRejected
	ProviderFailureKindPhoneRejected         = ProviderFailurePhoneRejected
	ProviderFailureKindRuntimeCapacity       = ProviderFailureRuntimeCapacity
	ProviderFailureKindProtocol              = ProviderFailureProtocol
	ProviderFailureKindRemoteRejected        = ProviderFailureRemoteRejected
	ProviderFailureKindRemoteFailure         = ProviderFailureRemoteFailure
	ProviderFailureKindTransportUnknown      = ProviderFailureTransportUnknown
)

const providerFailureMessage = "telegram authentication provider failure"

// ProviderFailureError is the safe application representation of a provider
// diagnostic. It deliberately retains no provider error, message, or vendor
// type. Protocol failures represent an invalid successful SendCode response
// and retain the provider-unavailable lifecycle classification; all other
// validated kinds retain the transient classification.
type ProviderFailureError struct {
	kind ProviderFailureKind
}

// NewProviderFailureError constructs a provider diagnostic from the bounded
// application taxonomy.
func NewProviderFailureError(kind ProviderFailureKind) (*ProviderFailureError, error) {
	if !validProviderFailureKind(kind) {
		return nil, ErrInvalidInput
	}
	return &ProviderFailureError{kind: kind}, nil
}

// Error implements error with a fixed message that contains no provider data.
func (failure *ProviderFailureError) Error() string {
	return providerFailureMessage
}

// Unwrap exposes only an approved application-level classification. An
// invalid zero value is treated as transient rather than exposing arbitrary
// or provider-derived state.
func (failure *ProviderFailureError) Unwrap() error {
	if failure != nil && failure.kind == ProviderFailureProtocol {
		return ErrProviderUnavailable
	}
	return ErrProviderTransient
}

// Kind returns the validated provider failure kind.
func (failure *ProviderFailureError) Kind() ProviderFailureKind {
	if failure == nil || !validProviderFailureKind(failure.kind) {
		return ProviderFailureUnknown
	}
	return failure.kind
}

func validProviderFailureKind(kind ProviderFailureKind) bool {
	switch kind {
	case ProviderFailureConfigurationRejected,
		ProviderFailurePhoneRejected,
		ProviderFailureRuntimeCapacity,
		ProviderFailureProtocol,
		ProviderFailureRemoteRejected,
		ProviderFailureRemoteFailure,
		ProviderFailureTransportUnknown:
		return true
	default:
		return false
	}
}

// RetryAfterError is the safe application representation of an auth flood
// wait. After is positive and bounded by time.Duration; the service bounds it
// further by the challenge expiry before returning it to a caller. The type
// never retains or formats the provider's original error.
type RetryAfterError struct {
	after time.Duration
}

// NewRetryAfterError constructs the only valid retry hint: a positive
// duration. The returned error is a *RetryAfterError so callers can retrieve it
// through errors.As without depending on provider error types.
func NewRetryAfterError(after time.Duration) (*RetryAfterError, error) {
	if after <= 0 {
		return nil, ErrInvalidRetryAfter
	}
	return &RetryAfterError{after: after}, nil
}

// Error implements error without exposing provider data.
func (failure RetryAfterError) Error() string {
	return ErrFloodWait.Error()
}

// Unwrap lets callers classify the error semantically.
func (failure RetryAfterError) Unwrap() error {
	return ErrFloodWait
}

// RetryAfter returns the safe retry duration.
func (failure RetryAfterError) RetryAfter() time.Duration {
	return failure.after
}
