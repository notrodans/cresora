package operatoraccounts

import "errors"

var (
	// ErrInvalidInput identifies malformed command input or an invalid actor
	// scope.
	ErrInvalidInput = errors.New("invalid operator account disconnect input")
	// ErrAccountStateConflict indicates that an operation is not permitted while
	// the owned account is in its current lifecycle state.
	ErrAccountStateConflict = errors.New("operator account lifecycle state conflict")
	// ErrAccountNotFound covers both an unknown account and an account outside
	// the actor's ownership scope. Keeping these cases equivalent avoids an
	// ownership oracle at application boundaries.
	ErrAccountNotFound = errors.New("operator account not found")
	// ErrAccountVersionConflict indicates that another writer changed the
	// account lifecycle after it was read.
	ErrAccountVersionConflict = errors.New("operator account lifecycle version conflict")
	// ErrSessionNotFound indicates that the requested account has no persisted
	// Telegram session.
	ErrSessionNotFound = errors.New("operator account session not found")
	// ErrRemoteLogoutNotConverged classifies a runtime failure after a durable
	// remote logout intent has been recorded. The intent and session must remain
	// durable until a later attempt converges.
	ErrRemoteLogoutNotConverged = errors.New("telegram account remote logout did not converge")
	// ErrRemoteLogoutFloodWait identifies a bounded provider rate-limit result.
	ErrRemoteLogoutFloodWait = errors.New("telegram account remote logout flood wait")
	// ErrRemoteLogoutTransient identifies a bounded transport or server failure.
	ErrRemoteLogoutTransient = errors.New("telegram account remote logout transient failure")
	// ErrRemoteLogoutAmbiguous identifies a cancellation or lost response where
	// the remote effect may have happened.
	ErrRemoteLogoutAmbiguous = errors.New("telegram account remote logout result is ambiguous")
	// ErrRemoteLogoutPermanent identifies a bounded non-retryable remote failure.
	ErrRemoteLogoutPermanent = errors.New("telegram account remote logout permanently failed")
	// ErrRuntimeUnavailable identifies a runtime that cannot currently perform
	// the single revoke-and-stop operation.
	ErrRuntimeUnavailable = errors.New("telegram account runtime unavailable")
	// ErrStartupRecovery identifies a startup-fatal inability to enumerate or
	// trust durable remote logout state. Account-local remote non-convergence is
	// represented by RecoveryResult.Pending instead.
	ErrStartupRecovery = errors.New("telegram account startup recovery failed")
	// ErrInvalidRemoteLogoutFailure identifies an invalid or contradictory safe
	// remote failure value.
	ErrInvalidRemoteLogoutFailure = errors.New("telegram account remote logout failure is invalid")
	// ErrInvalidRuntimeOutcome identifies a malformed adapter result at the
	// application runtime port.
	ErrInvalidRuntimeOutcome = errors.New("telegram account runtime result is invalid")
)
