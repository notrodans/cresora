package operatoraccounts

import "time"

// RemoteLogoutFailureKind is the bounded, transport-neutral taxonomy accepted
// at the runtime port. It intentionally contains no provider status code,
// response text, or vendor error type.
type RemoteLogoutFailureKind string

const (
	// RemoteLogoutFailureUnknown is reserved for an unclassified internal
	// result and is not accepted by NewRemoteLogoutFailure.
	RemoteLogoutFailureUnknown RemoteLogoutFailureKind = ""
	// RemoteLogoutFailureFloodWait is a provider rate limit with a retry-after
	// duration.
	RemoteLogoutFailureFloodWait RemoteLogoutFailureKind = "flood_wait"
	// RemoteLogoutFailureTransient covers transport failures and remote 5xx
	// responses.
	RemoteLogoutFailureTransient RemoteLogoutFailureKind = "transient"
	// RemoteLogoutFailureAmbiguous covers cancellation or lost responses where
	// the remote revoke may already have happened.
	RemoteLogoutFailureAmbiguous RemoteLogoutFailureKind = "ambiguous"
	// RemoteLogoutFailurePermanent covers remote 4xx responses and missing or
	// corrupt sessions that cannot be revoked.
	RemoteLogoutFailurePermanent RemoteLogoutFailureKind = "permanent"
	// RemoteLogoutFailureUnavailable covers runtime unavailability and
	// owner-fatal failures.
	RemoteLogoutFailureUnavailable RemoteLogoutFailureKind = "unavailable"
)

// RemoteLogoutFailure is the safe runtime error passed across the application
// port. FloodWait is the only class that carries a duration, and its duration
// is validated as positive. All other classes carry no provider-derived data.
type RemoteLogoutFailure struct {
	kind       RemoteLogoutFailureKind
	retryAfter time.Duration
}

// NewRemoteLogoutFailure constructs a validated safe runtime failure. A
// positive retryAfter is required only for FloodWait and is rejected for every
// other class.
func NewRemoteLogoutFailure(kind RemoteLogoutFailureKind, retryAfter time.Duration) (*RemoteLogoutFailure, error) {
	failure := &RemoteLogoutFailure{kind: kind, retryAfter: retryAfter}
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	return failure, nil
}

// Validate confirms that the failure contains exactly one supported bounded
// class and that only FloodWait carries a positive retry duration.
func (failure *RemoteLogoutFailure) Validate() error {
	if failure == nil || !validRemoteLogoutFailureKind(failure.kind) {
		return ErrInvalidRemoteLogoutFailure
	}
	if failure.kind == RemoteLogoutFailureFloodWait {
		if failure.retryAfter <= 0 {
			return ErrInvalidRemoteLogoutFailure
		}
	} else if failure.retryAfter != 0 {
		return ErrInvalidRemoteLogoutFailure
	}
	return nil
}

// Error intentionally has a fixed message and never exposes provider data.
func (failure *RemoteLogoutFailure) Error() string {
	return "telegram account remote logout failure"
}

// Kind returns the validated bounded classification, or Unknown for a nil or
// invalid zero value.
func (failure *RemoteLogoutFailure) Kind() RemoteLogoutFailureKind {
	if failure == nil || failure.Validate() != nil {
		return RemoteLogoutFailureUnknown
	}
	return failure.kind
}

// RetryAfter returns the safe FloodWait duration, or zero for every other
// classification.
func (failure *RemoteLogoutFailure) RetryAfter() time.Duration {
	if failure == nil || failure.Kind() != RemoteLogoutFailureFloodWait {
		return 0
	}
	return failure.retryAfter
}

// Unwrap exposes only a bounded application classification. Retryable classes
// include FloodWait, transient transport failures, and ambiguous response
// loss; ambiguity must remain durable and be retried conservatively.
func (failure *RemoteLogoutFailure) Unwrap() error {
	if failure == nil || failure.Validate() != nil {
		return ErrInvalidRemoteLogoutFailure
	}
	switch failure.kind {
	case RemoteLogoutFailureFloodWait:
		return ErrRemoteLogoutFloodWait
	case RemoteLogoutFailureTransient:
		return ErrRemoteLogoutTransient
	case RemoteLogoutFailureAmbiguous:
		return ErrRemoteLogoutAmbiguous
	case RemoteLogoutFailurePermanent:
		return ErrRemoteLogoutPermanent
	case RemoteLogoutFailureUnavailable:
		return ErrRuntimeUnavailable
	default:
		return ErrInvalidRemoteLogoutFailure
	}
}

func validRemoteLogoutFailureKind(kind RemoteLogoutFailureKind) bool {
	switch kind {
	case RemoteLogoutFailureFloodWait,
		RemoteLogoutFailureTransient,
		RemoteLogoutFailureAmbiguous,
		RemoteLogoutFailurePermanent,
		RemoteLogoutFailureUnavailable:
		return true
	default:
		return false
	}
}

var _ error = (*RemoteLogoutFailure)(nil)
