package operatoraccounts

// RevokeOutcome is the closed application result of one version-fenced remote
// revoke-and-stop operation. Its zero value is invalid so an adapter cannot
// accidentally turn an uninitialized result into success.
type RevokeOutcome struct {
	kind    revokeOutcomeKind
	failure *RemoteLogoutFailure
}

type revokeOutcomeKind uint8

const (
	revokeOutcomeInvalid revokeOutcomeKind = iota
	revokeOutcomeSucceeded
	revokeOutcomeAlreadyComplete
	revokeOutcomeFailed
)

// RevokeSucceeded reports that the remote revoke and local runtime stop
// converged.
func RevokeSucceeded() RevokeOutcome {
	return RevokeOutcome{kind: revokeOutcomeSucceeded}
}

// RevokeAlreadyComplete reports semantic convergence: the remote session was
// already absent and the durable disconnect may be completed.
func RevokeAlreadyComplete() RevokeOutcome {
	return RevokeOutcome{kind: revokeOutcomeAlreadyComplete}
}

// NewRevokeFailure constructs a failed result only from a validated bounded
// remote failure. The value is copied so callers cannot mutate the result's
// diagnostic after construction.
func NewRevokeFailure(failure *RemoteLogoutFailure) (RevokeOutcome, error) {
	if err := failure.Validate(); err != nil {
		return RevokeOutcome{}, err
	}
	copy := *failure
	return RevokeOutcome{kind: revokeOutcomeFailed, failure: &copy}, nil
}

// Validate rejects malformed adapter results before application code can use
// them. Invalid results are protocol/configuration failures, never remote
// failures and never successful convergence.
func (outcome RevokeOutcome) Validate() error {
	switch outcome.kind {
	case revokeOutcomeSucceeded, revokeOutcomeAlreadyComplete:
		if outcome.failure != nil {
			return ErrInvalidRuntimeOutcome
		}
		return nil
	case revokeOutcomeFailed:
		if outcome.failure == nil {
			return ErrInvalidRuntimeOutcome
		}
		if err := outcome.failure.Validate(); err != nil {
			return ErrInvalidRuntimeOutcome
		}
		return nil
	default:
		return ErrInvalidRuntimeOutcome
	}
}

// Converged reports whether the valid result is either direct or semantic
// convergence. Callers should validate the outcome before using this method.
func (outcome RevokeOutcome) Converged() bool {
	return outcome.failure == nil &&
		(outcome.kind == revokeOutcomeSucceeded || outcome.kind == revokeOutcomeAlreadyComplete)
}

// Failure returns a copy of the validated bounded failure, if this result is a
// failed outcome. It never exposes mutable adapter-owned state.
func (outcome RevokeOutcome) Failure() (*RemoteLogoutFailure, bool) {
	if outcome.kind != revokeOutcomeFailed || outcome.failure == nil || outcome.failure.Validate() != nil {
		return nil, false
	}
	copy := *outcome.failure
	return &copy, true
}
