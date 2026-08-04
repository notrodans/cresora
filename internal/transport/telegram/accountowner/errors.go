package accountowner

import "errors"

var (
	// ErrRegistryStopped means that the process-wide runtime no longer admits
	// new account owners.
	ErrRegistryStopped = errors.New("telegram account runtime stopped")
	// ErrAccountStopped means that admission for this operator/account scope was
	// closed before the operation could begin.
	ErrAccountStopped = errors.New("telegram account runtime stopped for account")
	// ErrStaleAdmission means that an operation used an older lifecycle version
	// than the currently admitted version.
	ErrStaleAdmission = errors.New("telegram account runtime admission is stale")
	// ErrInvalidAdmission means that the requested lifecycle state cannot own a
	// runtime through this boundary.
	ErrInvalidAdmission = errors.New("telegram account runtime admission is invalid")
	// ErrRuntimeCapacity means that all runtime slots are occupied by accounts
	// that are not idle and therefore cannot be evicted safely.
	ErrRuntimeCapacity = errors.New("telegram account runtime capacity is exhausted")
	// ErrFenceCapacity means that a new stop fence cannot be recorded without
	// evicting a protected fence for an account that is still stopping.
	ErrFenceCapacity = errors.New("telegram account runtime fence capacity is exhausted")
	// errRevokeCallbackPanicked is kept private so a panic from a privileged
	// callback can be recovered long enough to tear down its owner and then be
	// re-raised to the caller.
	errRevokeCallbackPanicked = errors.New("telegram account revoke callback panicked")
)
