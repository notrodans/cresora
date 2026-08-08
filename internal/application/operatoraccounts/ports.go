package operatoraccounts

import (
	"context"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// AccountLifecycleReader loads one current-only lifecycle snapshot in the
// actor's ownership scope. Implementations should return ErrAccountNotFound
// for an unknown or foreign account unless their boundary explicitly needs to
// distinguish the cases.
type AccountLifecycleReader interface {
	LoadAccount(context.Context, application.Actor, operatoraccount.ID) (operatoraccount.Account, error)
}

// AccountLifecycleWriter persists a state change produced by one of the
// explicit domain operations. The domain graph is authenticating, active,
// reauth_required, disconnected, and disconnecting; expectedVersion is the
// version observed before the operation. Implementations must reject stale
// writes with ErrAccountVersionConflict and return ErrAccountNotFound when the
// actor does not own the account.
//
// PersistLifecycle is intentionally narrower than a generic account write: it
// can persist only the current lifecycle snapshot and its optimistic version.
type AccountLifecycleWriter interface {
	PersistLifecycle(
		context.Context,
		application.Actor,
		operatoraccount.Account,
		operatoraccount.Version,
	) error
}

// AccountLifecycleRepository is the complete persistence port needed by a
// lifecycle use case. Consumers that only read or write should depend on the
// smaller interface above instead.
type AccountLifecycleRepository interface {
	AccountLifecycleReader
	AccountLifecycleWriter
}

// RemoteLogoutIntentLister returns only persisted disconnecting accounts whose
// remote Telegram session still needs revocation. The returned target carries
// the actor-owned account identity and the exact persisted lifecycle version;
// callers must not broaden this query to all disconnecting accounts.
type RemoteLogoutIntentLister interface {
	ListRemoteLogoutIntents(context.Context) ([]RuntimeTarget, error)
}

// DisconnectPersistence is the deliberately small persistence boundary used by
// disconnect and startup recovery. Its final PersistLifecycle call is where an
// adapter performs the disconnected-state write and transactional session
// deletion; no session-deletion port is needed by this workflow.
type DisconnectPersistence interface {
	AccountLifecycleReader
	AccountLifecycleWriter
	RemoteLogoutIntentLister
}

// RuntimeRevoker owns one version-fenced remote logout and local runtime stop.
// Its concrete result type prevents arbitrary provider errors from crossing
// the boundary. Implementations must translate cancellation, provider status,
// and runtime failures to RevokeOutcome constructors; gotd and other
// transport types must not cross this boundary.
type RuntimeRevoker interface {
	RevokeAndStop(context.Context, RuntimeTarget) RevokeOutcome
}

// RuntimeStopper owns the local runtime fence and teardown for one exact
// lifecycle target. It deliberately has no remote logout capability; local
// force-forget must never be able to reach Telegram auth.LogOut.
type RuntimeStopper interface {
	StopAccount(context.Context, RuntimeTarget) error
}

// ForceForgetPersistence is the dedicated persistence boundary for the local
// force-forget command. PersistForceForget must transition the exact
// disconnecting version, delete its encrypted session, and insert the audit
// event in one transaction. The bool result reports whether the supplied
// idempotency key had already been applied.
type ForceForgetPersistence interface {
	AccountLifecycleReader
	ForceForgetAlreadyApplied(context.Context, application.Actor, operatoraccount.ID, uuid.UUID) (bool, error)
	PersistForceForget(
		context.Context,
		application.Actor,
		operatoraccount.Account,
		operatoraccount.Version,
		uuid.UUID,
	) (bool, error)
}

// SessionDeleter removes the persisted Telegram session for one actor-owned
// domain account, including the final disconnect transition's transactional
// deletion. SessionScope and Telegram transport types are deliberately absent;
// an adapter performs that translation at the infrastructure boundary.
// Implementations return ErrAccountNotFound for an unknown or foreign account
// and treat a missing session on an owned account as an idempotent success.
// Standalone deletion must return ErrAccountStateConflict without changing
// state when the account is active. Cleanup is permitted for authenticating,
// reauth_required, disconnected, and disconnecting accounts; the final
// disconnect transition remains transactional with its session deletion.
type SessionDeleter interface {
	DeleteSession(context.Context, application.Actor, operatoraccount.ID) error
}
