package operatoraccounts

import (
	"context"
	"errors"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// ErrAccountStateConflict indicates that an operation is not permitted while
// the owned account is in its current lifecycle state.
var ErrAccountStateConflict = errors.New("operator account lifecycle state conflict")

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
