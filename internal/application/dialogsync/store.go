package dialogsync

import (
	"context"
	"time"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// Fetcher pulls an account's shared and private dialogs. Implementations
// perform the fetch inside the account runtime admission and return only
// transport-neutral dialogs or application failure send values.
type Fetcher interface {
	Fetch(context.Context, operatoraccounts.RuntimeTarget) ([]SharedDialog, []PrivateDialog, error)
}

// Store is the durable boundary consumed by the dialog sync worker. Claims
// and lifecycle/lease transitions are owned by PostgreSQL.
type Store interface {
	// Claim leases at most one claimable account dialog sync. It returns
	// ErrEmpty when none is ready.
	Claim(context.Context, time.Duration) (Task, error)
	// Backfill ensures every currently active account has a pending dialog sync
	// row. It returns the number of rows created and is safe to call repeatedly.
	Backfill(context.Context) (int, error)
}

// Task is one leased account dialog synchronization.
type Task interface {
	// Key identifies the claimed account and the lifecycle snapshot captured at
	// claim time.
	Key() TaskKey
	// Revalidate returns the canonical runtime target only when the account is
	// still active at the claimed lifecycle version. A missing or advanced
	// account returns operatoraccounts.ErrAccountNotFound.
	Revalidate(context.Context) (operatoraccounts.RuntimeTarget, error)
	// Renew extends the claim's lease. It returns ErrLeaseLost when the claim or
	// its lifecycle snapshot is no longer valid.
	Renew(context.Context, time.Duration) error
	// Complete atomically persists the fetched dialogs and marks the sync done.
	Complete(context.Context, []SharedDialog, []PrivateDialog) error
	// Retry marks the attempt failed transiently and schedules a retry at the
	// supplied delay unless the attempt budget is exhausted.
	Retry(context.Context, error, time.Duration) error
	// Fail marks the sync permanently failed with the given diagnostic.
	Fail(context.Context, error) error
	// Release returns an interrupted or inconclusive attempt to a retryable
	// pending state without consuming the attempt budget.
	Release(context.Context, error) error
}
