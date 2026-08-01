package delivery

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/domain/recipient"
)

var (
	ErrEmpty               = errors.New("no ready mailing deliveries")
	ErrLeaseLost           = errors.New("delivery lease lost")
	ErrOutcomeFinalization = errors.New("delivery outcome finalization failed")
)

const OutcomeFinalizationTimeout = 2 * time.Second

// Identifies an execution lane such as an account
type Route uuid.UUID

// Fences one claimed delivery lease
type Token uuid.UUID

func Routing(value uuid.UUID) Route {
	return Route(value)
}

func Fence(value uuid.UUID) Token {
	return Token(value)
}

func (route Route) UUID() uuid.UUID {
	return uuid.UUID(route)
}

func (token Token) UUID() uuid.UUID {
	return uuid.UUID(token)
}

// AccountAdmission is the immutable account lifecycle snapshot captured when
// a delivery is claimed. Route identifies the account and Version fences later
// runtime work to the exact lifecycle snapshot that was admitted.
type AccountAdmission struct {
	Route   Route
	Version operatoraccount.Version
}

// AccountRevalidationReader revalidates a claimed account admission before a
// transport runtime is used. Implementations must return a target only when
// the same account is still active at the admitted version.
type AccountRevalidationReader interface {
	Revalidate(context.Context, AccountAdmission) (operatoraccounts.RuntimeTarget, error)
}

// AdmittedTask is an optional extension of Task for consumers that need the
// lifecycle snapshot captured by a claim. Task retains Route for existing
// routing consumers.
type AdmittedTask interface {
	Task
	Admission() AccountAdmission
}

// Sends one domain message through an outer port. randomID is the persisted
// Telegram idempotency key for the logical delivery and must be reused for
// every admitted retry.
type Port interface {
	Send(context.Context, recipient.Recipient, message.Message, int64) error
}

// Represents all persistent mailing deliveries
type Deliveries interface {
	Delivery(mailing.ID, mailing.RunID, recipient.ID, Token) Delivery
}

// Represents one claimed persistent delivery
type Delivery interface {
	Dispatch(context.Context, Port) error
}

// Executes one claimed delivery
type Command interface {
	Execute(
		context.Context,
		mailing.ID,
		mailing.RunID,
		recipient.ID,
		Token,
	) error
}

// Represents one claimed background task
type Task interface {
	Route() Route
	Renew(context.Context, time.Duration) error
	Execute(context.Context, Command) error
	Release(context.Context, error) error
}

// Claims persistent delivery tasks
type Claims interface {
	Claim(context.Context) (Task, error)
}

// ReapResult reports the bounded set of expired sending deliveries handled by
// one reaper pass.
type ReapResult struct {
	Retried int
	Unknown int
}

// Reaper reclaims expired sending delivery leases without contacting a
// transport.
type Reaper interface {
	Reap(context.Context) (ReapResult, error)
}
