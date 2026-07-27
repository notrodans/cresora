package delivery

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/notrodans/nebula-go/internal/domain/mailing"
	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

var ErrEmpty = errors.New("no ready mailing deliveries")

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

// Sends one domain message through an outer port
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
	Execute(context.Context, Command) error
	Release(context.Context, error) error
}

// Claims persistent delivery tasks
type Claims interface {
	Claim(context.Context) (Task, error)
}
