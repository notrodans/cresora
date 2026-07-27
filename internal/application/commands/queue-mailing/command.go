package queuemailing

import (
	"context"
	"fmt"

	"github.com/notrodans/nebula-go/internal/domain/mailing"
)

// Выполняет постановку одной рассылки в очередь
type Command interface {
	Execute(context.Context, mailing.ID) error
}

// Ставит постоянную рассылку в очередь
type operation struct {
	mailings mailing.Mailings
}

func New(mailings mailing.Mailings) Command {
	return operation{
		mailings: mailings,
	}
}

func (operation operation) Execute(
	context context.Context,
	identity mailing.ID,
) error {
	if context == nil {
		panic("execute mailing dispatch without context")
	}
	if operation.mailings == nil {
		panic("execute mailing dispatch without mailings")
	}
	if failure := operation.mailings.
		Mailing(identity).
		Queue(context); failure != nil {
		return fmt.Errorf(
			"execute mailing %s dispatch: %w",
			identity.UUID(),
			failure,
		)
	}
	return nil
}
