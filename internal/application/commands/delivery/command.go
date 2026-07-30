package delivery

import (
	"context"
	"fmt"

	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/recipient"
)

// Composes persistent deliveries with one transport
type operation struct {
	deliveries Deliveries
	port       Port
}

func New(deliveries Deliveries, port Port) Command {
	return operation{
		deliveries: deliveries,
		port:       port,
	}
}

func (operation operation) Execute(
	context context.Context,
	mailingID mailing.ID,
	runID mailing.RunID,
	recipientID recipient.ID,
	token Token,
) error {
	if failure := operation.deliveries.
		Delivery(mailingID, runID, recipientID, token).
		Dispatch(context, operation.port); failure != nil {
		return fmt.Errorf("execute mailing delivery: %w", failure)
	}
	return nil
}
