package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/coordinates"
)

// Represents one leased PostgreSQL delivery task
type claimed struct {
	database *pgxpool.Pool
	route    delivery.Route
	identity coordinates.Coordinates
	token    delivery.Token
}

func (task claimed) Route() delivery.Route {
	return task.route
}

func (task claimed) Execute(
	context context.Context,
	command delivery.Command,
) error {
	if command == nil {
		panic("execute claimed delivery without command")
	}
	return command.Execute(
		context,
		task.identity.Mailing(),
		task.identity.Run(),
		task.identity.Recipient(),
		task.token,
	)
}

func (task claimed) Release(context context.Context, cause error) error {
	if context == nil {
		panic("release claimed delivery without context")
	}
	if task.database == nil {
		panic("release claimed delivery without database")
	}
	if cause == nil {
		panic("release claimed delivery without cause")
	}
	_, failure := task.database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET status = 'pending',
		     ready_at = CURRENT_TIMESTAMP + INTERVAL '5 seconds',
		     lease_until = NULL,
		     lease_token = NULL,
		     error_message = $5,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND status = 'sending'
		   AND lease_token = $4`,
		task.identity.Mailing().UUID(),
		task.identity.Run().UUID(),
		task.identity.Recipient().UUID(),
		task.token.UUID(),
		cause.Error(),
	)
	if failure != nil {
		return fmt.Errorf("release claimed mailing delivery: %w", failure)
	}
	return nil
}
