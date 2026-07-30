package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/coordinates"
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

func (task claimed) Renew(context context.Context, duration time.Duration) error {
	if context == nil {
		panic("renew claimed delivery without context")
	}
	if task.database == nil {
		panic("renew claimed delivery without database")
	}
	if duration <= 0 {
		panic("renew claimed delivery with invalid lease")
	}

	transaction, failure := task.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin delivery lease renewal transaction: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var mailingStatus string
	failure = transaction.QueryRow(
		context,
		`SELECT status::text
		 FROM mailings
		 WHERE id = $1
		 FOR UPDATE`,
		task.identity.Mailing().UUID(),
	).Scan(&mailingStatus)
	if errors.Is(failure, pgx.ErrNoRows) {
		return delivery.ErrLeaseLost
	}
	if failure != nil {
		return fmt.Errorf("lock mailing for delivery lease renewal: %w", failure)
	}

	var runStatus string
	var generation int64
	failure = transaction.QueryRow(
		context,
		`SELECT status::text, execution_generation
		 FROM mailing_runs
		 WHERE mailing_id = $1
		   AND id = $2
		 FOR UPDATE`,
		task.identity.Mailing().UUID(),
		task.identity.Run().UUID(),
	).Scan(&runStatus, &generation)
	if errors.Is(failure, pgx.ErrNoRows) {
		return delivery.ErrLeaseLost
	}
	if failure != nil {
		return fmt.Errorf("lock mailing run for delivery lease renewal: %w", failure)
	}
	if !((mailingStatus == "queued" && runStatus == "queued") ||
		(mailingStatus == "running" && runStatus == "running")) {
		return delivery.ErrLeaseLost
	}

	result, failure := transaction.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET lease_until = clock_timestamp() + ($5::double precision * INTERVAL '1 second')
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND status = 'pending'
		   AND lease_token = $4
		   AND lease_until > clock_timestamp()
		   AND lease_execution_generation = $6`,
		task.identity.Mailing().UUID(),
		task.identity.Run().UUID(),
		task.identity.Recipient().UUID(),
		task.token.UUID(),
		duration.Seconds(),
		generation,
	)
	if failure != nil {
		return fmt.Errorf("renew delivery lease: %w", failure)
	}
	if result.RowsAffected() == 0 {
		return delivery.ErrLeaseLost
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit delivery lease renewal: %w", failure)
	}
	return nil
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
		 SET ready_at = CURRENT_TIMESTAMP + INTERVAL '5 seconds',
		     lease_until = NULL,
		     lease_token = NULL,
		     lease_execution_generation = NULL,
		     error_message = $5,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND status = 'pending'
		   AND lease_token = $4`,
		task.identity.Mailing().UUID(),
		task.identity.Run().UUID(),
		task.identity.Recipient().UUID(),
		task.token.UUID(),
		delivery.BoundedErrorMessage(cause),
	)
	if failure != nil {
		return fmt.Errorf("release claimed mailing delivery: %w", failure)
	}
	return nil
}
