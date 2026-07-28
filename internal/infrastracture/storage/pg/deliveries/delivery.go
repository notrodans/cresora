package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
	"github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/coordinates"
)

var errStale = errors.New("mailing delivery lease is stale")

// Represents one claimed PostgreSQL delivery
type persistentDelivery struct {
	database *pgxpool.Pool
	identity coordinates.Coordinates
	token    application.Token
}

func (entity persistentDelivery) Dispatch(
	executionContext context.Context,
	port application.Port,
) error {
	if executionContext == nil {
		panic("dispatch PostgreSQL delivery without context")
	}
	if entity.database == nil {
		panic("dispatch PostgreSQL delivery without database")
	}
	if port == nil {
		panic("dispatch PostgreSQL delivery without port")
	}
	var body string
	var random int64
	failure := entity.database.QueryRow(
		executionContext,
		`UPDATE mailing_deliveries AS delivery
		 SET status = 'sending',
		     started_at = CURRENT_TIMESTAMP,
		     attempt_count = delivery.attempt_count + 1,
		     updated_at = CURRENT_TIMESTAMP
		 FROM mailings AS mailing,
		      mailing_runs AS run,
		      telegram_mailing_deliveries AS telegram
		 WHERE delivery.mailing_id = $1
		   AND delivery.run_id = $2
		   AND delivery.recipient_id = $3
		   AND mailing.id = delivery.mailing_id
		   AND run.mailing_id = delivery.mailing_id
		   AND run.id = delivery.run_id
		   AND telegram.mailing_id = delivery.mailing_id
		   AND telegram.run_id = delivery.run_id
		   AND telegram.recipient_id = delivery.recipient_id
		   AND delivery.status = 'pending'
		   AND delivery.lease_token = $4
		   AND delivery.lease_until > CURRENT_TIMESTAMP
		   AND delivery.lease_execution_generation = run.execution_generation
		   AND (
		         (mailing.status = 'queued' AND run.status = 'queued')
		      OR (mailing.status = 'running' AND run.status = 'running')
		   )
		 RETURNING mailing.message_text, telegram.random_id`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
	).Scan(&body, &random)
	if errors.Is(failure, pgx.ErrNoRows) {
		return errStale
	}
	if failure != nil {
		return fmt.Errorf("admit mailing delivery: %w", failure)
	}
	failure = port.Send(
		executionContext,
		recipient.Identity(entity.identity.Recipient().UUID()),
		message.Text(body),
		random,
	)
	finalizationContext, cancelFinalization := context.WithTimeout(
		context.WithoutCancel(executionContext),
		application.OutcomeFinalizationTimeout,
	)
	defer cancelFinalization()
	if failure != nil {
		if record := entity.quarantine(finalizationContext, failure); record != nil {
			return fmt.Errorf(
				"%w: quarantine mailing delivery after %v: %w",
				application.ErrOutcomeFinalization,
				failure,
				record,
			)
		}
		return nil
	}
	result, failure := entity.database.Exec(
		finalizationContext,
		`UPDATE mailing_deliveries
		 SET status = 'sent',
		     sent_at = CURRENT_TIMESTAMP,
		     lease_until = NULL,
		     lease_token = NULL,
		     lease_execution_generation = NULL,
		     error_message = NULL,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND status = 'sending'
		   AND lease_token = $4`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
	)
	if failure != nil {
		return fmt.Errorf("%w: mark mailing delivery sent: %w", application.ErrOutcomeFinalization, failure)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %w", application.ErrOutcomeFinalization, errStale)
	}
	return nil
}

func (entity persistentDelivery) quarantine(context context.Context, cause error) error {
	result, failure := entity.database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET error_message = $5,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND status = 'sending'
		   AND lease_token = $4`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
		cause.Error(),
	)
	if failure != nil {
		return fmt.Errorf("quarantine mailing delivery failure: %w", failure)
	}
	if result.RowsAffected() == 0 {
		return errStale
	}
	return nil
}
