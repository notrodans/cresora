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
	context context.Context,
	port application.Port,
) error {
	if context == nil {
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
		context,
		`SELECT mailing.message_text, telegram.random_id
		 FROM mailing_deliveries AS delivery
		 JOIN mailings AS mailing
		   ON mailing.id = delivery.mailing_id
		 JOIN telegram_mailing_deliveries AS telegram
		   ON telegram.mailing_id = delivery.mailing_id
		  AND telegram.run_id = delivery.run_id
		  AND telegram.recipient_id = delivery.recipient_id
		 WHERE delivery.mailing_id = $1
		   AND delivery.run_id = $2
		   AND delivery.recipient_id = $3
		   AND delivery.status = 'sending'
		   AND delivery.lease_token = $4`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
	).Scan(&body, &random)
	if errors.Is(failure, pgx.ErrNoRows) {
		return errStale
	}
	if failure != nil {
		return fmt.Errorf("load claimed mailing delivery: %w", failure)
	}
	failure = port.Send(
		context,
		recipient.Identity(entity.identity.Recipient().UUID()),
		message.Text(body),
		random,
	)
	if failure != nil {
		if record := entity.reject(context, failure); record != nil {
			return fmt.Errorf("record failed mailing delivery after %v: %w", failure, record)
		}
		return nil
	}
	result, failure := entity.database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET status = 'sent',
		     sent_at = CURRENT_TIMESTAMP,
		     lease_until = NULL,
		     lease_token = NULL,
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
		return fmt.Errorf("mark mailing delivery sent: %w", failure)
	}
	if result.RowsAffected() == 0 {
		return errStale
	}
	return nil
}

func (entity persistentDelivery) reject(context context.Context, cause error) error {
	result, failure := entity.database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET status = CASE
		         WHEN attempt_count >= max_attempts THEN 'failed'::mailing_delivery_status_type
		         ELSE 'pending'::mailing_delivery_status_type
		     END,
		     ready_at = CASE
		         WHEN attempt_count >= max_attempts THEN ready_at
		         ELSE CURRENT_TIMESTAMP + LEAST(
		             INTERVAL '5 minutes',
		             INTERVAL '5 seconds' * power(2, GREATEST(attempt_count - 1, 0))
		         )
		     END,
		     lease_until = NULL,
		     lease_token = NULL,
		     error_message = $5,
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
		return fmt.Errorf("record mailing delivery failure: %w", failure)
	}
	if result.RowsAffected() == 0 {
		return errStale
	}
	return nil
}
