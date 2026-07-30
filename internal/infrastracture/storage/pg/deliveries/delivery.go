package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/coordinates"
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
	if port == nil {
		panic("dispatch PostgreSQL delivery without port")
	}
	var body string
	var random int64
	var admittedAttempt int
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
		   AND delivery.attempt_count < delivery.max_attempts
		   AND (
		         (mailing.status = 'queued' AND run.status = 'queued')
		      OR (mailing.status = 'running' AND run.status = 'running')
		   )
		 RETURNING mailing.message_text, telegram.random_id, delivery.attempt_count`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
	).Scan(&body, &random, &admittedAttempt)
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
		if errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded) {
			if record := entity.quarantine(
				finalizationContext,
				failure,
				admittedAttempt,
				random,
			); record != nil {
				return fmt.Errorf(
					"%w: quarantine canceled mailing delivery after %v: %w",
					application.ErrOutcomeFinalization,
					failure,
					record,
				)
			}
			return nil
		}
		if record := entity.finalizeNegative(
			finalizationContext,
			failure,
			admittedAttempt,
			random,
		); record != nil {
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
		`UPDATE mailing_deliveries AS delivery
		 SET status = 'sent',
		     sent_at = CURRENT_TIMESTAMP,
		     lease_until = NULL,
		     lease_token = NULL,
		     lease_execution_generation = NULL,
		     skip_reason = NULL,
		     error_message = NULL,
		     updated_at = CURRENT_TIMESTAMP
		 FROM telegram_mailing_deliveries AS telegram
		 WHERE delivery.mailing_id = $1
		   AND delivery.run_id = $2
		   AND delivery.recipient_id = $3
		   AND telegram.mailing_id = delivery.mailing_id
		   AND telegram.run_id = delivery.run_id
		   AND telegram.recipient_id = delivery.recipient_id
		   AND telegram.random_id = $4
		   AND delivery.attempt_count >= $5
		   AND delivery.started_at IS NOT NULL
		   AND delivery.status IN ('pending', 'sending', 'unknown', 'failed', 'skipped')`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		random,
		admittedAttempt,
	)
	if failure != nil {
		return fmt.Errorf("%w: mark mailing delivery sent: %w", application.ErrOutcomeFinalization, failure)
	}
	if result.RowsAffected() == 0 {
		var (
			status         string
			persistedID    int64
			persistedCount int
			started        bool
		)
		failure = entity.database.QueryRow(
			finalizationContext,
			`SELECT delivery.status::text,
			        telegram.random_id,
			        delivery.attempt_count,
			        delivery.started_at IS NOT NULL
			 FROM mailing_deliveries AS delivery
			 JOIN telegram_mailing_deliveries AS telegram
			   ON telegram.mailing_id = delivery.mailing_id
			  AND telegram.run_id = delivery.run_id
			  AND telegram.recipient_id = delivery.recipient_id
			 WHERE delivery.mailing_id = $1
			   AND delivery.run_id = $2
			   AND delivery.recipient_id = $3`,
			entity.identity.Mailing().UUID(),
			entity.identity.Run().UUID(),
			entity.identity.Recipient().UUID(),
		).Scan(&status, &persistedID, &persistedCount, &started)
		if failure != nil {
			return fmt.Errorf("%w: verify already-sent mailing delivery: %w", application.ErrOutcomeFinalization, failure)
		}
		if status != "sent" || persistedID != random || persistedCount < admittedAttempt || !started {
			return fmt.Errorf(
				"%w: verify already-sent mailing delivery proof (status=%s random_id=%d attempt_count=%d started=%t)",
				application.ErrOutcomeFinalization,
				status,
				persistedID,
				persistedCount,
				started,
			)
		}
	}
	return nil
}

func (entity persistentDelivery) finalizeNegative(
	context context.Context,
	cause error,
	admittedAttempt int,
	random int64,
) error {
	classification := application.Classify(cause)
	kind := "unknown"
	delay := time.Duration(0)
	switch classification.Kind {
	case application.FailurePermanent:
		kind = "permanent"
	case application.FailureTransient:
		kind = "transient"
		delay = 5 * time.Second
	case application.FailureFloodWait:
		kind = "flood_wait"
		delay = classification.RetryAfter
	}

	transaction, failure := entity.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin classified mailing delivery finalization transaction: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var mailingStatus string
	failure = transaction.QueryRow(
		context,
		`SELECT status::text
		 FROM mailings
		 WHERE id = $1
		 FOR UPDATE`,
		entity.identity.Mailing().UUID(),
	).Scan(&mailingStatus)
	if errors.Is(failure, pgx.ErrNoRows) {
		return nil
	}
	if failure != nil {
		return fmt.Errorf("lock mailing for classified delivery finalization: %w", failure)
	}

	var (
		runStatus  string
		generation int64
	)
	failure = transaction.QueryRow(
		context,
		`SELECT status::text, execution_generation
		 FROM mailing_runs
		 WHERE mailing_id = $1
		   AND id = $2
		 FOR UPDATE`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
	).Scan(&runStatus, &generation)
	if errors.Is(failure, pgx.ErrNoRows) {
		return nil
	}
	if failure != nil {
		return fmt.Errorf("lock mailing run for classified delivery finalization: %w", failure)
	}

	active := (mailingStatus == "queued" && runStatus == "queued") ||
		(mailingStatus == "running" && runStatus == "running")
	_, failure = transaction.Exec(
		context,
		`UPDATE mailing_deliveries AS delivery
		 SET status = CASE
		                 WHEN delivery.lease_execution_generation IS DISTINCT FROM $10
			               OR NOT $11::boolean
			               THEN 'unknown'::mailing_delivery_status_type
		                 WHEN $7::text = 'unknown'
		                   THEN 'unknown'::mailing_delivery_status_type
		                 WHEN $7::text = 'permanent'
		                   THEN 'failed'::mailing_delivery_status_type
		                 WHEN delivery.attempt_count >= delivery.max_attempts
		                   THEN 'failed'::mailing_delivery_status_type
		                 ELSE 'pending'::mailing_delivery_status_type
		             END,
		     ready_at = CASE
		                    WHEN $7::text IN ('transient', 'flood_wait')
		                     AND delivery.attempt_count < delivery.max_attempts
		                     AND delivery.lease_execution_generation = $10
		                     AND $11::boolean
		                      THEN CURRENT_TIMESTAMP + ($8::double precision * INTERVAL '1 second')
		                    ELSE delivery.ready_at
		                END,
		     sent_at = NULL,
		     skip_reason = NULL,
		     lease_until = NULL,
		     lease_token = NULL,
		     lease_execution_generation = NULL,
		     error_message = $9,
		     updated_at = CURRENT_TIMESTAMP
		 FROM telegram_mailing_deliveries AS telegram
		 WHERE delivery.mailing_id = $1
		   AND delivery.run_id = $2
		   AND delivery.recipient_id = $3
		   AND telegram.mailing_id = delivery.mailing_id
		   AND telegram.run_id = delivery.run_id
		   AND telegram.recipient_id = delivery.recipient_id
		   AND telegram.random_id = $6
		   AND delivery.status = 'sending'
		   AND delivery.lease_token = $4
		   AND delivery.attempt_count = $5
		   AND delivery.started_at IS NOT NULL`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
		admittedAttempt,
		random,
		kind,
		delay.Seconds(),
		application.BoundedErrorMessage(cause),
		generation,
		active,
	)
	if failure != nil {
		return fmt.Errorf("persist classified mailing delivery failure: %w", failure)
	}
	// A zero-row update is the expected result for a stale negative outcome:
	// the reaper, a newer attempt, or a late success owns the row now.
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit classified mailing delivery finalization transaction: %w", failure)
	}
	return nil
}

func (entity persistentDelivery) quarantine(
	context context.Context,
	cause error,
	admittedAttempt int,
	random int64,
) error {
	_, failure := entity.database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET error_message = $7,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND recipient_id = $3
		   AND EXISTS (
		       SELECT 1
		       FROM telegram_mailing_deliveries AS telegram
		       WHERE telegram.mailing_id = mailing_deliveries.mailing_id
		         AND telegram.run_id = mailing_deliveries.run_id
		         AND telegram.recipient_id = mailing_deliveries.recipient_id
		         AND telegram.random_id = $6
		   )
		   AND status = 'sending'
		   AND lease_token = $4
		   AND attempt_count = $5
		   AND started_at IS NOT NULL`,
		entity.identity.Mailing().UUID(),
		entity.identity.Run().UUID(),
		entity.identity.Recipient().UUID(),
		entity.token.UUID(),
		admittedAttempt,
		random,
		application.BoundedErrorMessage(cause),
	)
	if failure != nil {
		return fmt.Errorf("quarantine mailing delivery failure: %w", failure)
	}
	// A stale failure is deliberately a no-op. The lease may have been reaped,
	// replaced by a newer attempt, or finalized by a late success.
	return nil
}
