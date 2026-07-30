package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/coordinates"
)

// Claims ready mailing deliveries from PostgreSQL
type claims struct {
	database *pgxpool.Pool
	lease    time.Duration
}

func NewClaims(database *pgxpool.Pool, lease time.Duration) delivery.Claims {
	return claims{
		database: database,
		lease:    lease,
	}
}

func (claims claims) Claim(context context.Context) (delivery.Task, error) {
	if context == nil {
		panic("claim PostgreSQL delivery without context")
	}
	if claims.database == nil {
		panic("claim PostgreSQL delivery without database")
	}
	if claims.lease <= 0 {
		panic("claim PostgreSQL delivery with invalid lease")
	}
	token := uuid.New()
	seconds := claims.lease.Seconds()
	var mailingID uuid.UUID
	var runID uuid.UUID
	var recipientID uuid.UUID
	var accountID uuid.UUID
	failure := claims.database.QueryRow(
		context,
		`WITH candidate AS (
		    SELECT delivery.mailing_id,
		           delivery.run_id,
		           delivery.recipient_id,
		           run.execution_generation
		    FROM mailing_deliveries AS delivery
		    JOIN mailings AS mailing
		      ON mailing.id = delivery.mailing_id
		    JOIN mailing_runs AS run
		      ON run.mailing_id = delivery.mailing_id
		     AND run.id = delivery.run_id
		    JOIN telegram_mailing_routes AS route
		      ON route.mailing_id = delivery.mailing_id
		    WHERE delivery.status = 'pending'
		      AND delivery.ready_at <= CURRENT_TIMESTAMP
		      AND delivery.attempt_count < delivery.max_attempts
		      AND (delivery.lease_until IS NULL OR delivery.lease_until < CURRENT_TIMESTAMP)
		      AND (
		            (mailing.status = 'queued' AND run.status = 'queued')
		         OR (mailing.status = 'running' AND run.status = 'running')
		      )
		    ORDER BY delivery.ready_at,
		             delivery.mailing_id,
		             delivery.run_id,
		             delivery.recipient_id
		    FOR UPDATE OF mailing, run, delivery SKIP LOCKED
		    LIMIT 1
		)
		UPDATE mailing_deliveries AS delivery
		SET lease_until = CURRENT_TIMESTAMP + ($1::double precision * INTERVAL '1 second'),
		    lease_token = $2,
		    lease_execution_generation = candidate.execution_generation,
		    updated_at = CURRENT_TIMESTAMP
		FROM candidate
		WHERE delivery.mailing_id = candidate.mailing_id
		  AND delivery.run_id = candidate.run_id
		  AND delivery.recipient_id = candidate.recipient_id
		RETURNING delivery.mailing_id,
		          delivery.run_id,
		          delivery.recipient_id,
		          (SELECT route.account_id
		             FROM telegram_mailing_routes AS route
		            WHERE route.mailing_id = delivery.mailing_id)`,
		seconds,
		token,
	).Scan(&mailingID, &runID, &recipientID, &accountID)
	if errors.Is(failure, pgx.ErrNoRows) {
		return nil, delivery.ErrEmpty
	}
	if failure != nil {
		return nil, fmt.Errorf("claim mailing delivery: %w", failure)
	}
	return claimed{
		database: claims.database,
		route:    delivery.Routing(accountID),
		identity: coordinates.New(
			mailing.Identity(mailingID),
			mailing.Run(runID),
			recipient.Identifier(recipientID),
		),
		token: delivery.Fence(token),
	}, nil
}
