package pg

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/coordinates"
)

// Claims ready mailing deliveries from PostgreSQL
type Claims struct {
	database *pgxpool.Pool
	lease    time.Duration
}

var (
	_ delivery.Claims                    = (*Claims)(nil)
	_ delivery.AccountRevalidationReader = (*Claims)(nil)
)

func NewClaims(database *pgxpool.Pool, lease time.Duration) *Claims {
	return &Claims{
		database: database,
		lease:    lease,
	}
}

func (claims *Claims) Claim(context context.Context) (delivery.Task, error) {
	if claims.lease <= 0 {
		panic("claim PostgreSQL delivery with invalid lease")
	}
	token := uuid.New()
	seconds := claims.lease.Seconds()
	var mailingID uuid.UUID
	var runID uuid.UUID
	var recipientID uuid.UUID
	var accountID uuid.UUID
	var accountVersion int64
	failure := claims.database.QueryRow(
		context,
		`WITH candidate AS (
		    SELECT delivery.mailing_id,
		           delivery.run_id,
		           delivery.recipient_id,
		           run.execution_generation,
		           route.account_id,
		           account.status_version
		    FROM mailing_deliveries AS delivery
		    JOIN mailings AS mailing
		      ON mailing.id = delivery.mailing_id
		    JOIN mailing_runs AS run
		      ON run.mailing_id = delivery.mailing_id
		     AND run.id = delivery.run_id
		    JOIN telegram_mailing_routes AS route
		      ON route.mailing_id = delivery.mailing_id
		    JOIN operator_accounts AS account
		      ON account.id = route.account_id
		     AND account.operator_id = mailing.operator_id
		    WHERE delivery.status = 'pending'
		      AND delivery.ready_at <= CURRENT_TIMESTAMP
		      AND delivery.attempt_count < delivery.max_attempts
		      AND (delivery.lease_until IS NULL OR delivery.lease_until < CURRENT_TIMESTAMP)
		      AND (
		            (mailing.status = 'queued' AND run.status = 'queued')
		         OR (mailing.status = 'running' AND run.status = 'running')
		      )
		      AND account.status = 'active'
		    ORDER BY delivery.ready_at,
		             delivery.mailing_id,
		             delivery.run_id,
		             delivery.recipient_id
		    FOR UPDATE OF mailing, run, delivery SKIP LOCKED
		    LIMIT 1
		), claimed AS (
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
		              delivery.recipient_id
		)
		SELECT claimed.mailing_id,
		       claimed.run_id,
		       claimed.recipient_id,
		       candidate.account_id,
		       candidate.status_version
		  FROM claimed
		  JOIN candidate
		    ON candidate.mailing_id = claimed.mailing_id
		   AND candidate.run_id = claimed.run_id
		   AND candidate.recipient_id = claimed.recipient_id`,
		seconds,
		token,
	).Scan(&mailingID, &runID, &recipientID, &accountID, &accountVersion)
	if errors.Is(failure, pgx.ErrNoRows) {
		return nil, delivery.ErrEmpty
	}
	if failure != nil {
		return nil, fmt.Errorf("claim mailing delivery: %w", failure)
	}
	if accountVersion <= 0 {
		return nil, fmt.Errorf("claim mailing delivery: invalid account status version %d", accountVersion)
	}
	return claimed{
		database: claims.database,
		admission: delivery.AccountAdmission{
			Route:   delivery.Routing(accountID),
			Version: operatoraccount.Version(accountVersion),
		},
		identity: coordinates.New(
			mailing.Identity(mailingID),
			mailing.Run(runID),
			recipient.Identifier(recipientID),
		),
		token: delivery.Fence(token),
	}, nil
}

// Revalidate returns the canonical runtime target only for the exact active
// account lifecycle snapshot captured in admission. It deliberately reads no
// delivery state and does not hold a transaction across transport work.
func (claims *Claims) Revalidate(
	context context.Context,
	admission delivery.AccountAdmission,
) (operatoraccounts.RuntimeTarget, error) {
	var target operatoraccounts.RuntimeTarget
	if admission.Route.UUID() == uuid.Nil || admission.Version == 0 || admission.Version > operatoraccount.Version(math.MaxInt64) {
		return target, operatoraccounts.ErrAccountNotFound
	}

	var (
		operatorID uuid.UUID
		accountID  uuid.UUID
		status     string
		version    int64
	)
	failure := claims.database.QueryRow(
		context,
		`SELECT account.operator_id,
		        account.id,
		        account.status::text,
		        account.status_version
		 FROM operator_accounts AS account
		 WHERE account.id = $1
		   AND account.status = 'active'
		   AND account.status_version = $2`,
		admission.Route.UUID(),
		int64(admission.Version),
	).Scan(&operatorID, &accountID, &status, &version)
	if errors.Is(failure, pgx.ErrNoRows) {
		return target, operatoraccounts.ErrAccountNotFound
	}
	if failure != nil {
		return target, fmt.Errorf("revalidate delivery account admission: %w", failure)
	}
	if status != string(operatoraccount.StatusActive) || accountID != admission.Route.UUID() || version != int64(admission.Version) {
		return target, operatoraccounts.ErrAccountNotFound
	}
	return operatoraccounts.RuntimeTarget{
		Actor:     application.Actor{OperatorID: operatorID},
		AccountID: operatoraccount.Identity(accountID),
		Status:    operatoraccount.Status(status),
		Version:   operatoraccount.Version(version),
	}, nil
}
