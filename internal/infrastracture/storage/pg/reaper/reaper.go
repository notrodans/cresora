// Package reaper performs bounded, transport-free recovery of expired
// PostgreSQL delivery leases.
package reaper

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
)

const (
	DefaultBatchSize         = 100
	DefaultExpiredLeaseGrace = time.Minute
	DefaultRetryDelay        = 5 * time.Second
	MaxBatchSize             = 1000
)

// Config bounds one reaper pass. ExpiredLeaseGrace prevents a reaper from
// racing a lease that has only just elapsed; RetryDelay is deliberately fixed
// rather than exponential.
type Config struct {
	BatchSize         int
	ExpiredLeaseGrace time.Duration
	RetryDelay        time.Duration
}

// Defaults returns the bounded recovery policy used by the application.
func Defaults() Config {
	return Config{
		BatchSize:         DefaultBatchSize,
		ExpiredLeaseGrace: DefaultExpiredLeaseGrace,
		RetryDelay:        DefaultRetryDelay,
	}
}

func (config Config) normalize() Config {
	if config.BatchSize <= 0 {
		panic("create delivery reaper with non-positive batch size")
	}
	if config.ExpiredLeaseGrace < 0 {
		panic("create delivery reaper with negative expired lease grace")
	}
	if config.RetryDelay < 0 {
		panic("create delivery reaper with negative retry delay")
	}
	if config.BatchSize > MaxBatchSize {
		config.BatchSize = MaxBatchSize
	}
	return config
}

// Reaper is a PostgreSQL implementation of the application recovery port.
type Reaper struct {
	database *pgxpool.Pool
	config   Config
}

var _ application.Reaper = Reaper{}

// New creates a bounded PostgreSQL delivery reaper.
func New(database *pgxpool.Pool, config Config) Reaper {
	if config == (Config{}) {
		config = Defaults()
	}
	return Reaper{database: database, config: config.normalize()}
}

// NewReaper is an explicit constructor alias for callers that prefer the
// implementation name at the wiring boundary.
func NewReaper(database *pgxpool.Pool, config Config) Reaper {
	return New(database, config)
}

// Reap handles at most Config.BatchSize expired sending rows. The single
// statement locks candidates with SKIP LOCKED and compares the old lease
// token in each update, so concurrent reapers can safely share the queue.
func (reaper Reaper) Reap(context context.Context) (application.ReapResult, error) {
	var result application.ReapResult
	failure := reaper.database.QueryRow(
		context,
		`WITH candidates AS (
			SELECT delivery.mailing_id,
			       delivery.run_id,
			       delivery.recipient_id,
			       delivery.lease_token AS old_lease_token,
			       delivery.lease_execution_generation,
			       delivery.attempt_count,
			       delivery.max_attempts,
			       delivery.started_at,
			       delivery.error_message,
			       (
				       (
				           (mailing.status = 'queued' AND run.status = 'queued')
				        OR (mailing.status = 'running' AND run.status = 'running')
				       )
				       AND delivery.lease_execution_generation = run.execution_generation
				       AND delivery.attempt_count < delivery.max_attempts
			       ) AS retryable,
			       CASE
				       WHEN delivery.attempt_count >= delivery.max_attempts
						THEN 'maximum delivery attempts exhausted'
				       WHEN delivery.lease_execution_generation IS DISTINCT FROM run.execution_generation
						THEN 'execution generation no longer matches'
				       WHEN NOT (
						       (mailing.status = 'queued' AND run.status = 'queued')
						    OR (mailing.status = 'running' AND run.status = 'running')
					       )
						THEN 'mailing or run is no longer active'
				       ELSE 'expired sending lease is not retryable'
			       END AS unknown_reason
			FROM mailing_deliveries AS delivery
			JOIN mailings AS mailing
			  ON mailing.id = delivery.mailing_id
			JOIN mailing_runs AS run
			  ON run.mailing_id = delivery.mailing_id
			 AND run.id = delivery.run_id
			WHERE delivery.status = 'sending'
			  AND delivery.lease_until IS NOT NULL
			  AND delivery.lease_until < CURRENT_TIMESTAMP - ($1::double precision * INTERVAL '1 second')
			ORDER BY delivery.lease_until,
			         delivery.mailing_id,
			         delivery.run_id,
			         delivery.recipient_id
			FOR UPDATE OF delivery SKIP LOCKED
			LIMIT $3
		), retried AS (
			UPDATE mailing_deliveries AS delivery
			SET status = 'pending',
			    ready_at = CURRENT_TIMESTAMP + ($2::double precision * INTERVAL '1 second'),
			    lease_token = NULL,
			    lease_until = NULL,
			    lease_execution_generation = NULL,
			    updated_at = CURRENT_TIMESTAMP
			FROM candidates AS candidate
			WHERE delivery.mailing_id = candidate.mailing_id
			  AND delivery.run_id = candidate.run_id
			  AND delivery.recipient_id = candidate.recipient_id
			  AND delivery.status = 'sending'
			  AND delivery.lease_token = candidate.old_lease_token
			  AND candidate.retryable
			  AND candidate.attempt_count < candidate.max_attempts
			RETURNING delivery.mailing_id, delivery.run_id, delivery.recipient_id
		), unknowned AS (
			UPDATE mailing_deliveries AS delivery
			SET status = 'unknown',
			    lease_token = NULL,
			    lease_until = NULL,
			    lease_execution_generation = NULL,
			    error_message = COALESCE(NULLIF(btrim(delivery.error_message), ''), candidate.unknown_reason),
			    updated_at = CURRENT_TIMESTAMP
			FROM candidates AS candidate
			WHERE delivery.mailing_id = candidate.mailing_id
			  AND delivery.run_id = candidate.run_id
			  AND delivery.recipient_id = candidate.recipient_id
			  AND delivery.status = 'sending'
			  AND delivery.lease_token = candidate.old_lease_token
			  AND NOT candidate.retryable
			  AND candidate.attempt_count >= 1
			  AND candidate.started_at IS NOT NULL
			  AND NOT EXISTS (
				  SELECT 1
				  FROM retried
				  WHERE retried.mailing_id = candidate.mailing_id
				    AND retried.run_id = candidate.run_id
				    AND retried.recipient_id = candidate.recipient_id
			  )
			RETURNING delivery.mailing_id
		)
		SELECT
			(SELECT COUNT(*) FROM retried),
			(SELECT COUNT(*) FROM unknowned)`,
		reaper.config.ExpiredLeaseGrace.Seconds(),
		reaper.config.RetryDelay.Seconds(),
		reaper.config.BatchSize,
	).Scan(&result.Retried, &result.Unknown)
	if failure != nil {
		return application.ReapResult{}, fmt.Errorf("reap expired sending deliveries: %w", failure)
	}
	return result, nil
}
