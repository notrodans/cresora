// Package invariants checks observable consistency conditions in the PostgreSQL
// mailing queue without changing database state.
package invariants

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultSampleLimit is the number of example rows returned for each check.
	DefaultSampleLimit = 10
	// MaxSampleLimit prevents a report from becoming an unbounded query result.
	MaxSampleLimit = 100
	// DefaultExpiredLeaseGrace is how long a sending lease must be expired before
	// the expired lease warning is reported.
	DefaultExpiredLeaseGrace = time.Minute
)

// CheckName is the stable identifier of an invariant check.
type CheckName string

const (
	CheckStoppedMailingWithClaimableDelivery         CheckName = "stopped_mailing_with_claimable_delivery"
	CheckCancelledRunWithClaimableDelivery           CheckName = "cancelled_run_with_claimable_delivery"
	CheckSendingDeliveryWithoutLease                 CheckName = "sending_delivery_without_lease"
	CheckExpiredSendingLease                         CheckName = "expired_sending_lease"
	CheckRunStatusTimestampContradiction             CheckName = "run_status_timestamp_contradiction"
	CheckMailingRunStatusContradiction               CheckName = "mailing_run_status_contradiction"
	CheckSendingDeliveryWithStaleExecutionGeneration CheckName = "sending_delivery_with_stale_execution_generation"
	CheckSendingDeliveryWithInactiveParent           CheckName = "sending_delivery_with_inactive_parent"
)

// Severity describes how a non-empty result should be interpreted.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Sample identifies one row participating in a violation. A zero
// RecipientID means that the check is about a mailing or run rather than a
// delivery.
type Sample struct {
	MailingID   uuid.UUID `json:"mailing_id"`
	RunID       uuid.UUID `json:"run_id"`
	RecipientID uuid.UUID `json:"recipient_id"`
}

// Result is the stable output of one invariant check.
type Result struct {
	Name     CheckName `json:"name"`
	Severity Severity  `json:"severity"`
	Count    int64     `json:"count"`
	Sample   []Sample  `json:"sample"`
}

// Report contains one result for every configured check, including checks
// whose count is zero.
type Report struct {
	Results []Result `json:"results"`
}

// HasViolations reports whether any check found at least one violating row.
// Warning results are still violations for the command's exit-status
// contract; their severity distinguishes them from error results in the
// report.
func (report Report) HasViolations() bool {
	for _, result := range report.Results {
		if result.Count > 0 {
			return true
		}
	}
	return false
}

// Checker executes the invariant queries against PostgreSQL.
type Checker struct {
	database          *pgxpool.Pool
	sampleLimit       int
	expiredLeaseGrace time.Duration
}

// New creates a checker using the default bounded sample size and expired lease
// warning grace period. An expired lease is reported only when its expiry is
// strictly more than DefaultExpiredLeaseGrace in the past.
func New(database *pgxpool.Pool) Checker {
	return Checker{
		database:          database,
		sampleLimit:       DefaultSampleLimit,
		expiredLeaseGrace: DefaultExpiredLeaseGrace,
	}
}

// NewWithSampleLimit creates a checker with a caller-selected bounded sample
// size. Values above MaxSampleLimit are capped so callers cannot accidentally
// turn a diagnostic query into an unbounded result.
func NewWithSampleLimit(database *pgxpool.Pool, sampleLimit int) Checker {
	return NewWithSampleLimitAndExpiredLeaseGrace(database, sampleLimit, DefaultExpiredLeaseGrace)
}

// NewWithSampleLimitAndExpiredLeaseGrace creates a checker with caller-selected
// bounds. The sample size is capped at MaxSampleLimit, and expired sending
// leases are reported only when they have exceeded expiredLeaseGrace. A lease
// exactly at the grace boundary is not reported.
func NewWithSampleLimitAndExpiredLeaseGrace(
	database *pgxpool.Pool,
	sampleLimit int,
	expiredLeaseGrace time.Duration,
) Checker {
	if sampleLimit <= 0 {
		panic("create invariant checker with non-positive sample limit")
	}
	if sampleLimit > MaxSampleLimit {
		sampleLimit = MaxSampleLimit
	}
	if expiredLeaseGrace < 0 {
		panic("create invariant checker with negative expired lease grace")
	}
	return Checker{
		database:          database,
		sampleLimit:       sampleLimit,
		expiredLeaseGrace: expiredLeaseGrace,
	}
}

type checkSpec struct {
	name             CheckName
	severity         Severity
	query            string
	usesExpiredGrace bool
}

var checks = [...]checkSpec{
	{
		name:     CheckStoppedMailingWithClaimableDelivery,
		severity: SeverityError,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			JOIN mailings AS mailing
			  ON mailing.id = delivery.mailing_id
			JOIN telegram_mailing_routes AS route
			  ON route.mailing_id = delivery.mailing_id
			WHERE mailing.status = 'stopped'
			  AND delivery.status = 'pending'
			  AND delivery.ready_at <= CURRENT_TIMESTAMP
			  AND delivery.attempt_count < delivery.max_attempts
			  AND (delivery.lease_until IS NULL OR delivery.lease_until < CURRENT_TIMESTAMP)`,
	},
	{
		name:     CheckCancelledRunWithClaimableDelivery,
		severity: SeverityError,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			JOIN mailing_runs AS run
			  ON run.mailing_id = delivery.mailing_id
			 AND run.id = delivery.run_id
			JOIN telegram_mailing_routes AS route
			  ON route.mailing_id = delivery.mailing_id
			WHERE run.status = 'cancelled'
			  AND delivery.status = 'pending'
			  AND delivery.ready_at <= CURRENT_TIMESTAMP
			  AND delivery.attempt_count < delivery.max_attempts
			  AND (delivery.lease_until IS NULL OR delivery.lease_until < CURRENT_TIMESTAMP)`,
	},
	{
		name:     CheckSendingDeliveryWithoutLease,
		severity: SeverityError,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			WHERE delivery.status = 'sending'
			  AND (
					delivery.lease_token IS NULL
				 OR delivery.lease_until IS NULL
				 OR delivery.lease_execution_generation IS NULL
				  )`,
	},
	{
		name:             CheckExpiredSendingLease,
		severity:         SeverityWarning,
		usesExpiredGrace: true,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			WHERE delivery.status = 'sending'
			  AND delivery.lease_until IS NOT NULL
			  AND CURRENT_TIMESTAMP - delivery.lease_until >
			      ($2::double precision * INTERVAL '1 second')`,
	},
	{
		name:     CheckRunStatusTimestampContradiction,
		severity: SeverityError,
		query: `
			SELECT run.mailing_id,
			       run.id AS run_id,
			       '00000000-0000-0000-0000-000000000000'::uuid AS recipient_id
			FROM mailing_runs AS run
			WHERE (run.status = 'queued'
			       AND (run.started_at IS NOT NULL OR run.finished_at IS NOT NULL))
			   OR (run.status = 'running'
			       AND (run.started_at IS NULL OR run.finished_at IS NOT NULL))
			   OR (run.status = 'completed'
			       AND (run.started_at IS NULL OR run.finished_at IS NULL))
			   OR (run.status IN ('cancelled', 'failed')
			       AND run.finished_at IS NULL)`,
	},
	{
		name:     CheckMailingRunStatusContradiction,
		severity: SeverityError,
		query: `
			WITH latest_runs AS (
				SELECT DISTINCT ON (run.mailing_id)
				       run.mailing_id, run.id, run.status
				FROM mailing_runs AS run
				ORDER BY run.mailing_id, run.number DESC, run.id DESC
			)
			SELECT mailing.id AS mailing_id,
			       latest.id AS run_id,
			       '00000000-0000-0000-0000-000000000000'::uuid AS recipient_id
			FROM mailings AS mailing
			JOIN latest_runs AS latest
			  ON latest.mailing_id = mailing.id
			WHERE (mailing.status = 'draft'
			       OR (mailing.status = 'queued' AND latest.status <> 'queued')
			       OR (mailing.status = 'running' AND latest.status <> 'running')
			       OR (mailing.status = 'stopped'
			           AND latest.status IN ('queued', 'running'))
			       OR (mailing.status = 'paused'
			           AND latest.status NOT IN ('queued', 'running'))
			       OR (mailing.status = 'completed' AND latest.status <> 'completed')
			       OR (mailing.status = 'failed' AND latest.status <> 'failed'))`,
	},
	{
		name:     CheckSendingDeliveryWithStaleExecutionGeneration,
		severity: SeverityError,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			JOIN mailing_runs AS run
			  ON run.mailing_id = delivery.mailing_id
			 AND run.id = delivery.run_id
			WHERE delivery.status = 'sending'
			  AND delivery.lease_execution_generation IS DISTINCT FROM run.execution_generation`,
	},
	{
		name:             CheckSendingDeliveryWithInactiveParent,
		severity:         SeverityError,
		usesExpiredGrace: true,
		query: `
			SELECT delivery.mailing_id, delivery.run_id, delivery.recipient_id
			FROM mailing_deliveries AS delivery
			JOIN mailings AS mailing
			  ON mailing.id = delivery.mailing_id
			JOIN mailing_runs AS run
			  ON run.mailing_id = delivery.mailing_id
			 AND run.id = delivery.run_id
			WHERE delivery.status = 'sending'
			  AND delivery.lease_until IS NOT NULL
			  AND CURRENT_TIMESTAMP - delivery.lease_until >
			      ($2::double precision * INTERVAL '1 second')
			  AND NOT (
					(mailing.status = 'queued' AND run.status = 'queued')
				 OR (mailing.status = 'running' AND run.status = 'running')
				  )`,
	},
}

// Check runs all checks in one repeatable-read, read-only transaction. The
// transaction contains only the SELECT statements in checks; no row is
// claimed, locked, updated, or otherwise changed by this package.
func (checker Checker) Check(context context.Context) (Report, error) {
	if checker.sampleLimit <= 0 {
		return Report{}, errors.New("check PostgreSQL invariants with invalid sample limit")
	}
	if checker.expiredLeaseGrace < 0 {
		return Report{}, errors.New("check PostgreSQL invariants with negative expired lease grace")
	}

	transaction, failure := checker.database.BeginTx(context, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if failure != nil {
		return Report{}, fmt.Errorf("begin read-only invariant transaction: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	report := Report{Results: make([]Result, 0, len(checks))}
	for _, check := range checks {
		result, checkFailure := runCheck(
			context,
			transaction,
			check,
			checker.sampleLimit,
			checker.expiredLeaseGrace,
		)
		if checkFailure != nil {
			return Report{}, fmt.Errorf("run invariant check %q: %w", check.name, checkFailure)
		}
		report.Results = append(report.Results, result)
	}

	if failure = transaction.Commit(context); failure != nil {
		return Report{}, fmt.Errorf("commit read-only invariant transaction: %w", failure)
	}
	return report, nil
}

func runCheck(
	context context.Context,
	transaction pgx.Tx,
	check checkSpec,
	sampleLimit int,
	expiredLeaseGrace time.Duration,
) (Result, error) {
	query := fmt.Sprintf(`
		WITH violations AS (%s)
		SELECT violations.mailing_id,
		       violations.run_id,
		       violations.recipient_id,
		       COUNT(*) OVER() AS total_count
		FROM violations
		ORDER BY violations.mailing_id,
		         violations.run_id,
		         violations.recipient_id
		LIMIT $1`, check.query)

	arguments := []any{sampleLimit}
	if check.usesExpiredGrace {
		arguments = append(arguments, expiredLeaseGrace.Seconds())
	}
	rows, failure := transaction.Query(context, query, arguments...)
	if failure != nil {
		return Result{}, fmt.Errorf("select violating rows: %w", failure)
	}
	defer rows.Close()

	result := Result{
		Name:     check.name,
		Severity: check.severity,
		Sample:   make([]Sample, 0, sampleLimit),
	}
	for rows.Next() {
		var sample Sample
		var total int64
		if failure = rows.Scan(
			&sample.MailingID,
			&sample.RunID,
			&sample.RecipientID,
			&total,
		); failure != nil {
			return Result{}, fmt.Errorf("scan violating row: %w", failure)
		}
		result.Count = total
		result.Sample = append(result.Sample, sample)
	}
	if failure = rows.Err(); failure != nil {
		return Result{}, fmt.Errorf("read violating rows: %w", failure)
	}
	return result, nil
}

func leaseExpiryExceedsGrace(expiredFor, grace time.Duration) bool {
	return expiredFor > grace
}
