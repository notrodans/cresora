// Package reconciler performs bounded, transport-free terminalization of
// PostgreSQL mailing runs.
package reconciler

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
)

const (
	DefaultBatchSize = 100
	MaxBatchSize     = 1000
)

var ErrInvalidConfig = errors.New("invalid delivery run reconciler configuration")

// Config bounds one reconciliation pass.  A pass never discovers more than
// BatchSize runs; each discovered run is handled in its own short transaction.
type Config struct {
	BatchSize int
}

func Defaults() Config {
	return Config{BatchSize: DefaultBatchSize}
}

func (config Config) Validate() error {
	if config.BatchSize <= 0 {
		return fmt.Errorf("%w: batch size must be positive", ErrInvalidConfig)
	}
	return nil
}

func (config Config) normalize() Config {
	if failure := config.Validate(); failure != nil {
		panic(failure.Error())
	}
	if config.BatchSize > MaxBatchSize {
		config.BatchSize = MaxBatchSize
	}
	return config
}

// Reconciler is the PostgreSQL implementation of the application run
// reconciliation port.
type Reconciler struct {
	database *pgxpool.Pool
	config   Config
}

var _ application.RunReconciler = Reconciler{}

func New(database *pgxpool.Pool, config Config) application.RunReconciler {
	if database == nil {
		panic("create PostgreSQL delivery run reconciler without database")
	}
	if config == (Config{}) {
		config = Defaults()
	}
	return Reconciler{database: database, config: config.normalize()}
}

// NewReconciler is an explicit constructor alias for wiring boundaries.
func NewReconciler(database *pgxpool.Pool, config Config) application.RunReconciler {
	return New(database, config)
}

// Reconcile discovers at most BatchSize active runs and reconciles each
// candidate independently.  Discovery is intentionally not locked: the
// per-run transaction below takes the lifecycle locks and performs the CAS,
// allowing concurrent passes to share the bounded scan safely.
func (reconciler Reconciler) Reconcile(context context.Context) (application.ReconciliationResult, error) {
	if context == nil {
		panic("reconcile PostgreSQL delivery runs without context")
	}
	if reconciler.database == nil {
		return application.ReconciliationResult{}, errors.New("reconcile PostgreSQL delivery runs without database")
	}

	candidates, failure := reconciler.findCandidates(context)
	if failure != nil {
		return application.ReconciliationResult{}, fmt.Errorf("discover delivery run reconciliation candidates: %w", failure)
	}

	result := application.ReconciliationResult{Candidates: len(candidates)}
	for _, candidate := range candidates {
		terminalStatus, changed, failure := reconciler.reconcileCandidate(context, candidate)
		if failure != nil {
			return result, fmt.Errorf("reconcile mailing run %s/%s: %w", candidate.MailingID.UUID(), candidate.RunID.UUID(), failure)
		}
		if !changed {
			continue
		}
		switch terminalStatus {
		case application.RunCompleted:
			result.Completed++
		case application.RunFailed:
			result.Failed++
		}
	}
	return result, nil
}

func (reconciler Reconciler) findCandidates(context context.Context) ([]application.ReconciliationCandidate, error) {
	rows, failure := reconciler.database.Query(
		context,
		`SELECT run.mailing_id,
		        run.id,
		        run.execution_generation
		 FROM mailing_runs AS run
		 JOIN mailings AS mailing
		   ON mailing.id = run.mailing_id
		 WHERE run.status IN ('queued', 'running')
		   AND (
		         (mailing.status = 'queued' AND run.status = 'queued')
		      OR (mailing.status = 'running' AND run.status = 'running')
		   )
		   AND EXISTS (
		       SELECT 1
		       FROM mailing_deliveries AS delivery
		       WHERE delivery.mailing_id = run.mailing_id
		         AND delivery.run_id = run.id
		   )
		   AND NOT EXISTS (
		       SELECT 1
		       FROM mailing_deliveries AS delivery
		       WHERE delivery.mailing_id = run.mailing_id
		         AND delivery.run_id = run.id
		         AND delivery.status NOT IN ('sent', 'skipped', 'failed', 'unknown')
		   )
		   AND NOT EXISTS (
		       SELECT 1
		       FROM mailing_deliveries AS delivery
		       WHERE delivery.mailing_id = run.mailing_id
		         AND delivery.run_id = run.id
		         AND delivery.status IN ('pending', 'sending')
		   )
		 ORDER BY run.queued_at, run.mailing_id, run.id
		 LIMIT $1`,
		reconciler.config.BatchSize,
	)
	if failure != nil {
		return nil, failure
	}
	defer rows.Close()

	candidates := make([]application.ReconciliationCandidate, 0, reconciler.config.BatchSize)
	for rows.Next() {
		var (
			mailingID           uuid.UUID
			runID               uuid.UUID
			executionGeneration int64
		)
		if failure := rows.Scan(&mailingID, &runID, &executionGeneration); failure != nil {
			return nil, fmt.Errorf("scan delivery run reconciliation candidate: %w", failure)
		}
		candidates = append(candidates, application.ReconciliationCandidate{
			MailingID:           mailing.Identity(mailingID),
			RunID:               mailing.Run(runID),
			ExecutionGeneration: executionGeneration,
		})
	}
	if failure := rows.Err(); failure != nil {
		return nil, fmt.Errorf("iterate delivery run reconciliation candidates: %w", failure)
	}
	return candidates, nil
}

func (reconciler Reconciler) reconcileCandidate(
	context context.Context,
	candidate application.ReconciliationCandidate,
) (application.RunTerminalStatus, bool, error) {
	transaction, failure := reconciler.database.Begin(context)
	if failure != nil {
		return "", false, fmt.Errorf("begin transaction: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	// Keep the lifecycle lock order identical to Stop and classified delivery
	// finalization: mailing first, then the concrete run.
	var mailingStatus string
	failure = transaction.QueryRow(
		context,
		`SELECT status::text
		 FROM mailings
		 WHERE id = $1
		 FOR UPDATE`,
		candidate.MailingID.UUID(),
	).Scan(&mailingStatus)
	if errors.Is(failure, pgx.ErrNoRows) {
		return "", false, commitNoop(transaction, context)
	}
	if failure != nil {
		return "", false, fmt.Errorf("lock mailing: %w", failure)
	}

	var (
		runStatus           string
		executionGeneration int64
	)
	failure = transaction.QueryRow(
		context,
		`SELECT status::text, execution_generation
		 FROM mailing_runs
		 WHERE mailing_id = $1
		   AND id = $2
		 FOR UPDATE`,
		candidate.MailingID.UUID(),
		candidate.RunID.UUID(),
	).Scan(&runStatus, &executionGeneration)
	if errors.Is(failure, pgx.ErrNoRows) {
		return "", false, commitNoop(transaction, context)
	}
	if failure != nil {
		return "", false, fmt.Errorf("lock mailing run: %w", failure)
	}

	// Stop increments the generation while cancelling the run.  Matching both
	// status lanes and the candidate generation avoids applying a stale scan to
	// a newer lifecycle execution and makes cancelled terminal state immutable.
	if executionGeneration != candidate.ExecutionGeneration || !activePair(mailingStatus, runStatus) {
		return "", false, commitNoop(transaction, context)
	}

	rows, failure := transaction.Query(
		context,
		`SELECT status::text
		 FROM mailing_deliveries
		 WHERE mailing_id = $1
		   AND run_id = $2
		 FOR UPDATE`,
		candidate.MailingID.UUID(),
		candidate.RunID.UUID(),
	)
	if failure != nil {
		return "", false, fmt.Errorf("lock mailing run deliveries: %w", failure)
	}

	states := make([]application.DeliveryState, 0)
	for rows.Next() {
		var state string
		if failure := rows.Scan(&state); failure != nil {
			rows.Close()
			return "", false, fmt.Errorf("scan mailing run delivery state: %w", failure)
		}
		states = append(states, application.DeliveryState(state))
	}
	if failure := rows.Err(); failure != nil {
		rows.Close()
		return "", false, fmt.Errorf("iterate mailing run delivery states: %w", failure)
	}
	rows.Close()

	terminalStatus, terminal := application.TerminalRunStatus(states)
	if !terminal {
		return "", false, commitNoop(transaction, context)
	}

	commandTag, failure := transaction.Exec(
		context,
		`UPDATE mailing_runs
		 SET status = $3::mailing_run_status_type,
		     started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
		     finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
		 WHERE mailing_id = $1
		   AND id = $2
		   AND execution_generation = $4
		   AND status = $5::mailing_run_status_type`,
		candidate.MailingID.UUID(),
		candidate.RunID.UUID(),
		string(terminalStatus),
		candidate.ExecutionGeneration,
		runStatus,
	)
	if failure != nil {
		return "", false, fmt.Errorf("terminalize mailing run: %w", failure)
	}
	if commandTag.RowsAffected() != 1 {
		return "", false, commitNoop(transaction, context)
	}

	// The mailing and run are fenced by the statuses read while holding the
	// lifecycle locks above. Keep the parent transition in this transaction so
	// a terminal run can never become visible without its matching mailing
	// status. The status predicate also makes a repeated reconciliation a no-op
	// without changing updated_at.
	mailingTag, failure := transaction.Exec(
		context,
		`UPDATE mailings
		 SET status = $2::mailing_status_type,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1
		   AND status = $3::mailing_status_type`,
		candidate.MailingID.UUID(),
		string(terminalStatus),
		mailingStatus,
	)
	if failure != nil {
		return "", false, fmt.Errorf("terminalize parent mailing: %w", failure)
	}
	if mailingTag.RowsAffected() != 1 {
		return "", false, fmt.Errorf("terminalize parent mailing: status CAS affected %d rows", mailingTag.RowsAffected())
	}
	if failure := transaction.Commit(context); failure != nil {
		return "", false, fmt.Errorf("commit terminal mailing run and parent mailing: %w", failure)
	}
	return terminalStatus, true, nil
}

func activePair(mailingStatus, runStatus string) bool {
	return (mailingStatus == "queued" && runStatus == "queued") ||
		(mailingStatus == "running" && runStatus == "running")
}

func commitNoop(transaction pgx.Tx, context context.Context) error {
	if failure := transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit no-op reconciliation transaction: %w", failure)
	}
	return nil
}
