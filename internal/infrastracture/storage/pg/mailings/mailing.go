package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/notrodans/cresora/internal/domain/mailing"
)

var _ mailing.Mailing = pgMailing{}

// pgMailing represents one PostgreSQL mailing row.
type pgMailing struct {
	database mailingDatabase
	identity mailing.ID
	scope    mailingScope
}

type mailingScope interface {
	mailingScope()
}

type systemMailingScope struct{}

func (systemMailingScope) mailingScope() {}

type operatorMailingScope struct {
	operatorID uuid.UUID
}

func (operatorMailingScope) mailingScope() {}

type lifecycleFailure struct {
	message string
	cause   error
}

func (failure lifecycleFailure) Error() string {
	return failure.message
}

func (failure lifecycleFailure) Unwrap() error {
	return failure.cause
}

func wrapLifecycleFailure(message string, cause error) error {
	return lifecycleFailure{message: message, cause: cause}
}

func (entity pgMailing) Queue(context context.Context) error {
	entity.validate(context, "queue")
	transaction, failure := entity.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin mailing %s queue transaction: %w", entity.identity.UUID(), failure)
	}
	defer func() { _ = transaction.Rollback(context) }()
	query := `SELECT status::text, repeat_mode::text FROM mailings WHERE id = $1`
	arguments := []any{entity.identity.UUID()}
	if operatorID, scoped := entity.operator(); scoped {
		query += ` AND operator_id = $2`
		arguments = append(arguments, operatorID)
	}
	query += ` FOR UPDATE`
	var status, repeat string
	failure = transaction.QueryRow(context, query, arguments...).Scan(&status, &repeat)
	if errors.Is(failure, pgx.ErrNoRows) {
		if _, scoped := entity.operator(); scoped {
			return wrapLifecycleFailure(
				fmt.Sprintf("mailing %s is not owned by operator", entity.identity.UUID()),
				mailing.ErrNotFound,
			)
		}
		return wrapLifecycleFailure(
			fmt.Sprintf("queue mailing %s: mailing does not exist", entity.identity.UUID()),
			mailing.ErrNotFound,
		)
	}
	if failure != nil {
		return fmt.Errorf("lock mailing %s for queue: %w", entity.identity.UUID(), failure)
	}
	if status != "draft" && status != "stopped" {
		if _, scoped := entity.operator(); scoped {
			return wrapLifecycleFailure(
				fmt.Sprintf("mailing %s cannot be queued from status %q", entity.identity.UUID(), status),
				mailing.ErrInvalidState,
			)
		}
		return wrapLifecycleFailure(
			fmt.Sprintf("queue mailing %s from invalid status %q", entity.identity.UUID(), status),
			mailing.ErrInvalidState,
		)
	}
	var unresolved bool
	failure = transaction.QueryRow(
		context,
		`SELECT EXISTS (
			SELECT 1
			FROM mailing_deliveries AS delivery
			WHERE delivery.mailing_id = $1
			  AND (
				       delivery.status IN ('sending', 'unknown')
				    OR (delivery.status = 'pending' AND delivery.attempt_count > 0)
				  )
		)`,
		entity.identity.UUID(),
	).Scan(&unresolved)
	if failure != nil {
		return fmt.Errorf("check unresolved deliveries for mailing %s queue: %w", entity.identity.UUID(), failure)
	}
	if unresolved {
		return wrapLifecycleFailure(
			fmt.Sprintf("queue mailing %s with unresolved delivery outcomes", entity.identity.UUID()),
			mailing.ErrUnresolvedDeliveryOutcomes,
		)
	}
	var runID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`INSERT INTO mailing_runs (mailing_id, number, status, execution_generation)
		 SELECT $1, COALESCE(MAX(number), 0) + 1, 'queued', 1
		 FROM mailing_runs WHERE mailing_id = $1 RETURNING id`,
		entity.identity.UUID(),
	).Scan(&runID)
	if failure != nil {
		return fmt.Errorf("create run for mailing %s: %w", entity.identity.UUID(), failure)
	}
	result, failure := transaction.Exec(
		context,
		`WITH inserted_deliveries AS (
		 INSERT INTO mailing_deliveries (mailing_id, run_id, recipient_id, status, ready_at)
		 SELECT recipients.mailing_id, $2, recipients.id, 'pending', CURRENT_TIMESTAMP
		 FROM mailing_recipients AS recipients
		 WHERE recipients.mailing_id = $1
		   AND (NOT $3 OR NOT EXISTS (
			SELECT 1 FROM mailing_deliveries AS deliveries
			WHERE deliveries.mailing_id = recipients.mailing_id
			  AND deliveries.recipient_id = recipients.id
			  AND deliveries.status = 'sent'
		   ))
		 RETURNING mailing_id, run_id, recipient_id
		)
		INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id)
		SELECT mailing_id, run_id, recipient_id, nextval('mailing_delivery_random_id_seq')
		FROM inserted_deliveries`,
		entity.identity.UUID(),
		runID,
		repeat == "once",
	)
	if failure != nil {
		return fmt.Errorf("create deliveries for mailing %s run %s: %w", entity.identity.UUID(), runID, failure)
	}
	if result.RowsAffected() == 0 {
		if _, scoped := entity.operator(); scoped {
			return wrapLifecycleFailure(
				fmt.Sprintf("mailing %s has no eligible recipients", entity.identity.UUID()),
				mailing.ErrNoEligibleRecipients,
			)
		}
		return wrapLifecycleFailure(
			fmt.Sprintf("queue mailing %s without eligible recipients", entity.identity.UUID()),
			mailing.ErrNoEligibleRecipients,
		)
	}
	update := `UPDATE mailings SET status = 'queued', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	updateArguments := []any{entity.identity.UUID()}
	if operatorID, scoped := entity.operator(); scoped {
		update += ` AND operator_id = $2`
		updateArguments = append(updateArguments, operatorID)
	}
	if _, failure = transaction.Exec(context, update, updateArguments...); failure != nil {
		return fmt.Errorf("mark mailing %s queued: %w", entity.identity.UUID(), failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit mailing %s queue transaction: %w", entity.identity.UUID(), failure)
	}
	return nil
}

func (entity pgMailing) Stop(context context.Context) error {
	entity.validate(context, "stop")
	transaction, failure := entity.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("begin mailing %s stop transaction: %w", entity.identity.UUID(), failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	query := `SELECT status::text FROM mailings WHERE id = $1`
	arguments := []any{entity.identity.UUID()}
	if operatorID, scoped := entity.operator(); scoped {
		query += ` AND operator_id = $2`
		arguments = append(arguments, operatorID)
	}
	query += ` FOR UPDATE`
	var status string
	failure = transaction.QueryRow(context, query, arguments...).Scan(&status)
	if errors.Is(failure, pgx.ErrNoRows) {
		if _, scoped := entity.operator(); scoped {
			return wrapLifecycleFailure(
				fmt.Sprintf("stop mailing %s from missing or unauthorized mailing", entity.identity.UUID()),
				mailing.ErrNotFound,
			)
		}
		return wrapLifecycleFailure(
			fmt.Sprintf("stop mailing %s: mailing does not exist", entity.identity.UUID()),
			mailing.ErrInvalidState,
		)
	}
	if failure != nil {
		return fmt.Errorf("lock mailing %s for stop: %w", entity.identity.UUID(), failure)
	}
	if status == "stopped" {
		if failure = transaction.Commit(context); failure != nil {
			return fmt.Errorf("commit repeated stop for mailing %s: %w", entity.identity.UUID(), failure)
		}
		return nil
	}
	if status != "queued" && status != "running" && status != "paused" {
		if _, scoped := entity.operator(); scoped {
			return wrapLifecycleFailure(
				fmt.Sprintf("stop mailing %s from invalid status %q", entity.identity.UUID(), status),
				mailing.ErrInvalidState,
			)
		}
		return wrapLifecycleFailure(
			fmt.Sprintf("stop mailing %s from invalid status %q", entity.identity.UUID(), status),
			mailing.ErrInvalidState,
		)
	}

	var runID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`SELECT id
		 FROM mailing_runs
		 WHERE mailing_id = $1
		   AND status IN ('queued', 'running')
		 ORDER BY number DESC, id DESC
		 FOR UPDATE`,
		entity.identity.UUID(),
	).Scan(&runID)
	if errors.Is(failure, pgx.ErrNoRows) {
		return wrapLifecycleFailure(
			fmt.Sprintf("stop mailing %s without an active run", entity.identity.UUID()),
			mailing.ErrInvalidState,
		)
	}
	if failure != nil {
		return fmt.Errorf("lock active run for mailing %s stop: %w", entity.identity.UUID(), failure)
	}
	if _, failure = transaction.Exec(
		context,
		`UPDATE mailing_runs
		 SET status = 'cancelled',
		     execution_generation = execution_generation + 1,
		     finished_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND id = $2
		   AND status IN ('queued', 'running')`,
		entity.identity.UUID(),
		runID,
	); failure != nil {
		return fmt.Errorf("cancel active run %s for mailing %s: %w", runID, entity.identity.UUID(), failure)
	}
	if _, failure = transaction.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET status = CASE WHEN attempt_count = 0 THEN 'skipped' ELSE 'unknown' END::mailing_delivery_status_type,
		     skip_reason = CASE WHEN attempt_count = 0 THEN 'mailing stopped' ELSE NULL END,
		     error_message = CASE
		         WHEN attempt_count = 0 THEN error_message
		         ELSE COALESCE(NULLIF(btrim(error_message), ''), 'mailing stopped with retry-pending delivery')
		     END,
		     lease_until = NULL,
		     lease_token = NULL,
		     lease_execution_generation = NULL,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1
		   AND run_id = $2
		   AND status = 'pending'`,
		entity.identity.UUID(),
		runID,
	); failure != nil {
		return fmt.Errorf("skip pending deliveries for mailing %s run %s: %w", entity.identity.UUID(), runID, failure)
	}
	update := `UPDATE mailings
		SET status = 'stopped', updated_at = CURRENT_TIMESTAMP
		WHERE id = $1`
	updateArguments := []any{entity.identity.UUID()}
	if operatorID, scoped := entity.operator(); scoped {
		update += ` AND operator_id = $2`
		updateArguments = append(updateArguments, operatorID)
	}
	if _, failure = transaction.Exec(context, update, updateArguments...); failure != nil {
		return fmt.Errorf("stop mailing %s: %w", entity.identity.UUID(), failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("commit mailing %s stop transaction: %w", entity.identity.UUID(), failure)
	}
	return nil
}

func (entity pgMailing) validate(context context.Context, operation string) {
	if context == nil {
		panic(operation + " PostgreSQL mailing without context")
	}
	if entity.database == nil {
		panic(operation + " PostgreSQL mailing without database")
	}
	if entity.identity.UUID() == uuid.Nil {
		panic(operation + " PostgreSQL mailing with zero identity")
	}
	if entity.scope == nil {
		panic(operation + " PostgreSQL mailing without scope")
	}
	if operatorID, scoped := entity.operator(); scoped {
		validateOperatorID(operatorID, operation+" PostgreSQL mailing")
	}
}

func (entity pgMailing) operator() (uuid.UUID, bool) {
	switch scope := entity.scope.(type) {
	case systemMailingScope:
		return uuid.Nil, false
	case operatorMailingScope:
		return scope.operatorID, true
	default:
		panic("use PostgreSQL mailing with unknown scope")
	}
}
