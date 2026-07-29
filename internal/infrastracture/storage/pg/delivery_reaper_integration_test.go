package pg_test

import (
	stdcontext "context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	applicationdelivery "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
	pgclaims "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/claims"
	pgdeliveries "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/deliveries"
	pgmailings "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/mailings"
	pgreaper "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/reaper"
	"github.com/notrodans/nebula-go/internal/infrastracture/transport/faketelegram"
)

func TestPostgreSQLDeliveryReaper(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := stdcontext.WithTimeout(stdcontext.Background(), 90*time.Second)
	defer cancel()
	database, _, failure := newIsolatedDeliveryPipelineDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	operatorID, accountID, failure := createLifecycleAccount(context, database)
	if failure != nil {
		t.Fatalf("create reaper account: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM mailings WHERE operator_id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup reaper mailings: %v", cleanupFailure)
		}
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM operators WHERE id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup reaper operator: %v", cleanupFailure)
		}
	})

	reaper := pgreaper.New(database, pgreaper.Config{
		BatchSize:         100,
		ExpiredLeaseGrace: 2 * time.Second,
		RetryDelay:        3 * time.Second,
	})

	t.Run("active expired sending retries and preserves evidence", func(t *testing.T) {
		item := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 5, 1)
		before := readRandomAndState(t, context, database, item.pipelineDelivery)
		result, reapFailure := reaper.Reap(context)
		if reapFailure != nil {
			t.Fatalf("reap active delivery: %v", reapFailure)
		}
		if result.Retried != 1 || result.Unknown != 0 {
			t.Fatalf("reap result = %+v, want one retry", result)
		}
		after := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
		if after.status != "pending" || after.attempts != 1 || !after.started.Valid || after.leaseToken != uuid.Nil || after.leaseUntil.Valid || after.leaseGeneration.Valid {
			t.Fatalf("retried delivery state = %+v", after)
		}
		var readyInSeconds float64
		if reapFailure = database.QueryRow(context, `SELECT EXTRACT(EPOCH FROM (ready_at - CURRENT_TIMESTAMP)) FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID).Scan(&readyInSeconds); reapFailure != nil {
			t.Fatalf("read retry delay: %v", reapFailure)
		}
		if readyInSeconds < 2 || readyInSeconds > 4 {
			t.Fatalf("retry ready delay = %0.3f seconds, want about 3", readyInSeconds)
		}
		afterRandom := readRandomAndState(t, context, database, item.pipelineDelivery)
		if before.randomID != afterRandom.randomID || afterRandom.errorMessage != before.errorMessage {
			t.Fatalf("retry changed logical delivery evidence: before=%+v after=%+v", before, afterRandom)
		}
	})

	t.Run("exhausted, inactive, and generation-mismatched rows become unknown", func(t *testing.T) {
		exhausted := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 1, 1)
		inactive := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 5, 1)
		if _, failure = database.Exec(context, `UPDATE mailings SET status = 'stopped' WHERE id = $1`, inactive.mailingID); failure != nil {
			t.Fatalf("stop inactive mailing fixture: %v", failure)
		}
		if _, failure = database.Exec(context, `UPDATE mailing_runs SET status = 'cancelled', execution_generation = 2, finished_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND id = $2`, inactive.mailingID, inactive.runID); failure != nil {
			t.Fatalf("cancel inactive run fixture: %v", failure)
		}
		generation := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 5, 1)
		if _, failure = database.Exec(context, `UPDATE mailing_runs SET execution_generation = 2 WHERE mailing_id = $1 AND id = $2`, generation.mailingID, generation.runID); failure != nil {
			t.Fatalf("advance generation fixture: %v", failure)
		}

		result, reapFailure := reaper.Reap(context)
		if reapFailure != nil {
			t.Fatalf("reap unknown fixtures: %v", reapFailure)
		}
		if result.Unknown != 3 || result.Retried != 0 {
			t.Fatalf("unknown fixture result = %+v, want three unknown", result)
		}
		for _, item := range []lifecycleDelivery{exhausted, inactive, generation} {
			state := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
			if state.status != "unknown" || state.attempts != 1 || !state.started.Valid || state.leaseToken != uuid.Nil || state.leaseUntil.Valid || state.leaseGeneration.Valid || state.errorMessage == "" {
				t.Errorf("unknown delivery %s state = %+v", item.mailingID, state)
			}
		}
	})

	t.Run("non-expired rows and rows beyond the batch remain unchanged", func(t *testing.T) {
		fresh := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 5, 1)
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, fresh.mailingID, fresh.runID, fresh.recipientID); failure != nil {
			t.Fatalf("make grace-boundary fixture: %v", failure)
		}
		before := readLifecycleDeliveryState(t, context, database, fresh.pipelineDelivery)
		bounded := pgreaper.New(database, pgreaper.Config{BatchSize: 1, ExpiredLeaseGrace: 2 * time.Second, RetryDelay: time.Second})
		result, reapFailure := bounded.Reap(context)
		if reapFailure != nil {
			t.Fatalf("reap grace-boundary fixture: %v", reapFailure)
		}
		if result.Retried != 0 || result.Unknown != 0 {
			t.Fatalf("grace-boundary result = %+v, want no mutation", result)
		}
		after := readLifecycleDeliveryState(t, context, database, fresh.pipelineDelivery)
		if after.status != before.status || after.attempts != before.attempts || after.leaseToken != before.leaseToken || after.leaseGeneration != before.leaseGeneration {
			t.Fatalf("grace-boundary state changed: before=%+v after=%+v", before, after)
		}

		batch := createLifecycleDelivery(t, context, database, operatorID, accountID, 2)
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = CURRENT_TIMESTAMP, lease_token = $4, lease_until = CURRENT_TIMESTAMP - INTERVAL '10 seconds', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id IN ($3, $5)`, batch.mailingID, batch.runID, batch.recipientID, uuid.New(), batch.otherRecipientID); failure != nil {
			t.Fatalf("create batch fixtures: %v", failure)
		}
		result, reapFailure = bounded.Reap(context)
		if reapFailure != nil {
			t.Fatalf("reap bounded batch: %v", reapFailure)
		}
		if result.Retried != 1 || result.Unknown != 0 {
			t.Fatalf("bounded batch result = %+v, want one retry", result)
		}
		var sendingCount int
		if failure = database.QueryRow(context, `SELECT COUNT(*) FROM mailing_deliveries WHERE mailing_id = $1 AND status = 'sending'`, batch.mailingID).Scan(&sendingCount); failure != nil {
			t.Fatalf("count unprocessed batch rows: %v", failure)
		}
		if sendingCount != 1 {
			t.Fatalf("unprocessed batch rows = %d, want one", sendingCount)
		}
	})

	t.Run("SKIP LOCKED leaves a locked row for a later pass", func(t *testing.T) {
		item := createExpiredSendingDelivery(t, context, database, operatorID, accountID, 1, 5, 1)
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET status = 'pending', lease_token = NULL, lease_until = NULL, lease_execution_generation = NULL WHERE status = 'sending' AND mailing_id <> $1`, item.mailingID); failure != nil {
			t.Fatalf("clear unrelated reaper fixtures: %v", failure)
		}
		transaction, beginFailure := database.Begin(context)
		if beginFailure != nil {
			t.Fatalf("begin lock transaction: %v", beginFailure)
		}
		defer func() { _ = transaction.Rollback(context) }()
		var locked int
		if beginFailure = transaction.QueryRow(context, `SELECT 1 FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3 FOR UPDATE`, item.mailingID, item.runID, item.recipientID).Scan(&locked); beginFailure != nil {
			t.Fatalf("lock reaper row: %v", beginFailure)
		}
		result, reapFailure := pgreaper.New(database, pgreaper.Config{BatchSize: 1, ExpiredLeaseGrace: 0}).Reap(context)
		if reapFailure != nil {
			t.Fatalf("reap around locked row: %v", reapFailure)
		}
		if result.Retried != 0 || result.Unknown != 0 {
			t.Fatalf("locked row result = %+v, want no mutation", result)
		}
	})
}

func TestPostgreSQLDeliveryRetryUsesStableRandomID(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := stdcontext.WithTimeout(stdcontext.Background(), 90*time.Second)
	defer cancel()
	database, _, failure := newIsolatedDeliveryPipelineDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	operatorID, accountID, failure := createLifecycleAccount(context, database)
	if failure != nil {
		t.Fatalf("create retry account: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM mailings WHERE operator_id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup retry mailings: %v", cleanupFailure)
		}
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM operators WHERE id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup retry operator: %v", cleanupFailure)
		}
	})

	item := createLifecycleDelivery(t, context, database, operatorID, accountID)
	claims := pgclaims.NewClaims(database, time.Minute)
	deliveries := pgdeliveries.NewDeliveries(database)
	fake := faketelegram.New(
		faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomeUnknown}),
		faketelegram.WithCallRecording(4),
	)
	command := applicationdelivery.New(deliveries, fake)
	task, claimFailure := claims.Claim(context)
	if claimFailure != nil {
		t.Fatalf("claim first retry attempt: %v", claimFailure)
	}
	if failure = task.Execute(context, command); failure != nil {
		t.Fatalf("execute first unknown attempt: %v", failure)
	}
	first := readRandomAndState(t, context, database, item.pipelineDelivery)
	if first.state.status != "sending" || first.state.attempts != 1 {
		t.Fatalf("first unknown state = %+v", first.state)
	}
	if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
		t.Fatalf("expire first retry attempt: %v", failure)
	}
	result, reapFailure := pgreaper.New(database, pgreaper.Config{BatchSize: 1, ExpiredLeaseGrace: 0, RetryDelay: 0}).Reap(context)
	if reapFailure != nil || result.Retried != 1 {
		t.Fatalf("reap first retry attempt = %+v/%v", result, reapFailure)
	}
	secondTask, claimFailure := claims.Claim(context)
	if claimFailure != nil {
		t.Fatalf("claim idempotent retry: %v", claimFailure)
	}
	if failure = secondTask.Execute(context, command); failure != nil {
		t.Fatalf("execute idempotent retry: %v", failure)
	}
	second := readRandomAndState(t, context, database, item.pipelineDelivery)
	if second.randomID != first.randomID || second.state.status != "sent" || second.state.attempts != 2 {
		t.Fatalf("idempotent retry state = %+v, first=%+v", second, first)
	}
	if calls := fake.Calls(); len(calls) != 2 {
		t.Fatalf("transport calls = %d, want two logical attempts", len(calls))
	}
	if effects := fake.Effects(); len(effects) != 1 {
		t.Fatalf("external effects = %d, want one stable-random effect", len(effects))
	}
}

func TestPostgreSQLLateOutcomesAndLifecycleGuards(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := stdcontext.WithTimeout(stdcontext.Background(), 90*time.Second)
	defer cancel()
	database, _, failure := newIsolatedDeliveryPipelineDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	operatorID, accountID, failure := createLifecycleAccount(context, database)
	if failure != nil {
		t.Fatalf("create late-outcome account: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM mailings WHERE operator_id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup late-outcome mailings: %v", cleanupFailure)
		}
		if _, cleanupFailure := database.Exec(cleanupContext, `DELETE FROM operators WHERE id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup late-outcome operator: %v", cleanupFailure)
		}
	})

	claims := pgclaims.NewClaims(database, time.Minute)
	deliveries := pgdeliveries.NewDeliveries(database)
	reap := pgreaper.New(database, pgreaper.Config{BatchSize: 1, ExpiredLeaseGrace: 0, RetryDelay: 0})

	t.Run("late success finalizes after reaper clears the old lease", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim late-success delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted late-success send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("expire late-success lease: %v", failure)
		}
		result, reapFailure := reap.Reap(context)
		if reapFailure != nil || result.Retried != 1 {
			t.Fatalf("reap late-success lease = %+v/%v", result, reapFailure)
		}
		close(port.release)
		if failure = <-done; failure != nil {
			t.Fatalf("late success finalization: %v", failure)
		}
		state := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
		if state.status != "sent" || state.attempts != 1 || state.leaseToken != uuid.Nil {
			t.Fatalf("late success state = %+v", state)
		}
	})

	t.Run("late success finalizes an unknown row after reaper exhaustion", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim exhausted late-success delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted exhausted late-success send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET max_attempts = 1, lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("exhaust late-success lease: %v", failure)
		}
		result, reapFailure := reap.Reap(context)
		if reapFailure != nil || result.Unknown != 1 || result.Retried != 0 {
			t.Fatalf("reap exhausted late-success lease = %+v/%v", result, reapFailure)
		}
		close(port.release)
		if failure = <-done; failure != nil {
			t.Fatalf("late success finalization from unknown: %v", failure)
		}
		state := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
		if state.status != "sent" || state.attempts != 1 || state.leaseToken != uuid.Nil {
			t.Fatalf("unknown late success state = %+v", state)
		}
	})

	t.Run("already-sent success is idempotent only with matching proof", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim already-sent delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted already-sent send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET status = 'sent', sent_at = CURRENT_TIMESTAMP, lease_token = NULL, lease_until = NULL, lease_execution_generation = NULL WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("persist prior sent outcome: %v", failure)
		}
		close(port.release)
		if failure = <-done; failure != nil {
			t.Fatalf("idempotent already-sent finalization: %v", failure)
		}
	})

	t.Run("proof mismatch returns outcome finalization error", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim random-mismatch delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted random-mismatch send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `DELETE FROM telegram_mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("remove random-mismatch Telegram delivery: %v", failure)
		}
		if _, failure = database.Exec(context, `INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id) VALUES ($1, $2, $3, nextval('mailing_delivery_random_id_seq'))`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("replace random-mismatch Telegram delivery: %v", failure)
		}
		close(port.release)
		if failure = <-done; !errors.Is(failure, applicationdelivery.ErrOutcomeFinalization) {
			t.Fatalf("random-mismatch finalization = %v, want ErrOutcomeFinalization", failure)
		}
	})

	t.Run("insufficient attempt evidence returns outcome finalization error", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim insufficient-evidence delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted insufficient-evidence send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET status = 'pending', attempt_count = 0, started_at = NULL, lease_token = NULL, lease_until = NULL, lease_execution_generation = NULL WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("regress insufficient-evidence delivery: %v", failure)
		}
		close(port.release)
		if failure = <-done; !errors.Is(failure, applicationdelivery.ErrOutcomeFinalization) {
			t.Fatalf("insufficient-evidence finalization = %v, want ErrOutcomeFinalization", failure)
		}
	})

	t.Run("missing delivery returns outcome finalization error", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim deleted delivery: %v", claimFailure)
		}
		port := newUncancellablePort(nil)
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted deleted send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `DELETE FROM mailings WHERE id = $1`, item.mailingID); failure != nil {
			t.Fatalf("delete late-outcome mailing: %v", failure)
		}
		close(port.release)
		if failure = <-done; !errors.Is(failure, applicationdelivery.ErrOutcomeFinalization) {
			t.Fatalf("deleted-delivery finalization = %v, want ErrOutcomeFinalization", failure)
		}
	})

	t.Run("late failure after reaping is a no-op", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim late-failure delivery: %v", claimFailure)
		}
		port := newUncancellablePort(errors.New("late transport failure"))
		done := make(chan error, 1)
		go func() { done <- task.Execute(context, applicationdelivery.New(deliveries, port)) }()
		select {
		case <-port.entered:
		case <-context.Done():
			t.Fatalf("wait for admitted late-failure send: %v", context.Err())
		}
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("expire late-failure lease: %v", failure)
		}
		result, reapFailure := reap.Reap(context)
		if reapFailure != nil || result.Retried != 1 {
			t.Fatalf("reap late-failure lease = %+v/%v", result, reapFailure)
		}
		close(port.release)
		if failure = <-done; failure != nil {
			t.Fatalf("late failure finalization = %v, want harmless no-op", failure)
		}
		state := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
		if state.status != "pending" || state.attempts != 1 || state.leaseToken != uuid.Nil {
			t.Fatalf("late failure state = %+v", state)
		}
	})

	t.Run("stop quarantines retry-pending and queue rejects unresolved outcomes", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET attempt_count = 1, started_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("create retry-pending stop fixture: %v", failure)
		}
		mailingEntity := pgmailings.NewMailings(database).Mailing(mailing.Identity(item.mailingID))
		if failure = mailingEntity.Stop(context); failure != nil {
			t.Fatalf("stop retry-pending mailing: %v", failure)
		}
		state := readLifecycleDeliveryState(t, context, database, item.pipelineDelivery)
		if state.status != "unknown" || state.errorMessage == "" {
			t.Fatalf("retry-pending stop state = %+v", state)
		}
		if failure = mailingEntity.Queue(context); !errors.Is(failure, mailing.ErrUnresolvedDeliveryOutcomes) {
			t.Fatalf("queue unresolved stopped mailing = %v, want ErrUnresolvedDeliveryOutcomes", failure)
		}
	})

	t.Run("stopped sending delivery also blocks queue", func(t *testing.T) {
		item := createLifecycleDelivery(t, context, database, operatorID, accountID)
		if _, failure = database.Exec(context, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = CURRENT_TIMESTAMP, lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, uuid.New()); failure != nil {
			t.Fatalf("create stopped-sending fixture: %v", failure)
		}
		mailingEntity := pgmailings.NewMailings(database).Mailing(mailing.Identity(item.mailingID))
		if failure = mailingEntity.Stop(context); failure != nil {
			t.Fatalf("stop sending mailing: %v", failure)
		}
		if failure = mailingEntity.Queue(context); !errors.Is(failure, mailing.ErrUnresolvedDeliveryOutcomes) {
			t.Fatalf("queue stopped sending mailing = %v, want ErrUnresolvedDeliveryOutcomes", failure)
		}
	})
}

func createExpiredSendingDelivery(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, operatorID, accountID uuid.UUID, attempts, maxAttempts, generation int) lifecycleDelivery {
	t.Helper()
	item := createLifecycleDelivery(t, context, database, operatorID, accountID)
	if _, failure := database.Exec(context, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = $4, max_attempts = $5, started_at = CURRENT_TIMESTAMP, lease_token = $6, lease_until = CURRENT_TIMESTAMP - INTERVAL '10 seconds', lease_execution_generation = $7, error_message = 'previous transport outcome' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, attempts, maxAttempts, uuid.New(), generation); failure != nil {
		t.Fatalf("create expired sending delivery: %v", failure)
	}
	return item
}

type randomAndState struct {
	randomID     int64
	errorMessage string
	state        deliveryLifecycleState
}

func readRandomAndState(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery) randomAndState {
	t.Helper()
	var result randomAndState
	result.state = readLifecycleDeliveryState(t, context, database, item)
	if failure := database.QueryRow(context, `SELECT random_id FROM telegram_mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID).Scan(&result.randomID); failure != nil {
		t.Fatalf("read stable random ID: %v", failure)
	}
	result.errorMessage = result.state.errorMessage
	return result
}
