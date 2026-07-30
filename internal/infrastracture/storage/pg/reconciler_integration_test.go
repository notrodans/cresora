package pg_test

import (
	stdcontext "context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	applicationdelivery "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
	pginvariants "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/invariants"
	pgmailings "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/mailings"
	pgreconciler "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/reconciler"
)

func TestPostgreSQLDeliveryRunReconciliation(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	context, cancel := stdcontext.WithTimeout(stdcontext.Background(), 60*time.Second)
	defer cancel()
	database, schema, failure := newIsolatedDeliveryPipelineDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	if failure := assertDeliveryPipelineIsolation(context, database, schema); failure != nil {
		t.Fatalf("assert PostgreSQL isolation: %v", failure)
	}

	fixture, failure := createDeliveryPipelineFixture(context, database)
	if failure != nil {
		t.Fatalf("create reconciliation fixture: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if cleanupFailure := cleanupDeliveryPipelineFixture(cleanupContext, database, fixture.operatorID); cleanupFailure != nil {
			t.Errorf("cleanup reconciliation fixture: %v", cleanupFailure)
		}
	})

	successfulSkipped, failure := addReconciliationDelivery(context, database, fixture.deliveries[0], 1)
	if failure != nil {
		t.Fatalf("add successful/skipped aggregate delivery: %v", failure)
	}
	failedUnknown, failure := addReconciliationDelivery(context, database, fixture.deliveries[1], 1)
	if failure != nil {
		t.Fatalf("add failed/unknown aggregate delivery: %v", failure)
	}
	setReconciliationDeliveryState(t, context, database, fixture.deliveries[0], "sent")
	setReconciliationDeliveryState(t, context, database, successfulSkipped, "skipped")
	setReconciliationDeliveryState(t, context, database, fixture.deliveries[1], "failed")
	setReconciliationDeliveryState(t, context, database, failedUnknown, "unknown")
	setReconciliationDeliveryState(t, context, database, fixture.deliveries[2], "unknown")
	setReconciliationDeliveryState(t, context, database, fixture.deliveries[4], "sending")

	reconciler := pgreconciler.New(database, pgreconciler.Config{BatchSize: 100})
	first, failure := reconciler.Reconcile(context)
	if failure != nil {
		t.Fatalf("reconcile terminal fixture: %v", failure)
	}
	if first.Candidates != 3 || first.Completed != 1 || first.Failed != 2 {
		t.Fatalf("first reconciliation result = %+v, want 3 candidates, 1 completed, 2 failed", first)
	}
	assertRunStatus(t, context, database, fixture.deliveries[0], "completed")
	assertRunStatus(t, context, database, fixture.deliveries[1], "failed")
	assertRunStatus(t, context, database, fixture.deliveries[2], "failed")
	assertRunStatus(t, context, database, fixture.deliveries[3], "queued")
	assertRunStatus(t, context, database, fixture.deliveries[4], "queued")
	assertMailingStatus(t, context, database, fixture.deliveries[0].mailingID, "completed")
	assertMailingStatus(t, context, database, fixture.deliveries[1].mailingID, "failed")
	assertMailingStatus(t, context, database, fixture.deliveries[2].mailingID, "failed")
	assertMailingStatus(t, context, database, fixture.deliveries[3].mailingID, "queued")
	assertMailingStatus(t, context, database, fixture.deliveries[4].mailingID, "queued")

	finishedAt := readRunFinishedAt(t, context, database, fixture.deliveries[0])
	mailingUpdatedAt := readMailingUpdatedAt(t, context, database, fixture.deliveries[0].mailingID)
	repeated, failure := reconciler.Reconcile(context)
	if failure != nil {
		t.Fatalf("repeat reconciliation: %v", failure)
	}
	if repeated != (applicationdelivery.ReconciliationResult{}) {
		t.Fatalf("repeat reconciliation result = %+v, want no candidates or transitions", repeated)
	}
	if got := readRunFinishedAt(t, context, database, fixture.deliveries[0]); !got.Equal(finishedAt) {
		t.Fatalf("repeat reconciliation changed finished_at from %s to %s", finishedAt, got)
	}
	if got := readMailingUpdatedAt(t, context, database, fixture.deliveries[0].mailingID); !got.Equal(mailingUpdatedAt) {
		t.Fatalf("repeat reconciliation changed mailing updated_at from %s to %s", mailingUpdatedAt, got)
	}

	mailings := pgmailings.NewMailings(database)
	if failure := mailings.Mailing(mailing.Identity(fixture.deliveries[5].mailingID)).Stop(context); failure != nil {
		t.Fatalf("stop cancellation fixture: %v", failure)
	}
	assertRunStatus(t, context, database, fixture.deliveries[5], "cancelled")
	assertMailingStatus(t, context, database, fixture.deliveries[5].mailingID, "stopped")
	stoppedUpdatedAt := readMailingUpdatedAt(t, context, database, fixture.deliveries[5].mailingID)
	cancelledFinishedAt := readRunFinishedAt(t, context, database, fixture.deliveries[5])
	// A late proved outcome may still make the delivery sent, but it must not
	// reopen or rewrite the run that Stop already cancelled.
	setReconciliationDeliveryState(t, context, database, fixture.deliveries[5], "sent")
	if _, failure := reconciler.Reconcile(context); failure != nil {
		t.Fatalf("reconcile cancelled fixture: %v", failure)
	}
	assertRunStatus(t, context, database, fixture.deliveries[5], "cancelled")
	if got := readRunFinishedAt(t, context, database, fixture.deliveries[5]); !got.Equal(cancelledFinishedAt) {
		t.Fatalf("reconciliation changed cancelled finished_at from %s to %s", cancelledFinishedAt, got)
	}
	if got := readMailingUpdatedAt(t, context, database, fixture.deliveries[5].mailingID); !got.Equal(stoppedUpdatedAt) {
		t.Fatalf("reconciliation changed stopped mailing updated_at from %s to %s", stoppedUpdatedAt, got)
	}

	for _, item := range fixture.deliveries[6:] {
		setReconciliationDeliveryState(t, context, database, item, "sent")
	}
	expected := readRunIDsInCandidateOrder(t, context, database, fixture.deliveries[6:])
	mailingByRun := make(map[uuid.UUID]uuid.UUID, len(fixture.deliveries[6:]))
	for _, item := range fixture.deliveries[6:] {
		mailingByRun[item.runID] = item.mailingID
	}
	bounded := pgreconciler.New(database, pgreconciler.Config{BatchSize: 2})
	pass, failure := bounded.Reconcile(context)
	if failure != nil {
		t.Fatalf("bounded reconciliation: %v", failure)
	}
	if pass.Candidates != 2 || pass.Completed != 2 {
		t.Fatalf("bounded reconciliation result = %+v, want 2 candidates and 2 completed", pass)
	}
	for index, runID := range expected {
		want := "queued"
		if index < 2 {
			want = "completed"
		}
		assertRunStatusByID(t, context, database, mailingByRun[runID], runID, want)
	}

	pass, failure = bounded.Reconcile(context)
	if failure != nil {
		t.Fatalf("second bounded reconciliation: %v", failure)
	}
	if pass.Candidates != 2 || pass.Completed != 2 {
		t.Fatalf("second bounded reconciliation result = %+v, want 2 candidates and 2 completed", pass)
	}

	// Two concurrent passes may discover the same bounded candidates, but row
	// locks and the active/generation CAS allow exactly one terminal write per
	// run and never produce an error for the losing pass.
	resetRunForConcurrentReconciliation(t, context, database, fixture.deliveries[6:8])
	results := make(chan applicationdelivery.ReconciliationResult, 6)
	errors := make(chan error, 6)
	for index := 0; index < 6; index++ {
		go func() {
			result, failure := bounded.Reconcile(context)
			results <- result
			errors <- failure
		}()
	}
	completed := 0
	for index := 0; index < 6; index++ {
		if failure := <-errors; failure != nil {
			t.Fatalf("concurrent reconciliation: %v", failure)
		}
		completed += (<-results).Completed
	}
	if completed != 2 {
		t.Fatalf("concurrent completed transitions = %d, want 2", completed)
	}
	for _, item := range fixture.deliveries[6:8] {
		assertRunStatus(t, context, database, item, "completed")
		assertMailingStatus(t, context, database, item.mailingID, "completed")
	}

	report, failure := pginvariants.New(database).Check(context)
	if failure != nil {
		t.Fatalf("check invariants after reconciliation: %v", failure)
	}
	for _, result := range report.Results {
		if result.Name == pginvariants.CheckMailingRunStatusContradiction && result.Count != 0 {
			t.Fatalf("mailing/run contradiction invariant = %+v, want no violations", result)
		}
	}
}

func TestPostgreSQLDeliveryRunReconciliationSkipsPoisonCandidates(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	context, cancel := stdcontext.WithTimeout(stdcontext.Background(), 60*time.Second)
	defer cancel()
	database, schema, failure := newIsolatedDeliveryPipelineDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	if failure := assertDeliveryPipelineIsolation(context, database, schema); failure != nil {
		t.Fatalf("assert PostgreSQL isolation: %v", failure)
	}

	fixture, failure := createDeliveryPipelineFixture(context, database)
	if failure != nil {
		t.Fatalf("create reconciliation fixture: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if cleanupFailure := cleanupDeliveryPipelineFixture(cleanupContext, database, fixture.operatorID); cleanupFailure != nil {
			t.Errorf("cleanup reconciliation fixture: %v", cleanupFailure)
		}
	})

	emptyMailingID, emptyRunID, failure := createEmptyActiveRun(context, database, fixture.operatorID)
	if failure != nil {
		t.Fatalf("create empty active run: %v", failure)
	}
	valid := fixture.deliveries[0]
	unknown := fixture.deliveries[1]
	setReconciliationDeliveryState(t, context, database, valid, "sent")
	setReconciliationDeliveryState(t, context, database, unknown, "unknown")
	setRunQueuedAt(t, context, database, emptyMailingID, emptyRunID, "1 hour")
	setRunQueuedAt(t, context, database, valid.mailingID, valid.runID, "30 minutes")
	setRunQueuedAt(t, context, database, unknown.mailingID, unknown.runID, "15 minutes")

	reconciler := pgreconciler.New(database, pgreconciler.Config{BatchSize: 1})
	first, failure := reconciler.Reconcile(context)
	if failure != nil {
		t.Fatalf("reconcile bounded poison-candidate fixture: %v", failure)
	}
	if first.Candidates != 1 || first.Completed != 1 || first.Failed != 0 {
		t.Fatalf("first bounded reconciliation result = %+v, want 1 candidate and 1 completed run", first)
	}
	assertRunStatusByID(t, context, database, valid.mailingID, valid.runID, "completed")
	assertMailingStatus(t, context, database, valid.mailingID, "completed")
	assertRunUntouched(t, context, database, emptyMailingID, emptyRunID)
	assertMailingStatus(t, context, database, emptyMailingID, "queued")

	second, failure := reconciler.Reconcile(context)
	if failure != nil {
		t.Fatalf("reconcile bounded unknown fixture: %v", failure)
	}
	if second.Candidates != 1 || second.Completed != 0 || second.Failed != 1 {
		t.Fatalf("second bounded reconciliation result = %+v, want 1 candidate and 1 failed run", second)
	}
	assertRunStatusByID(t, context, database, unknown.mailingID, unknown.runID, "failed")
	assertMailingStatus(t, context, database, unknown.mailingID, "failed")
	assertRunUntouched(t, context, database, emptyMailingID, emptyRunID)
	assertMailingStatus(t, context, database, emptyMailingID, "queued")
}

func addReconciliationDelivery(
	context stdcontext.Context,
	database *pgxpool.Pool,
	item pipelineDelivery,
	position int,
) (pipelineDelivery, error) {
	additional := pipelineDelivery{
		mailingID:   item.mailingID,
		runID:       item.runID,
		recipientID: uuid.New(),
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, $3)`,
		additional.mailingID,
		additional.recipientID,
		position,
	); failure != nil {
		return pipelineDelivery{}, fmt.Errorf("insert additional recipient: %w", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailing_deliveries (mailing_id, run_id, recipient_id, status)
		 VALUES ($1, $2, $3, 'pending')`,
		additional.mailingID,
		additional.runID,
		additional.recipientID,
	); failure != nil {
		return pipelineDelivery{}, fmt.Errorf("insert additional delivery: %w", failure)
	}
	return additional, nil
}

func setReconciliationDeliveryState(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	item pipelineDelivery,
	status string,
) {
	t.Helper()
	var query string
	var arguments []any
	switch status {
	case "sent":
		query = `UPDATE mailing_deliveries
		         SET status = 'sent', sent_at = CURRENT_TIMESTAMP,
		             started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
		             lease_token = NULL, lease_until = NULL,
		             lease_execution_generation = NULL
		         WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`
	case "failed":
		query = `UPDATE mailing_deliveries
		         SET status = 'failed', attempt_count = 1,
		             started_at = CURRENT_TIMESTAMP, error_message = 'test failure',
		             lease_token = NULL, lease_until = NULL,
		             lease_execution_generation = NULL
		         WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`
	case "unknown":
		query = `UPDATE mailing_deliveries
		         SET status = 'unknown', attempt_count = 1,
		             started_at = CURRENT_TIMESTAMP, error_message = 'test unknown',
		             lease_token = NULL, lease_until = NULL,
		             lease_execution_generation = NULL
		         WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`
	case "skipped":
		query = `UPDATE mailing_deliveries
		         SET status = 'skipped', skip_reason = 'test skipped',
		             lease_token = NULL, lease_until = NULL,
		             lease_execution_generation = NULL
		         WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`
	case "sending":
		query = `UPDATE mailing_deliveries
		         SET status = 'sending', attempt_count = 1,
		             started_at = CURRENT_TIMESTAMP, lease_token = $4,
		             lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute',
		             lease_execution_generation = 1
		         WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`
		arguments = append(arguments, uuid.New())
	default:
		t.Fatalf("unsupported reconciliation delivery state %q", status)
	}
	arguments = append([]any{item.mailingID, item.runID, item.recipientID}, arguments...)
	if _, failure := database.Exec(context, query, arguments...); failure != nil {
		t.Fatalf("set delivery %s state: %v", status, failure)
	}
}

func createEmptyActiveRun(
	context stdcontext.Context,
	database *pgxpool.Pool,
	operatorID uuid.UUID,
) (uuid.UUID, uuid.UUID, error) {
	mailingID := uuid.New()
	runID := uuid.New()
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailings (id, operator_id, name, message_text, status)
		 VALUES ($1, $2, $3, $4, 'queued')`,
		mailingID,
		operatorID,
		"empty-reconciliation-"+mailingID.String(),
		"empty reconciliation integration test",
	); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailing_runs (mailing_id, id, number, status, queued_at, execution_generation)
		 VALUES ($1, $2, 1, 'queued', CURRENT_TIMESTAMP, 1)`,
		mailingID,
		runID,
	); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	return mailingID, runID, nil
}

func setRunQueuedAt(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	mailingID, runID uuid.UUID,
	age string,
) {
	t.Helper()
	if _, failure := database.Exec(
		context,
		`UPDATE mailing_runs
		 SET queued_at = CURRENT_TIMESTAMP - $3::interval
		 WHERE mailing_id = $1 AND id = $2`,
		mailingID,
		runID,
		age,
	); failure != nil {
		t.Fatalf("set run %s/%s queue time: %v", mailingID, runID, failure)
	}
}

func assertRunUntouched(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	mailingID, runID uuid.UUID,
) {
	t.Helper()
	var (
		status     string
		startedAt  *time.Time
		finishedAt *time.Time
	)
	if failure := database.QueryRow(
		context,
		`SELECT status::text, started_at, finished_at
		 FROM mailing_runs
		 WHERE mailing_id = $1 AND id = $2`,
		mailingID,
		runID,
	).Scan(&status, &startedAt, &finishedAt); failure != nil {
		t.Fatalf("read untouched run %s/%s: %v", mailingID, runID, failure)
	}
	if status != "queued" || startedAt != nil || finishedAt != nil {
		t.Fatalf("empty run %s/%s = status %q, started_at %v, finished_at %v; want untouched queued run", mailingID, runID, status, startedAt, finishedAt)
	}
}

func assertRunStatus(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery, want string) {
	t.Helper()
	assertRunStatusByID(t, context, database, item.mailingID, item.runID, want)
}

func assertRunStatusByID(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, mailingID, runID uuid.UUID, want string) {
	t.Helper()
	var got string
	if failure := database.QueryRow(context, `SELECT status::text FROM mailing_runs WHERE mailing_id = $1 AND id = $2`, mailingID, runID).Scan(&got); failure != nil {
		t.Fatalf("read run %s/%s status: %v", mailingID, runID, failure)
	}
	if got != want {
		t.Fatalf("run %s/%s status = %q, want %q", mailingID, runID, got, want)
	}
}

func assertMailingStatus(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, mailingID uuid.UUID, want string) {
	t.Helper()
	var got string
	if failure := database.QueryRow(context, `SELECT status::text FROM mailings WHERE id = $1`, mailingID).Scan(&got); failure != nil {
		t.Fatalf("read mailing %s status: %v", mailingID, failure)
	}
	if got != want {
		t.Fatalf("mailing %s status = %q, want %q", mailingID, got, want)
	}
}

func readMailingUpdatedAt(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, mailingID uuid.UUID) time.Time {
	t.Helper()
	var updatedAt time.Time
	if failure := database.QueryRow(context, `SELECT updated_at FROM mailings WHERE id = $1`, mailingID).Scan(&updatedAt); failure != nil {
		t.Fatalf("read mailing updated_at: %v", failure)
	}
	return updatedAt
}

func readRunFinishedAt(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery) time.Time {
	t.Helper()
	var finishedAt time.Time
	if failure := database.QueryRow(context, `SELECT finished_at FROM mailing_runs WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID).Scan(&finishedAt); failure != nil {
		t.Fatalf("read run finished_at: %v", failure)
	}
	return finishedAt
}

func readRunIDsInCandidateOrder(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, items []pipelineDelivery) []uuid.UUID {
	t.Helper()
	if len(items) == 0 {
		return nil
	}
	rows, failure := database.Query(
		context,
		`SELECT run.id
		 FROM mailing_runs AS run
		 JOIN mailings AS mailing ON mailing.id = run.mailing_id
		 WHERE run.mailing_id = ANY($1::uuid[])
		   AND run.status IN ('queued', 'running')
		   AND mailing.status = 'queued'
		 ORDER BY run.queued_at, run.mailing_id, run.id`,
		[]uuid.UUID{items[0].mailingID, items[1].mailingID, items[2].mailingID, items[3].mailingID},
	)
	if failure != nil {
		t.Fatalf("read candidate order: %v", failure)
	}
	defer rows.Close()
	result := make([]uuid.UUID, 0, len(items))
	for rows.Next() {
		var runID uuid.UUID
		if failure := rows.Scan(&runID); failure != nil {
			t.Fatalf("scan candidate order: %v", failure)
		}
		result = append(result, runID)
	}
	if failure := rows.Err(); failure != nil {
		t.Fatalf("iterate candidate order: %v", failure)
	}
	return result
}

func resetRunForConcurrentReconciliation(t *testing.T, context stdcontext.Context, database *pgxpool.Pool, items []pipelineDelivery) {
	t.Helper()
	for _, item := range items {
		if _, failure := database.Exec(
			context,
			`UPDATE mailing_runs
			 SET status = 'queued', started_at = NULL, finished_at = NULL
			 WHERE mailing_id = $1 AND id = $2`,
			item.mailingID,
			item.runID,
		); failure != nil {
			t.Fatalf("reset run for concurrent reconciliation: %v", failure)
		}
		if _, failure := database.Exec(
			context,
			`UPDATE mailings SET status = 'queued' WHERE id = $1`,
			item.mailingID,
		); failure != nil {
			t.Fatalf("reset mailing for concurrent reconciliation: %v", failure)
		}
	}
}
