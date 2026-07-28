package invariants_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/invariants"
)

func TestPostgreSQLInvariants(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, _, failure := newIsolatedInvariantDatabase(ctx, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		t.Fatalf("ping PostgreSQL: %v", failure)
	}

	checker := invariants.New(database)
	baseline, failure := checker.Check(ctx)
	if failure != nil {
		t.Fatalf("run baseline invariant check: %v", failure)
	}

	fixture, failure := createFixture(ctx, database)
	if fixture.operatorID != uuid.Nil {
		t.Cleanup(func() {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			if cleanupFailure := cleanupFixture(cleanupContext, database, fixture.operatorID); cleanupFailure != nil {
				t.Errorf("cleanup invariant fixture: %v", cleanupFailure)
			}
		})
	}
	if failure != nil {
		t.Fatalf("create invariant fixture: %v", failure)
	}

	beforeState, failure := fixtureState(ctx, database, fixture.mailingIDs)
	if failure != nil {
		t.Fatalf("snapshot fixture before check: %v", failure)
	}
	observed, failure := checker.Check(ctx)
	if failure != nil {
		t.Fatalf("run invariant check: %v", failure)
	}
	afterState, failure := fixtureState(ctx, database, fixture.mailingIDs)
	if failure != nil {
		t.Fatalf("snapshot fixture after check: %v", failure)
	}
	if beforeState != afterState {
		t.Fatalf("checker changed fixture state: before=%q after=%q", beforeState, afterState)
	}

	baselineCounts := resultCounts(baseline)
	observedCounts := resultCounts(observed)
	for _, name := range []invariants.CheckName{
		invariants.CheckSendingDeliveryWithStaleExecutionGeneration,
		invariants.CheckSendingDeliveryWithInactiveParent,
	} {
		if baselineCounts[name] != 0 {
			t.Fatalf("clean baseline check %q count = %d, want 0", name, baselineCounts[name])
		}
	}
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckStoppedMailingWithClaimableDelivery, 0)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckCancelledRunWithClaimableDelivery, 0)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckSendingDeliveryWithoutLease, 0)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckExpiredSendingLease, 1)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckRunStatusTimestampContradiction, 1)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckMailingRunStatusContradiction, 4)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckSendingDeliveryWithStaleExecutionGeneration, 1)
	assertCountDelta(t, baselineCounts, observedCounts, invariants.CheckSendingDeliveryWithInactiveParent, 1)
	assertSample(t, observed, invariants.CheckSendingDeliveryWithStaleExecutionGeneration, fixture.generationMismatch)
	assertSample(t, observed, invariants.CheckSendingDeliveryWithInactiveParent, fixture.inactiveParent)

	for _, result := range observed.Results {
		if len(result.Sample) > invariants.DefaultSampleLimit {
			t.Fatalf("check %q returned %d samples, want at most %d", result.Name, len(result.Sample), invariants.DefaultSampleLimit)
		}
	}
}

type fixture struct {
	operatorID         uuid.UUID
	mailingIDs         []uuid.UUID
	generationMismatch invariants.Sample
	inactiveParent     invariants.Sample
}

func createFixture(context context.Context, database *pgxpool.Pool) (fixture, error) {
	fixture := fixture{operatorID: uuid.New()}
	if _, failure := database.Exec(
		context,
		`INSERT INTO operators (id, username, password) VALUES ($1, $2, $3)`,
		fixture.operatorID,
		"invariantcheck-"+fixture.operatorID.String(),
		"test-password",
	); failure != nil {
		return fixture, fmt.Errorf("insert operator: %w", failure)
	}
	accountID := uuid.New()
	if _, failure := database.Exec(
		context,
		`INSERT INTO operator_accounts
		 (id, operator_id, phone, telegram_username, telegram_first_name, api_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		accountID,
		fixture.operatorID,
		"+19990000001",
		"invariant_"+accountID.String()[0:8],
		"Invariant Test",
		1,
	); failure != nil {
		return fixture, fmt.Errorf("insert account: %w", failure)
	}

	mailingIDs := make([]uuid.UUID, 0, 8)
	insertMailing := func(status string) (uuid.UUID, error) {
		mailingID := uuid.New()
		_, failure := database.Exec(
			context,
			`INSERT INTO mailings (id, operator_id, name, message_text, status)
			 VALUES ($1, $2, $3, $4, $5)`,
			mailingID,
			fixture.operatorID,
			"invariant-"+mailingID.String(),
			"invariant checker fixture",
			status,
		)
		if failure != nil {
			return uuid.Nil, failure
		}
		mailingIDs = append(mailingIDs, mailingID)
		return mailingID, nil
	}

	stoppedMailing, failure := insertMailing("stopped")
	if failure != nil {
		return fixture, fmt.Errorf("insert stopped mailing: %w", failure)
	}
	cancelledMailing, failure := insertMailing("queued")
	if failure != nil {
		return fixture, fmt.Errorf("insert cancelled-run mailing: %w", failure)
	}
	noLeaseMailing, failure := insertMailing("queued")
	if failure != nil {
		return fixture, fmt.Errorf("insert no-lease mailing: %w", failure)
	}
	expiredLeaseMailing, failure := insertMailing("queued")
	if failure != nil {
		return fixture, fmt.Errorf("insert expired-lease mailing: %w", failure)
	}
	timestampMailing, failure := insertMailing("queued")
	if failure != nil {
		return fixture, fmt.Errorf("insert timestamp mailing: %w", failure)
	}
	statusMailing, failure := insertMailing("running")
	if failure != nil {
		return fixture, fmt.Errorf("insert status mailing: %w", failure)
	}
	generationMismatchMailing, failure := insertMailing("running")
	if failure != nil {
		return fixture, fmt.Errorf("insert generation-mismatch mailing: %w", failure)
	}
	noRouteMailing, failure := insertMailing("stopped")
	if failure != nil {
		return fixture, fmt.Errorf("insert no-route mailing: %w", failure)
	}

	for _, mailingID := range mailingIDs[:6] {
		if _, failure = database.Exec(
			context,
			`INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $2)`,
			mailingID,
			accountID,
		); failure != nil {
			return fixture, fmt.Errorf("insert route for mailing %s: %w", mailingID, failure)
		}
	}

	insertRun := func(mailingID uuid.UUID, status string, timestamps string) (uuid.UUID, uuid.UUID, error) {
		runID := uuid.New()
		recipientID := uuid.New()
		if _, failure := database.Exec(
			context,
			`INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, $3)`,
			mailingID,
			recipientID,
			0,
		); failure != nil {
			return uuid.Nil, uuid.Nil, failure
		}
		query := `INSERT INTO mailing_runs (mailing_id, id, number, status`
		values := ` VALUES ($1, $2, 1, $3`
		switch timestamps {
		case "cancelled":
			query += `, queued_at, started_at, finished_at)`
			values += `, CURRENT_TIMESTAMP - INTERVAL '1 second', CURRENT_TIMESTAMP - INTERVAL '1 second', CURRENT_TIMESTAMP)`
		case "running":
			query += `, queued_at, started_at)`
			values += `, CURRENT_TIMESTAMP - INTERVAL '1 second', CURRENT_TIMESTAMP)`
		case "timestamp":
			query += `, queued_at, started_at)`
			values += `, CURRENT_TIMESTAMP - INTERVAL '1 second', CURRENT_TIMESTAMP)`
		default:
			query += `)`
			values += `)`
		}
		if _, failure := database.Exec(context, query+values, mailingID, runID, status); failure != nil {
			return uuid.Nil, uuid.Nil, failure
		}
		return runID, recipientID, nil
	}

	stoppedRun, stoppedRecipient, failure := insertRun(stoppedMailing, "queued", "")
	if failure != nil {
		return fixture, fmt.Errorf("insert stopped run: %w", failure)
	}
	if failure = insertDelivery(context, database, stoppedMailing, stoppedRun, stoppedRecipient, "pending", "ready"); failure != nil {
		return fixture, fmt.Errorf("insert stopped delivery: %w", failure)
	}
	if _, failure = database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET attempt_count = max_attempts,
		     lease_token = $4,
		     lease_until = CURRENT_TIMESTAMP + INTERVAL '1 hour',
		     lease_execution_generation = 1
		 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
		stoppedMailing,
		stoppedRun,
		stoppedRecipient,
		uuid.New(),
	); failure != nil {
		return fixture, fmt.Errorf("make stopped pending delivery exhausted and leased: %w", failure)
	}

	cancelledRun, cancelledRecipient, failure := insertRun(cancelledMailing, "cancelled", "cancelled")
	if failure != nil {
		return fixture, fmt.Errorf("insert cancelled run: %w", failure)
	}
	if failure = insertDelivery(context, database, cancelledMailing, cancelledRun, cancelledRecipient, "sending", "expired"); failure != nil {
		return fixture, fmt.Errorf("insert cancelled delivery: %w", failure)
	}
	if _, failure = database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
		 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
		cancelledMailing,
		cancelledRun,
		cancelledRecipient,
	); failure != nil {
		return fixture, fmt.Errorf("make cancelled sending lease recently expired: %w", failure)
	}
	fixture.inactiveParent = invariants.Sample{
		MailingID:   cancelledMailing,
		RunID:       cancelledRun,
		RecipientID: cancelledRecipient,
	}

	noLeaseRun, noLeaseRecipient, failure := insertRun(noLeaseMailing, "queued", "")
	if failure != nil {
		return fixture, fmt.Errorf("insert no-lease run: %w", failure)
	}
	if failure = insertDelivery(context, database, noLeaseMailing, noLeaseRun, noLeaseRecipient, "pending", "no-lease"); failure != nil {
		return fixture, fmt.Errorf("insert no-lease delivery: %w", failure)
	}

	expiredRun, expiredRecipient, failure := insertRun(expiredLeaseMailing, "queued", "")
	if failure != nil {
		return fixture, fmt.Errorf("insert expired-lease run: %w", failure)
	}
	if failure = insertDelivery(context, database, expiredLeaseMailing, expiredRun, expiredRecipient, "sending", "expired"); failure != nil {
		return fixture, fmt.Errorf("insert expired-lease delivery: %w", failure)
	}

	if _, _, failure = insertRun(timestampMailing, "queued", "timestamp"); failure != nil {
		return fixture, fmt.Errorf("insert timestamp run: %w", failure)
	}
	if _, _, failure = insertRun(statusMailing, "queued", ""); failure != nil {
		return fixture, fmt.Errorf("insert status run: %w", failure)
	}
	generationMismatchRun, generationMismatchRecipient, failure := insertRun(generationMismatchMailing, "running", "running")
	if failure != nil {
		return fixture, fmt.Errorf("insert generation-mismatch run: %w", failure)
	}
	if failure = insertDelivery(context, database, generationMismatchMailing, generationMismatchRun, generationMismatchRecipient, "sending", "leased"); failure != nil {
		return fixture, fmt.Errorf("insert generation-mismatch delivery: %w", failure)
	}
	if _, failure = database.Exec(
		context,
		`UPDATE mailing_runs SET execution_generation = 2 WHERE mailing_id = $1 AND id = $2`,
		generationMismatchMailing,
		generationMismatchRun,
	); failure != nil {
		return fixture, fmt.Errorf("make delivery generation stale: %w", failure)
	}
	fixture.generationMismatch = invariants.Sample{
		MailingID:   generationMismatchMailing,
		RunID:       generationMismatchRun,
		RecipientID: generationMismatchRecipient,
	}
	noRouteRun, noRouteRecipient, failure := insertRun(noRouteMailing, "queued", "")
	if failure != nil {
		return fixture, fmt.Errorf("insert no-route run: %w", failure)
	}
	if failure = insertDelivery(context, database, noRouteMailing, noRouteRun, noRouteRecipient, "pending", "ready"); failure != nil {
		return fixture, fmt.Errorf("insert no-route delivery: %w", failure)
	}

	fixture.mailingIDs = mailingIDs
	return fixture, nil
}

func insertDelivery(
	context context.Context,
	database *pgxpool.Pool,
	mailingID, runID, recipientID uuid.UUID,
	status, kind string,
) error {
	query := `INSERT INTO mailing_deliveries
		(mailing_id, run_id, recipient_id, status, ready_at, lease_token, lease_until, lease_execution_generation)
		VALUES ($1, $2, $3, $4, CASE WHEN $5 = 'future' THEN CURRENT_TIMESTAMP + INTERVAL '1 hour' ELSE CURRENT_TIMESTAMP - INTERVAL '1 minute' END,
		        CASE WHEN $5 IN ('expired', 'leased') THEN $6::uuid ELSE NULL END,
		        CASE WHEN $5 = 'expired' THEN CURRENT_TIMESTAMP - INTERVAL '1 minute'
		             WHEN $5 = 'leased' THEN CURRENT_TIMESTAMP + INTERVAL '1 hour'
		             ELSE NULL END,
		        CASE WHEN $5 IN ('expired', 'leased') THEN 1 ELSE NULL END)`
	_, failure := database.Exec(context, query, mailingID, runID, recipientID, status, kind, uuid.New())
	return failure
}

func cleanupFixture(context context.Context, database *pgxpool.Pool, operatorID uuid.UUID) error {
	if _, failure := database.Exec(context, `DELETE FROM mailings WHERE operator_id = $1`, operatorID); failure != nil {
		return fmt.Errorf("delete fixture mailings: %w", failure)
	}
	if _, failure := database.Exec(context, `DELETE FROM operators WHERE id = $1`, operatorID); failure != nil {
		return fmt.Errorf("delete fixture operator: %w", failure)
	}
	return nil
}

func fixtureState(context context.Context, database *pgxpool.Pool, mailingIDs []uuid.UUID) (string, error) {
	var state string
	failure := database.QueryRow(
		context,
		`SELECT md5(COALESCE(string_agg(
			format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
			       delivery.mailing_id,
			       delivery.run_id,
			       delivery.recipient_id,
			       mailing.status,
			       run.status,
			       run.execution_generation,
			       delivery.status,
			       delivery.attempt_count,
			       delivery.ready_at,
			       COALESCE(delivery.started_at::text, ''),
			       COALESCE(delivery.lease_token::text, ''),
			       COALESCE(delivery.lease_until::text, ''),
			       COALESCE(delivery.lease_execution_generation::text, '')),
			'|' ORDER BY delivery.mailing_id, delivery.run_id, delivery.recipient_id), ''))
		 FROM mailing_deliveries AS delivery
		 JOIN mailings AS mailing
		   ON mailing.id = delivery.mailing_id
		 JOIN mailing_runs AS run
		   ON run.mailing_id = delivery.mailing_id
		  AND run.id = delivery.run_id
		 WHERE delivery.mailing_id = ANY($1)`,
		mailingIDs,
	).Scan(&state)
	if failure != nil {
		return "", fmt.Errorf("read fixture state: %w", failure)
	}
	return state, nil
}

func resultCounts(report invariants.Report) map[invariants.CheckName]int64 {
	counts := make(map[invariants.CheckName]int64, len(report.Results))
	for _, result := range report.Results {
		counts[result.Name] = result.Count
	}
	return counts
}

func assertCountDelta(
	t *testing.T,
	baseline, observed map[invariants.CheckName]int64,
	name invariants.CheckName,
	want int64,
) {
	t.Helper()
	delta := observed[name] - baseline[name]
	if delta != want {
		t.Fatalf("check %q count delta = %d, want %d (baseline=%d observed=%d)", name, delta, want, baseline[name], observed[name])
	}
}

func assertSample(t *testing.T, report invariants.Report, name invariants.CheckName, want invariants.Sample) {
	t.Helper()
	for _, result := range report.Results {
		if result.Name != name {
			continue
		}
		if len(result.Sample) != 1 {
			t.Fatalf("check %q samples = %#v, want one stable sample", name, result.Sample)
		}
		if result.Sample[0] != want {
			t.Fatalf("check %q sample = %#v, want %#v", name, result.Sample[0], want)
		}
		return
	}
	t.Fatalf("check %q missing from report", name)
}

func newIsolatedInvariantDatabase(
	ctx context.Context,
	t *testing.T,
	databaseURL string,
) (*pgxpool.Pool, string, error) {
	t.Helper()
	baseConfig, failure := pgxpool.ParseConfig(databaseURL)
	if failure != nil {
		return nil, "", fmt.Errorf("parse PostgreSQL URL: %w", failure)
	}
	adminDatabase, failure := pgxpool.NewWithConfig(ctx, baseConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open PostgreSQL admin pool: %w", failure)
	}
	if failure = adminDatabase.Ping(ctx); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("ping PostgreSQL admin pool: %w", failure)
	}

	schema := "invariant_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if _, failure = adminDatabase.Exec(ctx, "CREATE SCHEMA "+quotedSchema); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("create isolated schema: %w", failure)
	}

	var database *pgxpool.Pool
	t.Cleanup(func() {
		if database != nil {
			database.Close()
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := adminDatabase.Exec(cleanupContext, "DROP SCHEMA "+quotedSchema+" CASCADE"); cleanupFailure != nil {
			t.Errorf("drop isolated schema %q: %v", schema, cleanupFailure)
		}
		adminDatabase.Close()
	})

	isolatedConfig := baseConfig.Copy()
	if isolatedConfig.ConnConfig.RuntimeParams == nil {
		isolatedConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	options := isolatedConfig.ConnConfig.RuntimeParams["options"]
	if options != "" {
		options += " "
	}
	isolatedConfig.ConnConfig.RuntimeParams["options"] = options + "-c search_path=" + schema
	isolatedURL, failure := invariantDatabaseURL(databaseURL, schema)
	if failure != nil {
		return nil, "", failure
	}
	if failure = applyInvariantMigrations(ctx, isolatedURL); failure != nil {
		return nil, "", fmt.Errorf("apply migrations to isolated schema: %w", failure)
	}
	database, failure = pgxpool.NewWithConfig(ctx, isolatedConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open isolated PostgreSQL pool: %w", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		database.Close()
		return nil, "", fmt.Errorf("ping isolated PostgreSQL pool: %w", failure)
	}
	return database, schema, nil
}

func invariantDatabaseURL(databaseURL, schema string) (string, error) {
	parsedURL, failure := url.Parse(databaseURL)
	if failure != nil {
		return "", fmt.Errorf("parse isolated PostgreSQL URL: %w", failure)
	}
	if parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql" {
		return "", fmt.Errorf("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	options := query.Get("options")
	if options != "" {
		options += " "
	}
	query.Set("options", options+"-c search_path="+schema)
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String(), nil
}

func applyInvariantMigrations(context context.Context, databaseURL string) error {
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		return fmt.Errorf("open migration database: %w", failure)
	}
	defer database.Close()
	if failure = database.PingContext(context); failure != nil {
		return fmt.Errorf("ping migration database: %w", failure)
	}

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate invariant integration test")
	}
	provider, failure := goose.NewProvider(
		goose.DialectPostgres,
		database,
		os.DirFS(filepath.Join(filepath.Dir(filename), "../../../../../migrations")),
		goose.WithAllowOutofOrder(true),
	)
	if failure != nil {
		return fmt.Errorf("create migration provider: %w", failure)
	}
	if _, failure = provider.Up(context); failure == nil {
		return fmt.Errorf("apply migrations without delivery execution v2 acknowledgement")
	}
	if _, failure = database.ExecContext(context, `INSERT INTO delivery_execution_v2_cutover_ack (acknowledgement_id, acknowledged_by) VALUES (TRUE, current_user)`); failure != nil {
		return fmt.Errorf("acknowledge delivery execution v2 cutover: %w", failure)
	}
	if _, failure = provider.Up(context); failure != nil {
		return fmt.Errorf("apply acknowledged migrations: %w", failure)
	}
	return nil
}
