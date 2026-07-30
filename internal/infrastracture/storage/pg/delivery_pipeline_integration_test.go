package pg_test

import (
	stdcontext "context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	applicationdelivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
	pgclaims "github.com/notrodans/cresora/internal/infrastracture/storage/pg/claims"
	pgdeliveries "github.com/notrodans/cresora/internal/infrastracture/storage/pg/deliveries"
	pgmailings "github.com/notrodans/cresora/internal/infrastracture/storage/pg/mailings"
	"github.com/notrodans/cresora/internal/infrastracture/transport/faketelegram"
)

func TestPostgreSQLDeliveryPipeline(t *testing.T) {
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
	if fixture.operatorID != uuid.Nil {
		t.Cleanup(func() {
			cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
			defer cleanupCancel()
			if cleanupFailure := cleanupDeliveryPipelineFixture(cleanupContext, database, fixture.operatorID); cleanupFailure != nil {
				t.Errorf("cleanup delivery pipeline fixture: %v", cleanupFailure)
			}
		})
	}
	if failure != nil {
		t.Fatalf("create delivery pipeline fixture: %v", failure)
	}

	for _, item := range fixture.deliveries {
		if failure = setDeliveryReady(context, database, item, false); failure != nil {
			t.Fatalf("hold delivery %s before pipeline test: %v", item.mailingID, failure)
		}
	}

	claims := pgclaims.NewClaims(database, time.Minute)
	deliveries := pgdeliveries.NewDeliveries(database)

	t.Run("success sends persisted random ID and marks sent", func(t *testing.T) {
		item := fixture.deliveries[0]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready success delivery: %v", failure)
		}
		fake := faketelegram.New(faketelegram.WithCallRecording(4))
		command := applicationdelivery.New(deliveries, fake)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim success delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, command); executeFailure != nil {
			t.Fatalf("execute success delivery: %v", executeFailure)
		}

		state := readDeliveryState(t, context, database, item)
		if state.status != "sent" {
			t.Fatalf("success delivery status = %q, want sent", state.status)
		}
		if state.attemptCount != 1 {
			t.Fatalf("success delivery attempt count = %d, want 1", state.attemptCount)
		}
		calls := fake.Calls()
		if len(calls) != 1 {
			t.Fatalf("fake call count = %d, want 1", len(calls))
		}
		if calls[0].RandomID != item.randomID {
			t.Fatalf("fake random ID = %d, want persisted ID %d", calls[0].RandomID, item.randomID)
		}
		if calls[0].Error != nil {
			t.Fatalf("fake success call error = %v, want nil", calls[0].Error)
		}
	})

	t.Run("transient transport error schedules a bounded retry with the stable random ID", func(t *testing.T) {
		item := fixture.deliveries[1]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready retry delivery: %v", failure)
		}
		fake := faketelegram.New(
			faketelegram.WithScriptFor(
				item.randomID,
				faketelegram.Step{Outcome: faketelegram.OutcomeTransient},
				faketelegram.Step{Outcome: faketelegram.OutcomeSuccess},
			),
			faketelegram.WithCallRecording(4),
		)
		command := applicationdelivery.New(deliveries, fake)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim retry delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, command); executeFailure != nil {
			t.Fatalf("execute retry delivery: %v", executeFailure)
		}

		state := readDeliveryState(t, context, database, item)
		if state.status != "pending" {
			t.Fatalf("transient-error delivery status = %q, want pending", state.status)
		}
		if state.attemptCount != 1 {
			t.Fatalf("transient-error delivery attempt count = %d, want 1", state.attemptCount)
		}
		if state.errorMessage == "" {
			t.Fatal("transient-error delivery error message is empty")
		}
		if state.leaseToken != uuid.Nil {
			t.Fatal("transient-error delivery lease token is not zero")
		}
		if until := time.Until(state.readyAt); until < 4*time.Second || until > 7*time.Second {
			t.Fatalf("transient-error retry delay = %s, want approximately 5s", until)
		}
		if len(fake.Calls()) != 1 {
			t.Fatalf("fake transient-error call count = %d, want 1", len(fake.Calls()))
		}

		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready second retry attempt: %v", failure)
		}
		retryTask, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim second retry attempt: %v", claimFailure)
		}
		if executeFailure := retryTask.Execute(context, command); executeFailure != nil {
			t.Fatalf("execute second retry attempt: %v", executeFailure)
		}
		state = readDeliveryState(t, context, database, item)
		if state.status != "sent" || state.attemptCount != 2 {
			t.Fatalf("retried delivery state = %+v, want sent after two attempts", state)
		}
		calls := fake.Calls()
		if len(calls) != 2 || calls[0].RandomID != item.randomID || calls[1].RandomID != item.randomID {
			t.Fatalf("retry random IDs = %+v, want two calls with %d", calls, item.randomID)
		}
	})

	t.Run("permanent transport error finalizes exhausted delivery", func(t *testing.T) {
		item := fixture.deliveries[2]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready exhausted delivery: %v", failure)
		}
		if _, failure := database.Exec(
			context,
			`UPDATE mailing_deliveries SET max_attempts = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
			item.mailingID,
			item.runID,
			item.recipientID,
		); failure != nil {
			t.Fatalf("set exhausted delivery attempt limit: %v", failure)
		}
		fake := faketelegram.New(
			faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomePermanent}),
			faketelegram.WithCallRecording(4),
		)
		command := applicationdelivery.New(deliveries, fake)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim exhausted delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, command); executeFailure != nil {
			t.Fatalf("execute exhausted delivery: %v", executeFailure)
		}

		state := readDeliveryState(t, context, database, item)
		if state.status != "failed" {
			t.Fatalf("exhausted delivery status = %q, want failed", state.status)
		}
		if state.attemptCount != 1 {
			t.Fatalf("exhausted delivery attempt count = %d, want 1", state.attemptCount)
		}
		if state.errorMessage == "" {
			t.Fatal("exhausted delivery error message is empty")
		}
		if state.leaseToken != uuid.Nil {
			t.Fatal("exhausted delivery lease token is not zero")
		}
		if len(fake.Calls()) != 1 {
			t.Fatalf("fake exhausted call count = %d, want 1", len(fake.Calls()))
		}
	})

	t.Run("FloodWait schedules the exact server duration below the attempt limit", func(t *testing.T) {
		item := fixture.deliveries[3]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready FloodWait delivery: %v", failure)
		}
		fake := faketelegram.New(
			faketelegram.WithDefault(faketelegram.Step{
				Outcome:   faketelegram.OutcomeFloodWait,
				FloodWait: 3 * time.Second,
			}),
			faketelegram.WithCallRecording(4),
		)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim FloodWait delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, applicationdelivery.New(deliveries, fake)); executeFailure != nil {
			t.Fatalf("execute FloodWait delivery: %v", executeFailure)
		}

		state := readDeliveryState(t, context, database, item)
		if state.status != "pending" || state.leaseToken != uuid.Nil {
			t.Fatalf("FloodWait state = %+v, want pending without lease", state)
		}
		var readyAt, updatedAt time.Time
		if failure := database.QueryRow(
			context,
			`SELECT ready_at, updated_at FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
			item.mailingID,
			item.runID,
			item.recipientID,
		).Scan(&readyAt, &updatedAt); failure != nil {
			t.Fatalf("read FloodWait database timestamps: %v", failure)
		}
		if !readyAt.Equal(updatedAt.Add(3 * time.Second)) {
			t.Fatalf("FloodWait ready_at = %s, updated_at = %s, want exact database interval of 3s", readyAt, updatedAt)
		}
	})

	t.Run("transient at maximum attempts becomes failed", func(t *testing.T) {
		item := fixture.deliveries[5]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready exhausted transient delivery: %v", failure)
		}
		if _, failure := database.Exec(context, `UPDATE mailing_deliveries SET max_attempts = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("set exhausted transient attempt limit: %v", failure)
		}
		fake := faketelegram.New(
			faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomeTransient}),
			faketelegram.WithCallRecording(2),
		)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim exhausted transient delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, applicationdelivery.New(deliveries, fake)); executeFailure != nil {
			t.Fatalf("execute exhausted transient delivery: %v", executeFailure)
		}
		state := readDeliveryState(t, context, database, item)
		if state.status != "failed" || state.attemptCount != 1 || state.leaseToken != uuid.Nil || state.errorMessage == "" {
			t.Fatalf("exhausted transient state = %+v, want failed with evidence and no lease", state)
		}
		if len(fake.Calls()) != 1 {
			t.Fatalf("exhausted transient call count = %d, want 1", len(fake.Calls()))
		}
	})

	t.Run("FloodWait at maximum attempts becomes failed", func(t *testing.T) {
		item := fixture.deliveries[6]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready exhausted FloodWait delivery: %v", failure)
		}
		if _, failure := database.Exec(context, `UPDATE mailing_deliveries SET max_attempts = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("set exhausted FloodWait attempt limit: %v", failure)
		}
		fake := faketelegram.New(
			faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomeFloodWait, FloodWait: 3 * time.Second}),
			faketelegram.WithCallRecording(2),
		)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim exhausted FloodWait delivery: %v", claimFailure)
		}
		if executeFailure := task.Execute(context, applicationdelivery.New(deliveries, fake)); executeFailure != nil {
			t.Fatalf("execute exhausted FloodWait delivery: %v", executeFailure)
		}
		state := readDeliveryState(t, context, database, item)
		if state.status != "failed" || state.attemptCount != 1 || state.leaseToken != uuid.Nil || state.errorMessage == "" {
			t.Fatalf("exhausted FloodWait state = %+v, want failed with evidence and no lease", state)
		}
		if len(fake.Calls()) != 1 {
			t.Fatalf("exhausted FloodWait call count = %d, want 1", len(fake.Calls()))
		}
	})

	for _, test := range []struct {
		name      string
		itemIndex int
		mutate    func(stdcontext.Context, *pgxpool.Pool, pipelineDelivery) error
	}{
		{
			name:      "stale negative token is a no-op",
			itemIndex: 7,
			mutate: func(context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery) error {
				_, failure := database.Exec(context, `UPDATE mailing_deliveries SET lease_token = $4 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, uuid.New())
				return failure
			},
		},
		{
			name:      "stale negative attempt is a no-op",
			itemIndex: 8,
			mutate: func(context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery) error {
				_, failure := database.Exec(context, `UPDATE mailing_deliveries SET attempt_count = 2 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID)
				return failure
			},
		},
		{
			name:      "stale negative random ID is a no-op",
			itemIndex: 9,
			mutate: func(context stdcontext.Context, database *pgxpool.Pool, item pipelineDelivery) error {
				if _, failure := database.Exec(context, `DELETE FROM telegram_mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
					return failure
				}
				_, failure := database.Exec(context, `INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id) VALUES ($1, $2, $3, $4)`, item.mailingID, item.runID, item.recipientID, item.randomID+1_000_000)
				return failure
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			item := fixture.deliveries[test.itemIndex]
			if failure := setDeliveryReady(context, database, item, true); failure != nil {
				t.Fatalf("ready stale-negative delivery: %v", failure)
			}
			task, claimFailure := claims.Claim(context)
			if claimFailure != nil {
				t.Fatalf("claim stale-negative delivery: %v", claimFailure)
			}
			port := mutatingFailurePort{database: database, item: item, mutate: test.mutate}
			if executeFailure := task.Execute(context, applicationdelivery.New(deliveries, &port)); executeFailure != nil {
				t.Fatalf("execute stale-negative delivery: %v", executeFailure)
			}
			state := readDeliveryState(t, context, database, item)
			if state.status != "sending" || state.attemptCount < 1 || state.leaseToken == uuid.Nil {
				t.Fatalf("stale-negative state = %+v, want unchanged sending lease", state)
			}
		})
	}

	t.Run("stale fencing token skips transport", func(t *testing.T) {
		item := fixture.deliveries[4]
		if failure := setDeliveryReady(context, database, item, true); failure != nil {
			t.Fatalf("ready stale delivery: %v", failure)
		}
		fake := faketelegram.New(faketelegram.WithCallRecording(4))
		command := applicationdelivery.New(deliveries, fake)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim stale delivery: %v", claimFailure)
		}
		if _, failure := database.Exec(
			context,
			`UPDATE mailing_deliveries SET lease_token = $4
			 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
			item.mailingID,
			item.runID,
			item.recipientID,
			uuid.New(),
		); failure != nil {
			t.Fatalf("replace stale delivery fencing token: %v", failure)
		}
		if executeFailure := task.Execute(context, command); executeFailure == nil {
			t.Fatal("execute stale delivery returned nil, want fencing error")
		}
		if calls := fake.Calls(); len(calls) != 0 {
			t.Fatalf("fake call count for stale delivery = %d, want 0", len(calls))
		}

		state := readDeliveryState(t, context, database, item)
		if state.status != "pending" {
			t.Fatalf("stale delivery status = %q, want pending", state.status)
		}
		if state.attemptCount != 0 {
			t.Fatalf("stale delivery attempt count = %d, want 0", state.attemptCount)
		}
	})
}

type deliveryPipelineFixture struct {
	operatorID uuid.UUID
	deliveries []pipelineDelivery
}

type pipelineDelivery struct {
	mailingID   uuid.UUID
	runID       uuid.UUID
	recipientID uuid.UUID
	randomID    int64
}

func createDeliveryPipelineFixture(
	context stdcontext.Context,
	database *pgxpool.Pool,
) (deliveryPipelineFixture, error) {
	fixture := deliveryPipelineFixture{operatorID: uuid.New()}
	if _, failure := database.Exec(
		context,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		fixture.operatorID,
		"delivery-pipeline-"+fixture.operatorID.String(),
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
		"pipeline_"+accountID.String()[:8],
		"Pipeline Test",
		1,
	); failure != nil {
		return fixture, fmt.Errorf("insert account: %w", failure)
	}

	mailings := pgmailings.NewMailings(database)
	fixture.deliveries = make([]pipelineDelivery, 0, 10)
	for index := 0; index < 10; index++ {
		item, failure := createQueuedDelivery(context, database, mailings, fixture.operatorID, accountID, index)
		if failure != nil {
			return fixture, failure
		}
		fixture.deliveries = append(fixture.deliveries, item)
	}
	return fixture, nil
}

func createQueuedDelivery(
	context stdcontext.Context,
	database *pgxpool.Pool,
	mailings interface {
		Mailing(mailing.ID) mailing.Mailing
	},
	operatorID, accountID uuid.UUID,
	index int,
) (pipelineDelivery, error) {
	item := pipelineDelivery{mailingID: uuid.New(), recipientID: uuid.New()}
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailings (id, operator_id, name, message_text, status)
		 VALUES ($1, $2, $3, $4, 'draft')`,
		item.mailingID,
		operatorID,
		fmt.Sprintf("delivery-pipeline-%d-%s", index, item.mailingID),
		"delivery pipeline integration test",
	); failure != nil {
		return item, fmt.Errorf("insert mailing: %w", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $2)`,
		item.mailingID,
		accountID,
	); failure != nil {
		return item, fmt.Errorf("insert mailing route: %w", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, 0)`,
		item.mailingID,
		item.recipientID,
	); failure != nil {
		return item, fmt.Errorf("insert recipient: %w", failure)
	}
	if failure := mailings.Mailing(mailing.Identity(item.mailingID)).Queue(context); failure != nil {
		return item, fmt.Errorf("queue mailing: %w", failure)
	}
	failure := database.QueryRow(
		context,
		`SELECT run_id, random_id
		 FROM telegram_mailing_deliveries
		 WHERE mailing_id = $1 AND recipient_id = $2`,
		item.mailingID,
		item.recipientID,
	).Scan(&item.runID, &item.randomID)
	if failure != nil {
		return item, fmt.Errorf("read queued delivery: %w", failure)
	}
	return item, nil
}

type deliveryState struct {
	status       string
	attemptCount int
	errorMessage string
	leaseToken   uuid.UUID
	readyAt      time.Time
}

func readDeliveryState(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	item pipelineDelivery,
) deliveryState {
	t.Helper()
	var state deliveryState
	if failure := database.QueryRow(
		context,
		`SELECT status::text, attempt_count, COALESCE(error_message, ''), COALESCE(lease_token, '00000000-0000-0000-0000-000000000000'), ready_at
		 FROM mailing_deliveries
		 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
		item.mailingID,
		item.runID,
		item.recipientID,
	).Scan(&state.status, &state.attemptCount, &state.errorMessage, &state.leaseToken, &state.readyAt); failure != nil {
		t.Fatalf("read delivery state: %v", failure)
	}
	return state
}

type mutatingFailurePort struct {
	database *pgxpool.Pool
	item     pipelineDelivery
	mutate   func(stdcontext.Context, *pgxpool.Pool, pipelineDelivery) error
}

func (port *mutatingFailurePort) Send(
	context stdcontext.Context,
	_ recipient.Recipient,
	_ message.Message,
	_ int64,
) error {
	if failure := port.mutate(context, port.database, port.item); failure != nil {
		return failure
	}
	return errors.New("stale negative test failure")
}

func setDeliveryReady(
	context stdcontext.Context,
	database *pgxpool.Pool,
	item pipelineDelivery,
	ready bool,
) error {
	readyAt := `CURRENT_TIMESTAMP + INTERVAL '1 hour'`
	if ready {
		readyAt = `CURRENT_TIMESTAMP`
	}
	_, failure := database.Exec(
		context,
		`UPDATE mailing_deliveries
		 SET ready_at = `+readyAt+`, updated_at = CURRENT_TIMESTAMP
		 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
		item.mailingID,
		item.runID,
		item.recipientID,
	)
	return failure
}

func cleanupDeliveryPipelineFixture(context stdcontext.Context, database *pgxpool.Pool, operatorID uuid.UUID) error {
	if _, failure := database.Exec(context, `DELETE FROM mailings WHERE operator_id = $1`, operatorID); failure != nil {
		return fmt.Errorf("delete fixture mailings: %w", failure)
	}
	if _, failure := database.Exec(context, `DELETE FROM operators WHERE id = $1`, operatorID); failure != nil {
		return fmt.Errorf("delete fixture operator: %w", failure)
	}
	return nil
}

func applyDeliveryPipelineMigrations(context stdcontext.Context, databaseURL string) error {
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
		return fmt.Errorf("locate delivery pipeline integration test")
	}
	provider, failure := goose.NewProvider(
		goose.DialectPostgres,
		database,
		os.DirFS(filepath.Join(filepath.Dir(filename), "../../../../migrations")),
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
