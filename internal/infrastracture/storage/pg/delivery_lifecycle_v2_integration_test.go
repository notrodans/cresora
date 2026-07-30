package pg_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib"
	applicationdelivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
	pgclaims "github.com/notrodans/cresora/internal/infrastracture/storage/pg/claims"
	pgdeliveries "github.com/notrodans/cresora/internal/infrastracture/storage/pg/deliveries"
	pgmailings "github.com/notrodans/cresora/internal/infrastracture/storage/pg/mailings"
	"github.com/notrodans/cresora/internal/infrastracture/transport/faketelegram"
)

const preDeliveryExecutionV2Migration int64 = 20260726100000

func TestDeliveryExecutionV2MigrationCutover(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	migrationDatabase, database, _, provider, failure := newPreV2MigrationDatabase(ctx, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare pre-v2 schema: %v", failure)
	}
	operatorID, accountID, failure := createPreV2LifecycleAccount(ctx, database)
	if failure != nil {
		t.Fatalf("create cutover account: %v", failure)
	}
	draftID := uuid.New()
	activeID := uuid.New()
	if _, failure = database.Exec(ctx, `INSERT INTO mailings (id, operator_id, name, message_text, status) VALUES ($1, $3, 'draft to preserve', 'draft message', 'draft'), ($2, $3, 'active to stop', 'active message', 'running')`, draftID, activeID, operatorID); failure != nil {
		t.Fatalf("insert cutover mailings: %v", failure)
	}
	if _, failure = database.Exec(ctx, `INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $3), ($2, $3)`, draftID, activeID, accountID); failure != nil {
		t.Fatalf("insert cutover routes: %v", failure)
	}
	draftRecipientID := uuid.New()
	activeRecipientID := uuid.New()
	if _, failure = database.Exec(ctx, `INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $3, 0), ($2, $4, 0)`, draftID, activeID, draftRecipientID, activeRecipientID); failure != nil {
		t.Fatalf("insert cutover recipients: %v", failure)
	}
	runID := uuid.New()
	if _, failure = database.Exec(ctx, `INSERT INTO mailing_runs (mailing_id, id, number, status) VALUES ($1, $2, 1, 'queued')`, activeID, runID); failure != nil {
		t.Fatalf("insert legacy run: %v", failure)
	}
	if _, failure = database.Exec(ctx, `INSERT INTO mailing_deliveries (mailing_id, run_id, recipient_id) VALUES ($1, $2, $3)`, activeID, runID, activeRecipientID); failure != nil {
		t.Fatalf("insert legacy delivery: %v", failure)
	}
	if _, failure = database.Exec(ctx, `INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id) VALUES ($1, $2, $3, nextval('mailing_delivery_random_id_seq'))`, activeID, runID, activeRecipientID); failure != nil {
		t.Fatalf("insert legacy Telegram delivery: %v", failure)
	}
	var sequenceBefore int64
	if failure = database.QueryRow(ctx, `SELECT last_value FROM mailing_delivery_random_id_seq`).Scan(&sequenceBefore); failure != nil {
		t.Fatalf("read random-ID sequence before cutover: %v", failure)
	}
	if _, failure = provider.Up(ctx); failure == nil {
		t.Fatal("delivery execution v2 migration succeeded without operator acknowledgement")
	}
	var remainingRuns, remainingDeliveries int
	if failure = database.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM mailing_runs), (SELECT COUNT(*) FROM mailing_deliveries)`).Scan(&remainingRuns, &remainingDeliveries); failure != nil {
		t.Fatalf("read graph after rejected cutover: %v", failure)
	}
	if remainingRuns != 1 || remainingDeliveries != 1 {
		t.Fatalf("rejected cutover deleted legacy graph: runs %d deliveries %d", remainingRuns, remainingDeliveries)
	}
	if _, failure = migrationDatabase.ExecContext(ctx, `INSERT INTO delivery_execution_v2_cutover_ack (acknowledgement_id, acknowledged_by) VALUES (TRUE, current_user)`); failure != nil {
		t.Fatalf("acknowledge delivery execution v2 cutover: %v", failure)
	}
	if _, failure = provider.Up(ctx); failure != nil {
		t.Fatalf("apply acknowledged delivery execution v2 migration: %v", failure)
	}
	var draftStatus, activeStatus string
	if failure = database.QueryRow(ctx, `SELECT (SELECT status::text FROM mailings WHERE id = $1), (SELECT status::text FROM mailings WHERE id = $2)`, draftID, activeID).Scan(&draftStatus, &activeStatus); failure != nil {
		t.Fatalf("read cutover mailing statuses: %v", failure)
	}
	if draftStatus != "draft" || activeStatus != "stopped" {
		t.Fatalf("cutover statuses = %s/%s, want draft/stopped", draftStatus, activeStatus)
	}
	var runCount, deliveryCount, draftRecipientCount int
	if failure = database.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM mailing_runs), (SELECT COUNT(*) FROM mailing_deliveries), (SELECT COUNT(*) FROM mailing_recipients WHERE mailing_id = $1)`, draftID).Scan(&runCount, &deliveryCount, &draftRecipientCount); failure != nil {
		t.Fatalf("read cutover graph: %v", failure)
	}
	if runCount != 0 || deliveryCount != 0 || draftRecipientCount != 1 {
		t.Fatalf("cutover graph = runs %d deliveries %d draft recipients %d", runCount, deliveryCount, draftRecipientCount)
	}
	var sequenceAfter int64
	if failure = database.QueryRow(ctx, `SELECT last_value FROM mailing_delivery_random_id_seq`).Scan(&sequenceAfter); failure != nil {
		t.Fatalf("read random-ID sequence after cutover: %v", failure)
	}
	if sequenceAfter != sequenceBefore {
		t.Fatalf("random-ID sequence changed from %d to %d", sequenceBefore, sequenceAfter)
	}
	var v2RunID uuid.UUID
	var generation int64
	if failure = database.QueryRow(ctx, `INSERT INTO mailing_runs (mailing_id, number, status) VALUES ($1, 1, 'queued') RETURNING id, execution_generation`, draftID).Scan(&v2RunID, &generation); failure != nil {
		t.Fatalf("insert v2 run with default generation: %v", failure)
	}
	if generation != 1 {
		t.Fatalf("new run generation = %d, want 1", generation)
	}
	if _, failure = database.Exec(ctx, `INSERT INTO mailing_deliveries (mailing_id, run_id, recipient_id) VALUES ($1, $2, $3)`, draftID, v2RunID, draftRecipientID); failure != nil {
		t.Fatalf("insert v2 constraint fixture delivery: %v", failure)
	}
	if _, failure = database.Exec(ctx, `INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id) VALUES ($1, $2, $3, nextval('mailing_delivery_random_id_seq'))`, draftID, v2RunID, draftRecipientID); failure != nil {
		t.Fatalf("insert v2 Telegram constraint fixture delivery: %v", failure)
	}
	_, failure = database.Exec(ctx, `UPDATE telegram_mailing_deliveries SET random_id = random_id + 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID)
	assertConstraintViolation(t, failure, "ck_telegram_mailing_deliveries_random_id_immutable")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = NULL WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New())
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_lease")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID)
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_sending_lease")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sent', sent_at = CURRENT_TIMESTAMP, lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New())
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_terminal_lease")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'unknown' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID)
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_unknown_evidence")
	if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'unknown', attempt_count = 1, started_at = CURRENT_TIMESTAMP, error_message = 'expired sending lease' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID); failure != nil {
		t.Fatalf("insert valid unknown delivery: %v", failure)
	}
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET error_message = '   ' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID)
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_unknown_evidence")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 0, started_at = CURRENT_TIMESTAMP, lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New())
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_sending_evidence")
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = NULL, lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New())
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_sending_evidence")
	if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'pending', attempt_count = 0, started_at = NULL, error_message = NULL, lease_token = NULL, lease_until = NULL, lease_execution_generation = NULL WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID); failure != nil {
		t.Fatalf("persist valid pending delivery: %v", failure)
	}
	if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = CURRENT_TIMESTAMP, lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New()); failure != nil {
		t.Fatalf("persist valid sending delivery: %v", failure)
	}
	if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'unknown', error_message = 'expired sending lease', lease_token = NULL, lease_until = NULL, lease_execution_generation = NULL WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID); failure != nil {
		t.Fatalf("persist valid unknown delivery: %v", failure)
	}
	_, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET lease_token = $4, lease_until = CURRENT_TIMESTAMP + INTERVAL '1 minute', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID, uuid.New())
	assertConstraintViolation(t, failure, "ck_mailing_deliveries_terminal_lease")
	var constraintStatus string
	if failure = database.QueryRow(ctx, `SELECT status::text FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, draftID, v2RunID, draftRecipientID).Scan(&constraintStatus); failure != nil {
		t.Fatalf("read v2 constraint fixture delivery: %v", failure)
	}
	if constraintStatus != "unknown" {
		t.Fatalf("constraint fixture status after rejected updates = %q, want unknown", constraintStatus)
	}
	if _, failure = provider.Down(ctx); failure == nil {
		t.Fatal("v2 migration Down returned nil, want irreversible migration failure")
	}
	migrationDatabase.Close()
}

func TestDeliveryLifecycleV2PostgreSQL(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database, _, failure := newIsolatedDeliveryPipelineDatabase(ctx, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	operatorID, accountID, failure := createLifecycleAccount(ctx, database)
	if failure != nil {
		t.Fatalf("create lifecycle account: %v", failure)
	}
	t.Cleanup(func() {
		if _, cleanupFailure := database.Exec(context.Background(), `DELETE FROM mailings WHERE operator_id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup lifecycle mailings: %v", cleanupFailure)
			return
		}
		if _, cleanupFailure := database.Exec(context.Background(), `DELETE FROM operators WHERE id = $1`, operatorID); cleanupFailure != nil {
			t.Errorf("cleanup lifecycle operator: %v", cleanupFailure)
		}
	})

	claims := pgclaims.NewClaims(database, time.Minute)
	deliveries := pgdeliveries.NewDeliveries(database)
	mailings := pgmailings.NewMailings(database)
	assertTerminalUnknown := func(t *testing.T, item pipelineDelivery) {
		t.Helper()
		state := readLifecycleDeliveryState(t, ctx, database, item)
		if state.status != "unknown" || state.attempts < 1 || !state.started.Valid || state.errorMessage == "" || state.leaseToken != uuid.Nil || state.leaseUntil.Valid || state.leaseGeneration.Valid {
			t.Fatalf("terminal unknown delivery state = %+v", state)
		}
		if _, failure := claims.Claim(ctx); !errors.Is(failure, applicationdelivery.ErrEmpty) {
			t.Fatalf("claim after terminal unknown delivery = %v, want ErrEmpty", failure)
		}
	}
	finalizeNegativeAfterAdmission := func(t *testing.T, item pipelineDelivery, sendFailure error, invalidate func() error) {
		t.Helper()
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim delivery for negative finalization: %v", claimFailure)
		}
		port := newUncancellablePort(sendFailure)
		dispatchDone := make(chan error, 1)
		go func() {
			dispatchDone <- task.Execute(ctx, applicationdelivery.New(deliveries, port))
		}()
		select {
		case <-port.entered:
		case <-ctx.Done():
			t.Fatalf("wait for admitted negative send: %v", ctx.Err())
		}
		if failure := invalidate(); failure != nil {
			close(port.release)
			t.Fatalf("invalidate delivery parent before negative finalization: %v", failure)
		}
		close(port.release)
		select {
		case failure := <-dispatchDone:
			if failure != nil {
				t.Fatalf("finalize negative delivery: %v", failure)
			}
		case <-ctx.Done():
			t.Fatalf("wait for negative finalization: %v", ctx.Err())
		}
		assertTerminalUnknown(t, item)
	}

	t.Run("claim matrix and lease semantics", func(t *testing.T) {
		for _, test := range []struct {
			name          string
			mailingStatus string
			runStatus     string
			withRoute     bool
			attempts      int
			readyStatus   string
			wantClaim     bool
		}{
			{name: "queued queued", mailingStatus: "queued", runStatus: "queued", withRoute: true, wantClaim: true},
			{name: "running running", mailingStatus: "running", runStatus: "running", withRoute: true, wantClaim: true},
			{name: "queued running mismatch", mailingStatus: "queued", runStatus: "running", withRoute: true},
			{name: "running queued mismatch", mailingStatus: "running", runStatus: "queued", withRoute: true},
			{name: "stopped parent", mailingStatus: "stopped", runStatus: "queued", withRoute: true},
			{name: "cancelled run", mailingStatus: "queued", runStatus: "cancelled", withRoute: true},
			{name: "missing route", mailingStatus: "queued", runStatus: "queued", withRoute: false},
			{name: "exhausted attempts", mailingStatus: "queued", runStatus: "queued", withRoute: true, attempts: 5},
			{name: "expired sending is not reclaimed", mailingStatus: "queued", runStatus: "queued", withRoute: true, readyStatus: "sending"},
		} {
			t.Run(test.name, func(t *testing.T) {
				item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
				if !test.withRoute {
					if _, failure := database.Exec(ctx, `DELETE FROM telegram_mailing_routes WHERE mailing_id = $1`, item.mailingID); failure != nil {
						t.Fatalf("delete route: %v", failure)
					}
				}
				if _, failure := database.Exec(ctx, `UPDATE mailings SET status = $2::mailing_status_type WHERE id = $1`, item.mailingID, test.mailingStatus); failure != nil {
					t.Fatalf("set mailing status: %v", failure)
				}
				if _, failure := database.Exec(ctx, `UPDATE mailing_runs SET status = $2::mailing_run_status_type, started_at = CASE WHEN $2 = 'running' THEN CURRENT_TIMESTAMP ELSE started_at END, finished_at = CASE WHEN $2 = 'cancelled' THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE id = $1`, item.runID, test.runStatus); failure != nil {
					t.Fatalf("set run status: %v", failure)
				}
				if test.attempts != 0 {
					if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET attempt_count = max_attempts WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
						t.Fatalf("exhaust attempts: %v", failure)
					}
				}
				if test.readyStatus == "sending" {
					if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', started_at = CURRENT_TIMESTAMP, attempt_count = 1, lease_token = $4, lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second', lease_execution_generation = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, uuid.New()); failure != nil {
						t.Fatalf("create expired sending row: %v", failure)
					}
				}

				task, claimFailure := claims.Claim(ctx)
				if test.wantClaim {
					if claimFailure != nil {
						t.Fatalf("claim delivery: %v", claimFailure)
					}
					before := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
					if before.status != "pending" || before.attempts != 0 || before.started.Valid || !before.leaseGeneration.Valid || before.leaseGeneration.Int64 != 1 || before.leaseToken == uuid.Nil || !before.leaseUntil.Valid {
						t.Fatalf("claim changed unexpected state: %+v", before)
					}
					if test.name != "running running" && test.name != "queued queued" {
						t.Fatalf("unexpected positive matrix case %q", test.name)
					}
					return
				}
				if !errors.Is(claimFailure, applicationdelivery.ErrEmpty) {
					t.Fatalf("claim error = %v, want %v", claimFailure, applicationdelivery.ErrEmpty)
				}
				if task != nil {
					t.Fatal("empty claim returned a task")
				}
			})
		}
	})

	t.Run("expired pending lease is reclaimed with a new token", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		_, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("initial claim: %v", claimFailure)
		}
		firstState := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("expire pending lease: %v", failure)
		}
		second, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("reclaim expired pending lease: %v", claimFailure)
		}
		if second == nil {
			t.Fatal("reclaim returned a nil task")
		}
		secondState := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if secondState.status != "pending" || secondState.attempts != 0 || secondState.started.Valid || secondState.leaseToken == uuid.Nil || secondState.leaseToken == firstState.leaseToken || !secondState.leaseUntil.Valid || !secondState.leaseGeneration.Valid || secondState.leaseGeneration.Int64 != 1 {
			t.Fatalf("reclaimed pending state = %+v, first=%+v", secondState, firstState)
		}
	})

	t.Run("subsecond lease duration remains valid", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		shortClaims := pgclaims.NewClaims(database, 250*time.Millisecond)
		if _, claimFailure := shortClaims.Claim(ctx); claimFailure != nil {
			t.Fatalf("claim with subsecond lease: %v", claimFailure)
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if !state.leaseUntil.Valid || state.leaseGeneration.Int64 != 1 {
			t.Fatalf("subsecond lease state = %+v", state)
		}
		var validSeconds float64
		if failure := database.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (lease_until - CURRENT_TIMESTAMP)) FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID).Scan(&validSeconds); failure != nil {
			t.Fatalf("read subsecond lease duration: %v", failure)
		}
		if validSeconds <= 0 || validSeconds >= 1 {
			t.Fatalf("subsecond lease duration = %0.6f seconds, want between zero and one second", validSeconds)
		}
	})

	t.Run("renewal fence matrix", func(t *testing.T) {
		t.Run("valid queued pair", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			before := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
			if failure = task.Renew(ctx, 250*time.Millisecond); failure != nil {
				t.Fatalf("renew delivery lease: %v", failure)
			}
			after := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
			if after.status != "pending" || after.attempts != before.attempts || after.started != before.started || after.leaseToken != before.leaseToken || after.leaseGeneration != before.leaseGeneration {
				t.Fatalf("renewed delivery state = %+v, want only lease expiry changed from %+v", after, before)
			}
			var remainingSeconds float64
			if failure = database.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (lease_until - clock_timestamp())) FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID).Scan(&remainingSeconds); failure != nil {
				t.Fatalf("read renewed lease duration: %v", failure)
			}
			if remainingSeconds <= 0 || remainingSeconds >= 1 {
				t.Fatalf("renewed lease duration = %0.6f seconds, want between zero and one second", remainingSeconds)
			}
		})

		t.Run("valid running pair", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			if _, failure := database.Exec(ctx, `UPDATE mailings SET status = 'running' WHERE id = $1`, item.mailingID); failure != nil {
				t.Fatalf("set mailing running: %v", failure)
			}
			if _, failure := database.Exec(ctx, `UPDATE mailing_runs SET status = 'running', started_at = clock_timestamp() WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID); failure != nil {
				t.Fatalf("set run running: %v", failure)
			}
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim running delivery: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); failure != nil {
				t.Fatalf("renew running delivery lease: %v", failure)
			}
		})

		t.Run("stale token", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET lease_token = $4 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, uuid.New()); failure != nil {
				t.Fatalf("replace lease token: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); !errors.Is(failure, applicationdelivery.ErrLeaseLost) {
				t.Fatalf("renew stale token error = %v, want ErrLeaseLost", failure)
			}
		})

		t.Run("expired lease", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
				t.Fatalf("expire lease: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); !errors.Is(failure, applicationdelivery.ErrLeaseLost) {
				t.Fatalf("renew expired lease error = %v, want ErrLeaseLost", failure)
			}
		})

		t.Run("stopped parent", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			if failure = pgmailings.NewMailings(database).Mailing(mailing.Identity(item.mailingID)).Stop(ctx); failure != nil {
				t.Fatalf("stop mailing: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); !errors.Is(failure, applicationdelivery.ErrLeaseLost) {
				t.Fatalf("renew stopped delivery error = %v, want ErrLeaseLost", failure)
			}
		})

		t.Run("generation mismatch", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			if _, failure = database.Exec(ctx, `UPDATE mailing_runs SET execution_generation = execution_generation + 1 WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID); failure != nil {
				t.Fatalf("advance execution generation: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); !errors.Is(failure, applicationdelivery.ErrLeaseLost) {
				t.Fatalf("renew generation-mismatched delivery error = %v, want ErrLeaseLost", failure)
			}
		})

		t.Run("sending delivery", func(t *testing.T) {
			item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
			task, failure := claims.Claim(ctx)
			if failure != nil {
				t.Fatalf("claim delivery: %v", failure)
			}
			if _, failure = database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', started_at = clock_timestamp(), attempt_count = 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
				t.Fatalf("make delivery sending: %v", failure)
			}
			if failure = task.Renew(ctx, 250*time.Millisecond); !errors.Is(failure, applicationdelivery.ErrLeaseLost) {
				t.Fatalf("renew sending delivery error = %v, want ErrLeaseLost", failure)
			}
		})
	})

	t.Run("renewal waits behind the mailing lock", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, failure := claims.Claim(ctx)
		if failure != nil {
			t.Fatalf("claim delivery: %v", failure)
		}
		lockContext, cancelLock := context.WithTimeout(ctx, time.Second)
		defer cancelLock()
		transaction, failure := database.Begin(lockContext)
		if failure != nil {
			t.Fatalf("begin lock transaction: %v", failure)
		}
		t.Cleanup(func() { _ = transaction.Rollback(context.Background()) })
		var locked int
		if failure = transaction.QueryRow(lockContext, `SELECT 1 FROM mailings WHERE id = $1 FOR UPDATE`, item.mailingID).Scan(&locked); failure != nil {
			t.Fatalf("lock mailing: %v", failure)
		}

		renewContext, cancelRenew := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancelRenew()
		renewalDone := make(chan error, 1)
		go func() { renewalDone <- task.Renew(renewContext, 250*time.Millisecond) }()
		select {
		case failure = <-renewalDone:
			if !errors.Is(failure, context.DeadlineExceeded) {
				t.Fatalf("renewal while mailing is locked = %v, want context deadline", failure)
			}
		case <-ctx.Done():
			t.Fatalf("wait for locked renewal: %v", ctx.Err())
		}
	})

	t.Run("final fence rejects without calling transport", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET ready_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("ready delivery: %v", failure)
		}
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim delivery: %v", claimFailure)
		}
		if _, failure := database.Exec(ctx, `UPDATE mailings SET status = 'stopped' WHERE id = $1`, item.mailingID); failure != nil {
			t.Fatalf("invalidate mailing parent: %v", failure)
		}
		fake := faketelegram.New(faketelegram.WithCallRecording(2))
		command := applicationdelivery.New(deliveries, fake)
		if failure = task.Execute(ctx, command); failure == nil {
			t.Fatal("final fence returned nil for stopped parent")
		}
		if calls := fake.Calls(); len(calls) != 0 {
			t.Fatalf("transport call count = %d, want 0", len(calls))
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "pending" || state.attempts != 0 {
			t.Fatalf("rejected delivery state = %+v, want pending with zero attempts", state)
		}
	})

	t.Run("stale token rejects without calling transport", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim delivery: %v", claimFailure)
		}
		if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET lease_token = $4 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID, uuid.New()); failure != nil {
			t.Fatalf("replace lease token: %v", failure)
		}
		fake := faketelegram.New(faketelegram.WithCallRecording(2))
		if failure = task.Execute(ctx, applicationdelivery.New(deliveries, fake)); failure == nil {
			t.Fatal("stale token execution returned nil")
		}
		if calls := fake.Calls(); len(calls) != 0 {
			t.Fatalf("transport call count = %d, want 0", len(calls))
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "pending" || state.attempts != 0 {
			t.Fatalf("stale-token delivery state = %+v", state)
		}
	})

	t.Run("release only clears pending lease", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim pending delivery: %v", claimFailure)
		}
		if failure := task.Release(ctx, errors.New("not admitted")); failure != nil {
			t.Fatalf("release pending delivery: %v", failure)
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "pending" || state.leaseToken != uuid.Nil || state.leaseUntil.Valid || state.leaseGeneration.Valid {
			t.Fatalf("released pending state = %+v", state)
		}

		if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET ready_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("ready released delivery: %v", failure)
		}
		task, claimFailure = claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("reclaim released delivery: %v", claimFailure)
		}
		claimedState := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if _, failure := database.Exec(ctx, `UPDATE mailing_deliveries SET status = 'sending', attempt_count = 1, started_at = CURRENT_TIMESTAMP WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID); failure != nil {
			t.Fatalf("make delivery sending: %v", failure)
		}
		if failure := task.Release(ctx, errors.New("must not demote")); failure != nil {
			t.Fatalf("release sending delivery: %v", failure)
		}
		state = readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "sending" || state.leaseToken != claimedState.leaseToken {
			t.Fatalf("release demoted sending delivery: before=%+v after=%+v", claimedState, state)
		}
	})

	t.Run("unknown error finalizes as terminal unknown outcome", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		fake := faketelegram.New(faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomeUnknown}), faketelegram.WithCallRecording(2))
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim unknown-error delivery: %v", claimFailure)
		}
		if failure := task.Execute(ctx, applicationdelivery.New(deliveries, fake)); failure != nil {
			t.Fatalf("finalize unknown outcome: %v", failure)
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "unknown" || state.attempts != 1 || state.errorMessage == "" || state.leaseToken != uuid.Nil {
			t.Fatalf("unknown outcome state = %+v", state)
		}
	})

	t.Run("successful outcome finalizes after execution context cancellation", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, failure := claims.Claim(ctx)
		if failure != nil {
			t.Fatalf("claim delivery: %v", failure)
		}
		port := newUncancellablePort(nil)
		executionContext, cancelExecution := context.WithCancel(ctx)
		dispatchDone := make(chan error, 1)
		go func() {
			dispatchDone <- task.Execute(executionContext, applicationdelivery.New(deliveries, port))
		}()
		select {
		case <-port.entered:
		case <-ctx.Done():
			t.Fatalf("wait for admitted send: %v", ctx.Err())
		}
		cancelExecution()
		close(port.release)
		select {
		case failure = <-dispatchDone:
			if failure != nil {
				t.Fatalf("finalize successful delivery after cancellation: %v", failure)
			}
		case <-ctx.Done():
			t.Fatalf("wait for canceled successful delivery: %v", ctx.Err())
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "sent" || state.leaseToken != uuid.Nil || state.leaseUntil.Valid {
			t.Fatalf("successful delivery after context cancellation = %+v", state)
		}
	})

	t.Run("quarantine finalizes after execution context cancellation", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, failure := claims.Claim(ctx)
		if failure != nil {
			t.Fatalf("claim delivery: %v", failure)
		}
		sendFailure := fmt.Errorf("transport canceled after admission: %w", context.Canceled)
		port := newUncancellablePort(sendFailure)
		executionContext, cancelExecution := context.WithCancel(ctx)
		dispatchDone := make(chan error, 1)
		go func() {
			dispatchDone <- task.Execute(executionContext, applicationdelivery.New(deliveries, port))
		}()
		select {
		case <-port.entered:
		case <-ctx.Done():
			t.Fatalf("wait for admitted send: %v", ctx.Err())
		}
		cancelExecution()
		close(port.release)
		select {
		case failure = <-dispatchDone:
			if failure != nil {
				t.Fatalf("quarantine delivery after cancellation: %v", failure)
			}
		case <-ctx.Done():
			t.Fatalf("wait for canceled quarantined delivery: %v", ctx.Err())
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "sending" || state.errorMessage == "" || state.leaseToken == uuid.Nil {
			t.Fatalf("quarantined delivery after context cancellation = %+v", state)
		}
	})

	t.Run("outcome finalization timeout preserves sending and claim exclusion", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, failure := claims.Claim(ctx)
		if failure != nil {
			t.Fatalf("claim delivery: %v", failure)
		}
		port := newUncancellablePort(nil)
		dispatchDone := make(chan error, 1)
		go func() {
			dispatchDone <- task.Execute(ctx, applicationdelivery.New(deliveries, port))
		}()
		select {
		case <-port.entered:
		case <-ctx.Done():
			t.Fatalf("wait for admitted send: %v", ctx.Err())
		}

		lockContext, cancelLock := context.WithTimeout(ctx, time.Second)
		transaction, failure := database.Begin(lockContext)
		cancelLock()
		if failure != nil {
			t.Fatalf("begin finalization lock transaction: %v", failure)
		}
		t.Cleanup(func() { _ = transaction.Rollback(context.Background()) })
		var locked int
		if failure = transaction.QueryRow(ctx, `SELECT 1 FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3 FOR UPDATE`, item.mailingID, item.runID, item.recipientID).Scan(&locked); failure != nil {
			t.Fatalf("lock delivery for finalization timeout: %v", failure)
		}
		close(port.release)

		select {
		case failure = <-dispatchDone:
			if !errors.Is(failure, applicationdelivery.ErrOutcomeFinalization) {
				t.Fatalf("finalization timeout error = %v, want ErrOutcomeFinalization", failure)
			}
		case <-ctx.Done():
			t.Fatalf("wait for finalization timeout: %v", ctx.Err())
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "sending" || state.leaseToken == uuid.Nil {
			t.Fatalf("delivery after finalization timeout = %+v, want sending with lease", state)
		}
		if _, failure = claims.Claim(ctx); !errors.Is(failure, applicationdelivery.ErrEmpty) {
			t.Fatalf("claim after finalization timeout error = %v, want ErrEmpty", failure)
		}
	})

	t.Run("unknown outcome has no claimable lease", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim unknown delivery: %v", claimFailure)
		}
		fake := faketelegram.New(faketelegram.WithDefault(faketelegram.Step{Outcome: faketelegram.OutcomeUnknown}))
		if failure := task.Execute(ctx, applicationdelivery.New(deliveries, fake)); failure != nil {
			t.Fatalf("finalize unknown delivery: %v", failure)
		}
		state := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if state.status != "unknown" || state.leaseToken != uuid.Nil || state.leaseUntil.Valid {
			t.Fatalf("unknown outcome state = %+v", state)
		}
	})

	t.Run("stop skips pending, cancels once, and permits sending success", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID, 2)
		task, claimFailure := claims.Claim(ctx)
		if claimFailure != nil {
			t.Fatalf("claim delivery for stop race: %v", claimFailure)
		}
		port := newBlockingPort()
		dispatchDone := make(chan error, 1)
		go func() {
			dispatchDone <- task.Execute(ctx, applicationdelivery.New(deliveries, port))
		}()
		select {
		case <-port.entered:
		case <-ctx.Done():
			t.Fatal("waiting for admitted send: context deadline exceeded")
		}

		mailing := mailings.Mailing(mailing.Identity(item.mailingID))
		if failure := mailing.Stop(ctx); failure != nil {
			t.Fatalf("stop mailing with active send: %v", failure)
		}
		var mailingStatus, runStatus string
		var generation int64
		if failure := database.QueryRow(ctx, `SELECT mailing.status::text, run.status::text, run.execution_generation FROM mailings AS mailing JOIN mailing_runs AS run ON run.mailing_id = mailing.id WHERE mailing.id = $1 AND run.id = $2`, item.mailingID, item.runID).Scan(&mailingStatus, &runStatus, &generation); failure != nil {
			t.Fatalf("read stopped lifecycle: %v", failure)
		}
		if mailingStatus != "stopped" || runStatus != "cancelled" || generation != 2 {
			t.Fatalf("stopped lifecycle = %s/%s/generation %d", mailingStatus, runStatus, generation)
		}
		pendingState := readLifecycleDeliveryState(t, ctx, database, pipelineDelivery{mailingID: item.mailingID, runID: item.runID, recipientID: item.otherRecipientID})
		if pendingState.status != "skipped" || pendingState.leaseToken != uuid.Nil {
			t.Fatalf("pending delivery after stop = %+v", pendingState)
		}
		sendingState := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if sendingState.status != "sending" || sendingState.attempts != 1 {
			t.Fatalf("sending delivery after stop = %+v", sendingState)
		}

		close(port.release)
		select {
		case failure := <-dispatchDone:
			if failure != nil {
				t.Fatalf("persist success after stop: %v", failure)
			}
		case <-ctx.Done():
			t.Fatal("waiting for send completion: context deadline exceeded")
		}
		finalState := readLifecycleDeliveryState(t, ctx, database, item.pipelineDelivery)
		if finalState.status != "sent" || finalState.leaseToken != uuid.Nil {
			t.Fatalf("successful delivery after stop = %+v", finalState)
		}
		if failure := mailing.Stop(ctx); failure != nil {
			t.Fatalf("repeat stop: %v", failure)
		}
		if failure := database.QueryRow(ctx, `SELECT execution_generation FROM mailing_runs WHERE id = $1`, item.runID).Scan(&generation); failure != nil {
			t.Fatalf("read repeated-stop generation: %v", failure)
		}
		if generation != 2 {
			t.Fatalf("repeated stop generation = %d, want 2", generation)
		}
	})

	t.Run("stop after admission then transient finalizes unknown", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		finalizeNegativeAfterAdmission(t, item.pipelineDelivery, applicationdelivery.ErrTransient, func() error {
			return mailings.Mailing(mailing.Identity(item.mailingID)).Stop(ctx)
		})
	})

	t.Run("stop after admission then FloodWait finalizes unknown", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		finalizeNegativeAfterAdmission(t, item.pipelineDelivery, &applicationdelivery.FloodWaitError{Duration: time.Minute}, func() error {
			return mailings.Mailing(mailing.Identity(item.mailingID)).Stop(ctx)
		})
	})

	t.Run("generation mismatch finalizes retryable negative as unknown", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		finalizeNegativeAfterAdmission(t, item.pipelineDelivery, applicationdelivery.ErrTransient, func() error {
			_, failure := database.Exec(ctx, `UPDATE mailing_runs SET execution_generation = execution_generation + 1 WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID)
			return failure
		})
	})

	t.Run("parent status mismatch finalizes retryable negative as unknown", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		finalizeNegativeAfterAdmission(t, item.pipelineDelivery, &applicationdelivery.FloodWaitError{Duration: time.Minute}, func() error {
			_, failure := database.Exec(ctx, `UPDATE mailings SET status = 'running' WHERE id = $1`, item.mailingID)
			return failure
		})
	})

	t.Run("negative finalization waits for uncommitted parent invalidation", func(t *testing.T) {
		for _, test := range []struct {
			name       string
			invalidate func(transaction pgx.Tx, item lifecycleDelivery) error
		}{
			{
				name: "stop cancellation",
				invalidate: func(transaction pgx.Tx, item lifecycleDelivery) error {
					if _, failure := transaction.Exec(ctx, `UPDATE mailing_runs
						SET status = 'cancelled',
						    execution_generation = execution_generation + 1,
						    finished_at = CURRENT_TIMESTAMP
						WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID); failure != nil {
						return failure
					}
					_, failure := transaction.Exec(ctx, `UPDATE mailings SET status = 'stopped' WHERE id = $1`, item.mailingID)
					return failure
				},
			},
			{
				name: "generation invalidation",
				invalidate: func(transaction pgx.Tx, item lifecycleDelivery) error {
					_, failure := transaction.Exec(ctx, `UPDATE mailing_runs
						SET execution_generation = execution_generation + 1
						WHERE mailing_id = $1 AND id = $2`, item.mailingID, item.runID)
					return failure
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
				task, claimFailure := claims.Claim(ctx)
				if claimFailure != nil {
					t.Fatalf("claim delivery for finalization race: %v", claimFailure)
				}
				port := newUncancellablePort(applicationdelivery.ErrTransient)
				dispatchDone := make(chan error, 1)
				go func() {
					dispatchDone <- task.Execute(ctx, applicationdelivery.New(deliveries, port))
				}()
				select {
				case <-port.entered:
				case <-ctx.Done():
					t.Fatalf("wait for admitted negative send: %v", ctx.Err())
				}

				transaction, failure := database.Begin(ctx)
				if failure != nil {
					t.Fatalf("begin parent invalidation transaction: %v", failure)
				}
				defer func() { _ = transaction.Rollback(context.Background()) }()
				var mailingStatus string
				if failure = transaction.QueryRow(ctx, `SELECT status::text FROM mailings WHERE id = $1 FOR UPDATE`, item.mailingID).Scan(&mailingStatus); failure != nil {
					t.Fatalf("lock mailing for parent invalidation: %v", failure)
				}
				var runStatus string
				var generation int64
				if failure = transaction.QueryRow(ctx, `SELECT status::text, execution_generation FROM mailing_runs WHERE mailing_id = $1 AND id = $2 FOR UPDATE`, item.mailingID, item.runID).Scan(&runStatus, &generation); failure != nil {
					t.Fatalf("lock run for parent invalidation: %v", failure)
				}
				if failure = test.invalidate(transaction, item); failure != nil {
					t.Fatalf("invalidate parent in uncommitted transaction: %v", failure)
				}

				close(port.release)
				select {
				case failure = <-dispatchDone:
					t.Fatalf("negative finalization completed before parent invalidation committed: %v", failure)
				case <-time.After(250 * time.Millisecond):
				}
				if failure = transaction.Commit(ctx); failure != nil {
					t.Fatalf("commit parent invalidation transaction: %v", failure)
				}
				select {
				case failure = <-dispatchDone:
					if failure != nil {
						t.Fatalf("finalize negative delivery after parent invalidation: %v", failure)
					}
				case <-ctx.Done():
					t.Fatalf("wait for negative finalization: %v", ctx.Err())
				}
				assertTerminalUnknown(t, item.pipelineDelivery)
			})
		}
	})

	t.Run("concurrent stops advance generation once", func(t *testing.T) {
		item := createLifecycleDelivery(t, ctx, database, operatorID, accountID)
		start := make(chan struct{})
		results := make(chan error, 2)
		mailing := mailings.Mailing(mailing.Identity(item.mailingID))
		for range 2 {
			go func() {
				<-start
				results <- mailing.Stop(ctx)
			}()
		}
		close(start)
		for range 2 {
			if failure := <-results; failure != nil {
				t.Fatalf("concurrent stop: %v", failure)
			}
		}
		var status string
		var generation int64
		if failure := database.QueryRow(ctx, `SELECT mailing.status::text, run.execution_generation FROM mailings AS mailing JOIN mailing_runs AS run ON run.mailing_id = mailing.id WHERE mailing.id = $1 AND run.id = $2`, item.mailingID, item.runID).Scan(&status, &generation); failure != nil {
			t.Fatalf("read concurrent stop state: %v", failure)
		}
		if status != "stopped" || generation != 2 {
			t.Fatalf("concurrent stop state = %s/generation %d", status, generation)
		}
	})
}

func newPreV2MigrationDatabase(
	ctx context.Context,
	t *testing.T,
	databaseURL string,
) (*sql.DB, *pgxpool.Pool, string, *goose.Provider, error) {
	t.Helper()
	baseConfig, failure := pgxpool.ParseConfig(databaseURL)
	if failure != nil {
		return nil, nil, "", nil, fmt.Errorf("parse PostgreSQL URL: %w", failure)
	}
	adminDatabase, failure := pgxpool.NewWithConfig(ctx, baseConfig)
	if failure != nil {
		return nil, nil, "", nil, fmt.Errorf("open PostgreSQL admin pool: %w", failure)
	}
	if failure = adminDatabase.Ping(ctx); failure != nil {
		adminDatabase.Close()
		return nil, nil, "", nil, fmt.Errorf("ping PostgreSQL admin pool: %w", failure)
	}
	schema := "delivery_v2_cutover_" + uuid.NewString()
	quotedSchema := `"` + schema + `"`
	if _, failure = adminDatabase.Exec(ctx, "CREATE SCHEMA "+quotedSchema); failure != nil {
		adminDatabase.Close()
		return nil, nil, "", nil, fmt.Errorf("create cutover schema: %w", failure)
	}

	var migrationDatabase *sql.DB
	var database *pgxpool.Pool
	t.Cleanup(func() {
		if database != nil {
			database.Close()
		}
		if migrationDatabase != nil {
			migrationDatabase.Close()
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := adminDatabase.Exec(cleanupContext, "DROP SCHEMA "+quotedSchema+" CASCADE"); cleanupFailure != nil {
			t.Errorf("drop cutover schema %q: %v", schema, cleanupFailure)
		}
		adminDatabase.Close()
	})

	isolatedURL, failure := deliveryPipelineDatabaseURL(databaseURL, schema)
	if failure != nil {
		return nil, nil, "", nil, failure
	}
	migrationDatabase, failure = sql.Open("pgx", isolatedURL)
	if failure != nil {
		return nil, nil, "", nil, fmt.Errorf("open migration database: %w", failure)
	}
	if failure = migrationDatabase.PingContext(ctx); failure != nil {
		return nil, nil, "", nil, fmt.Errorf("ping migration database: %w", failure)
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, nil, "", nil, errors.New("locate cutover integration test")
	}
	provider, failure := goose.NewProvider(
		goose.DialectPostgres,
		migrationDatabase,
		os.DirFS(filepath.Join(filepath.Dir(filename), "../../../../migrations")),
		goose.WithAllowOutofOrder(true),
	)
	if failure != nil {
		return nil, nil, "", nil, fmt.Errorf("create migration provider: %w", failure)
	}
	if _, failure = provider.UpTo(ctx, preDeliveryExecutionV2Migration); failure != nil {
		return nil, nil, "", nil, fmt.Errorf("apply pre-v2 migrations: %w", failure)
	}
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
	database, failure = pgxpool.NewWithConfig(ctx, isolatedConfig)
	if failure != nil {
		return nil, nil, "", nil, fmt.Errorf("open cutover pool: %w", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		return nil, nil, "", nil, fmt.Errorf("ping cutover pool: %w", failure)
	}
	return migrationDatabase, database, schema, provider, nil
}

type lifecycleDelivery struct {
	pipelineDelivery
	otherRecipientID uuid.UUID
}

func createLifecycleAccount(context context.Context, database *pgxpool.Pool) (uuid.UUID, uuid.UUID, error) {
	operatorID := uuid.New()
	accountID := uuid.New()
	if _, failure := database.Exec(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "lifecycle-"+operatorID.String()); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	if _, failure := database.Exec(context, `INSERT INTO operator_accounts (id, operator_id, phone, telegram_username, telegram_first_name, api_id) VALUES ($1, $2, $3, $4, $5, 1)`, accountID, operatorID, "+19990000009", "lifecycle_"+accountID.String()[:8], "Lifecycle Test"); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	return operatorID, accountID, nil
}

func createPreV2LifecycleAccount(context context.Context, database *pgxpool.Pool) (uuid.UUID, uuid.UUID, error) {
	operatorID := uuid.New()
	accountID := uuid.New()
	// This fixture targets the schema before 20260729000300_secure_operator_credentials,
	// where operators.password is required. Post-cutover fixtures stay password-free.
	if _, failure := database.Exec(context, `INSERT INTO operators (id, username, password) VALUES ($1, $2, $3)`, operatorID, "lifecycle-"+operatorID.String(), "test-password"); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	if _, failure := database.Exec(context, `INSERT INTO operator_accounts (id, operator_id, phone, telegram_username, telegram_first_name, api_id) VALUES ($1, $2, $3, $4, $5, 1)`, accountID, operatorID, "+19990000009", "lifecycle_"+accountID.String()[:8], "Lifecycle Test"); failure != nil {
		return uuid.Nil, uuid.Nil, failure
	}
	return operatorID, accountID, nil
}

func createLifecycleDelivery(t *testing.T, context context.Context, database *pgxpool.Pool, operatorID, accountID uuid.UUID, recipientCount ...int) lifecycleDelivery {
	t.Helper()
	count := 1
	if len(recipientCount) != 0 {
		count = recipientCount[0]
	}
	mailingID := uuid.New()
	if _, failure := database.Exec(context, `INSERT INTO mailings (id, operator_id, name, message_text, status) VALUES ($1, $2, $3, $4, 'draft')`, mailingID, operatorID, "lifecycle-"+mailingID.String(), "lifecycle message"); failure != nil {
		t.Fatalf("insert lifecycle mailing: %v", failure)
	}
	if _, failure := database.Exec(context, `INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $2)`, mailingID, accountID); failure != nil {
		t.Fatalf("insert lifecycle route: %v", failure)
	}
	recipientIDs := make([]uuid.UUID, count)
	for index := range recipientIDs {
		recipientIDs[index] = uuid.New()
		if _, failure := database.Exec(context, `INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, $3)`, mailingID, recipientIDs[index], index); failure != nil {
			t.Fatalf("insert lifecycle recipient: %v", failure)
		}
	}
	if failure := pgmailings.NewMailings(database).Mailing(mailing.Identity(mailingID)).Queue(context); failure != nil {
		t.Fatalf("queue lifecycle mailing: %v", failure)
	}
	// Keep rows from earlier subtests out of the next claim. The lifecycle
	// matrix intentionally leaves leases behind, while Claim correctly
	// reclaims expired pending leases; isolate each fixture without changing
	// that production behavior.
	if _, failure := database.Exec(context, `UPDATE mailing_deliveries AS delivery
		SET ready_at = CURRENT_TIMESTAMP + INTERVAL '1 hour'
		FROM mailings AS mailing
		WHERE delivery.mailing_id = mailing.id
		  AND mailing.operator_id = $1
		  AND delivery.mailing_id <> $2
		  AND delivery.status = 'pending'`, operatorID, mailingID); failure != nil {
		t.Fatalf("hold prior lifecycle pending deliveries: %v", failure)
	}
	if count > 1 {
		if _, failure := database.Exec(context, `UPDATE mailing_deliveries SET ready_at = CURRENT_TIMESTAMP + INTERVAL '1 hour' WHERE mailing_id = $1 AND recipient_id <> $2`, mailingID, recipientIDs[0]); failure != nil {
			t.Fatalf("hold secondary lifecycle deliveries: %v", failure)
		}
	}
	var runID uuid.UUID
	if failure := database.QueryRow(context, `SELECT id FROM mailing_runs WHERE mailing_id = $1`, mailingID).Scan(&runID); failure != nil {
		t.Fatalf("read lifecycle run: %v", failure)
	}
	item := lifecycleDelivery{pipelineDelivery: pipelineDelivery{mailingID: mailingID, runID: runID, recipientID: recipientIDs[0]}}
	if len(recipientIDs) > 1 {
		item.otherRecipientID = recipientIDs[1]
	}
	return item
}

type deliveryLifecycleState struct {
	status          string
	attempts        int
	started         pgtype.Timestamptz
	leaseToken      uuid.UUID
	leaseUntil      pgtype.Timestamptz
	leaseGeneration pgtype.Int8
	errorMessage    string
}

func readLifecycleDeliveryState(t *testing.T, context context.Context, database *pgxpool.Pool, item pipelineDelivery) deliveryLifecycleState {
	t.Helper()
	var state deliveryLifecycleState
	if failure := database.QueryRow(context, `SELECT status::text, attempt_count, started_at, COALESCE(lease_token, '00000000-0000-0000-0000-000000000000'), lease_until, lease_execution_generation, COALESCE(error_message, '') FROM mailing_deliveries WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, item.mailingID, item.runID, item.recipientID).Scan(&state.status, &state.attempts, &state.started, &state.leaseToken, &state.leaseUntil, &state.leaseGeneration, &state.errorMessage); failure != nil {
		t.Fatalf("read lifecycle delivery: %v", failure)
	}
	return state
}

func assertConstraintViolation(t *testing.T, failure error, constraint string) {
	t.Helper()
	if failure == nil {
		t.Fatalf("constraint %q accepted invalid delivery state", constraint)
	}
	var postgresFailure *pgconn.PgError
	if !errors.As(failure, &postgresFailure) {
		t.Fatalf("constraint %q failure = %v, want PostgreSQL constraint violation", constraint, failure)
	}
	if postgresFailure.Code != "23514" || postgresFailure.ConstraintName != constraint {
		t.Fatalf("constraint failure = %s/%q, want 23514/%q", postgresFailure.Code, postgresFailure.ConstraintName, constraint)
	}
}

type blockingPort struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPort() *blockingPort {
	return &blockingPort{entered: make(chan struct{}), release: make(chan struct{})}
}

func (port *blockingPort) Send(context context.Context, _ recipient.Recipient, _ message.Message, _ int64) error {
	port.once.Do(func() { close(port.entered) })
	select {
	case <-port.release:
		return nil
	case <-context.Done():
		return fmt.Errorf("blocking transport: %w", context.Err())
	}
}

type uncancellablePort struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	failure error
}

func newUncancellablePort(failure error) *uncancellablePort {
	return &uncancellablePort{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		failure: failure,
	}
}

func (port *uncancellablePort) Send(context.Context, recipient.Recipient, message.Message, int64) error {
	port.once.Do(func() { close(port.entered) })
	<-port.release
	return port.failure
}
