package dialogsync

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"

	"github.com/notrodans/cresora/internal/application/dialogsync"
)

func TestDialogSyncQueueClaimsCompletesAndUpserts(t *testing.T) {
	ctx, database := newIntegrationDatabase(t)
	operatorID := uuid.New()
	accountID := uuid.New()
	if _, failure := database.Exec(
		ctx,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		operatorID,
		"dialogsync-"+operatorID.String()[:8],
	); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}
	if _, failure := database.Exec(
		ctx,
		`INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id)
		 VALUES ($1, $2, 'active', 1, $3)`,
		accountID, operatorID, int64(1000),
	); failure != nil {
		t.Fatalf("insert active operator account: %v", failure)
	}

	store := New(database)

	created, failure := store.Backfill(ctx)
	if failure != nil {
		t.Fatalf("backfill: %v", failure)
	}
	if created != 1 {
		t.Fatalf("backfill created = %d, want 1", created)
	}

	task, failure := store.Claim(ctx, time.Minute)
	if failure != nil {
		t.Fatalf("claim: %v", failure)
	}
	key := task.Key()
	if key.AccountID != accountID || key.OperatorID != operatorID {
		t.Fatalf("claimed key = %+v, want account %s operator %s", key, accountID, operatorID)
	}

	target, failure := task.Revalidate(ctx)
	if failure != nil {
		t.Fatalf("revalidate: %v", failure)
	}
	if target.AccountID.UUID() != accountID {
		t.Fatalf("revalidated account = %s, want %s", target.AccountID.UUID(), accountID)
	}

	channelHash := int64(11)
	shared := []dialogsync.SharedDialog{{
		PeerID: 1, Kind: dialogsync.SharedBroadcastChannel, Title: "News",
		Username: "news", AccessHash: &channelHash,
	}}
	userHash := int64(7)
	private := []dialogsync.PrivateDialog{{
		PeerType: dialogsync.PeerUser, PeerID: 5, Title: "Alice", Username: "alice", AccessHash: &userHash,
	}}
	if failure = task.Complete(ctx, shared, private); failure != nil {
		t.Fatalf("complete: %v", failure)
	}

	var (
		status string
		title  string
	)
	if failure = database.QueryRow(ctx,
		`SELECT d.title
		   FROM telegram_shared_dialogs AS d
		   JOIN operator_accounts_shared_dialogs AS link ON link.shared_dialog_id = d.id
		  WHERE link.account_id = $1`,
		accountID,
	).Scan(&title); failure != nil || title != "News" {
		t.Fatalf("shared dialog after sync: title=%q err=%v", title, failure)
	}
	if failure = database.QueryRow(ctx,
		`SELECT membership_status::text
		   FROM operator_accounts_private_dialogs
		  WHERE account_id = $1 AND peer_type = 'user' AND telegram_peer_id = 5`,
		accountID,
	).Scan(&status); failure != nil || status != "joined" {
		t.Fatalf("private dialog after sync: status=%q err=%v", status, failure)
	}
	if failure = database.QueryRow(ctx,
		`SELECT status::text FROM account_dialog_syncs WHERE account_id = $1`,
		accountID,
	).Scan(&status); failure != nil || status != "done" {
		t.Fatalf("sync row status = %q err=%v, want done", status, failure)
	}

	if _, failure = store.Claim(ctx, time.Minute); failure != dialogsync.ErrEmpty {
		t.Fatalf("second claim error = %v, want ErrEmpty", failure)
	}
}

func TestDialogSyncRetryExhaustsThenFails(t *testing.T) {
	ctx, database := newIntegrationDatabase(t)
	operatorID := uuid.New()
	accountID := uuid.New()
	if _, failure := database.Exec(ctx,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		operatorID, "dialogsync-retry-"+operatorID.String()[:6],
	); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}
	if _, failure := database.Exec(ctx,
		`INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id)
		 VALUES ($1, $2, 'active', 1, $3)`,
		accountID, operatorID, int64(2000),
	); failure != nil {
		t.Fatalf("insert active operator account: %v", failure)
	}

	store := New(database)
	if _, failure := store.Backfill(ctx); failure != nil {
		t.Fatalf("backfill: %v", failure)
	}

	task, failure := store.Claim(ctx, time.Minute)
	if failure != nil {
		t.Fatalf("claim: %v", failure)
	}
	if failure = task.Retry(ctx, dialogsync.WrapTransient(nil), time.Second); failure != nil {
		t.Fatalf("retry: %v", failure)
	}

	task, failure = store.Claim(ctx, time.Minute)
	if failure != nil {
		t.Fatalf("claim after retry: %v", failure)
	}
	if failure = task.Fail(ctx, dialogsync.WrapPermanent(nil)); failure != nil {
		t.Fatalf("fail: %v", failure)
	}

	var status string
	if failure = database.QueryRow(ctx,
		`SELECT status::text FROM account_dialog_syncs WHERE account_id = $1`,
		accountID,
	).Scan(&status); failure != nil || status != "failed" {
		t.Fatalf("sync row status = %q err=%v, want failed", status, failure)
	}
}

func TestDialogFloodWaitNeverTerminalAtAttemptCap(t *testing.T) {
	ctx, database := newIntegrationDatabase(t)
	operatorID := uuid.New()
	accountID := uuid.New()
	if _, failure := database.Exec(ctx,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		operatorID, "dialogsync-flood-"+operatorID.String()[:6],
	); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}
	if _, failure := database.Exec(ctx,
		`INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id)
		 VALUES ($1, $2, 'active', 1, $3)`,
		accountID, operatorID, int64(3000),
	); failure != nil {
		t.Fatalf("insert active operator account: %v", failure)
	}

	store := New(database)
	if _, failure := store.Backfill(ctx); failure != nil {
		t.Fatalf("backfill: %v", failure)
	}
	if _, failure := database.Exec(ctx,
		`UPDATE account_dialog_syncs SET max_attempts = 1 WHERE account_id = $1`, accountID,
	); failure != nil {
		t.Fatalf("cap attempts: %v", failure)
	}

	task, failure := store.Claim(ctx, time.Minute)
	if failure != nil {
		t.Fatalf("claim: %v", failure)
	}
	flood := &dialogsync.FloodWaitError{Duration: 15 * time.Second}
	// Even though attempt_count == max_attempts, a flood wait must requeue.
	if failure = task.Retry(ctx, flood, flood.RetryAfter()); failure != nil {
		t.Fatalf("retry flood: %v", failure)
	}

	var status string
	var attempts int
	if failure = database.QueryRow(ctx,
		`SELECT status::text, attempt_count FROM account_dialog_syncs WHERE account_id = $1`,
		accountID,
	).Scan(&status, &attempts); failure != nil || status != "pending" {
		t.Fatalf("sync row status = %q err=%v, want pending (not failed)", status, failure)
	}
	if attempts != 0 {
		t.Fatalf("sync row attempts = %d, want 0 (flood must not consume the budget)", attempts)
	}
	// A flood-retried row must remain claimable even at max_attempts.
	if _, failure = store.Claim(ctx, time.Minute); failure != nil {
		t.Fatalf("second claim after flood retry: %v", failure)
	}
}

func newIntegrationDatabase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	admin, failure := pgxpool.New(ctx, databaseURL)
	if failure != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", failure)
	}
	schema := "dialogsync_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, failure = admin.Exec(ctx, "CREATE SCHEMA "+schema); failure != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", failure)
	}
	isolatedURL, failure := isolatedURL(databaseURL, schema)
	if failure != nil {
		admin.Close()
		t.Fatalf("build isolated database URL: %v", failure)
	}

	migrationDatabase, failure := sql.Open("pgx", isolatedURL)
	if failure != nil {
		admin.Close()
		t.Fatalf("open migration database: %v", failure)
	}
	if failure = migrationDatabase.PingContext(ctx); failure != nil {
		migrationDatabase.Close()
		admin.Close()
		t.Fatalf("ping migration database: %v", failure)
	}
	provider, failure := goose.NewProvider(goose.DialectPostgres, migrationDatabase, os.DirFS(migrationsPath(t)))
	if failure != nil {
		migrationDatabase.Close()
		admin.Close()
		t.Fatalf("create migration provider: %v", failure)
	}
	if _, failure = provider.Up(ctx); failure != nil {
		provider.Close()
		migrationDatabase.Close()
		admin.Close()
		t.Fatalf("apply current baseline: %v", failure)
	}
	provider.Close()
	migrationDatabase.Close()

	database, failure := pgxpool.New(ctx, isolatedURL)
	if failure != nil {
		admin.Close()
		t.Fatalf("open isolated PostgreSQL pool: %v", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		database.Close()
		admin.Close()
		t.Fatalf("ping isolated PostgreSQL pool: %v", failure)
	}
	t.Cleanup(func() {
		database.Close()
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := admin.Exec(cleanupContext, "DROP SCHEMA "+schema+" CASCADE"); cleanupFailure != nil {
			t.Errorf("drop isolated schema: %v", cleanupFailure)
		}
		admin.Close()
	})
	return ctx, database
}

func isolatedURL(databaseURL, schema string) (string, error) {
	parsed, failure := url.Parse(databaseURL)
	if failure != nil {
		return "", failure
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", os.ErrInvalid
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	options := query.Get("options")
	if options != "" {
		options += " "
	}
	query.Set("options", options+"-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func migrationsPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test")
	}
	return filepath.Join(filepath.Dir(filename), "../../../../../migrations")
}
