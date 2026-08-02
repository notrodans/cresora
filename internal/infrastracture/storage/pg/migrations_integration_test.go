package pg

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/notrodans/cresora/config"
)

const (
	currentBaselineVersion  int64 = 20260801000000
	phoneIndexRepairVersion int64 = 20260801200000
	remoteLogoutVersion     int64 = 20260802000000
)

func TestCurrentMigrationsPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	applyCurrentMigrations(t, context, databaseURL)

	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()

	var (
		firstAppliedCount int
		firstVersion      sql.NullInt64
	)
	if failure := database.QueryRowContext(context, `SELECT count(*), max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&firstAppliedCount, &firstVersion); failure != nil {
		t.Fatalf("read first applied migration version: %v", failure)
	}
	if firstAppliedCount != 4 || !firstVersion.Valid || firstVersion.Int64 != remoteLogoutVersion {
		t.Fatalf("first applied migration history = count %d version %v, want count 4 version %d", firstAppliedCount, firstVersion, remoteLogoutVersion)
	}

	applyCurrentMigrations(t, context, databaseURL)
	var (
		repeatedAppliedCount int
		repeatedVersion      sql.NullInt64
	)
	if failure := database.QueryRowContext(context, `SELECT count(*), max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&repeatedAppliedCount, &repeatedVersion); failure != nil {
		t.Fatalf("read repeated migration history: %v", failure)
	}
	if repeatedAppliedCount != firstAppliedCount || !repeatedVersion.Valid || repeatedVersion.Int64 != remoteLogoutVersion {
		t.Fatalf("repeated migration history = count %d version %v, want count %d version %d", repeatedAppliedCount, repeatedVersion, firstAppliedCount, remoteLogoutVersion)
	}

	assertOperatorAccountCatalog(t, context, database)
	assertCurrentDeliverySecurityCatalog(t, context, database)

	var apiIDColumns int
	if failure := database.QueryRowContext(context, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'operator_accounts'
		  AND column_name = 'api_id'`).Scan(&apiIDColumns); failure != nil {
		t.Fatalf("inspect operator account columns: %v", failure)
	}
	if apiIDColumns != 0 {
		t.Fatal("operator_accounts contains api_id")
	}
}

func TestOperatorAccountRemoteLogoutMigrationUpgradePreservesDisconnectingAccountPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPathForTest(t)))
	if failure != nil {
		t.Fatalf("create migration provider: %v", failure)
	}
	defer provider.Close()
	if _, failure = provider.UpTo(context, phoneIndexRepairVersion); failure != nil {
		t.Fatalf("apply migrations through phone index repair: %v", failure)
	}

	operatorID := uuid.New()
	if _, failure = database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "upgrade-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert preexisting upgrade operator: %v", failure)
	}
	accountID := uuid.New()
	if _, failure = database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'disconnecting', 1)`, accountID, operatorID); failure != nil {
		t.Fatalf("insert preexisting disconnecting account: %v", failure)
	}

	if _, failure = provider.ApplyVersion(context, remoteLogoutVersion, true); failure != nil {
		t.Fatalf("apply remote logout migration: %v", failure)
	}

	var remoteLogoutRequired bool
	if failure = database.QueryRowContext(context, `SELECT remote_logout_required FROM operator_accounts WHERE id = $1 AND status = 'disconnecting'`, accountID).Scan(&remoteLogoutRequired); failure != nil {
		t.Fatalf("read preexisting account after remote logout migration: %v", failure)
	}
	if remoteLogoutRequired {
		t.Fatal("preexisting disconnecting account remote logout requirement = true, want false")
	}
}

func TestOperatorAccountRemoteLogoutMigrationRollbackAndReapplyPreservesBaselinePostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	applyCurrentMigrations(t, context, databaseURL)

	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPathForTest(t)))
	if failure != nil {
		t.Fatalf("create migration provider: %v", failure)
	}
	defer provider.Close()
	if _, failure = provider.ApplyVersion(context, remoteLogoutVersion, false); failure != nil {
		t.Fatalf("roll back remote logout migration: %v", failure)
	}

	var remoteLogoutColumns int
	if failure = database.QueryRowContext(context, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'operator_accounts'
		  AND column_name = 'remote_logout_required'`).Scan(&remoteLogoutColumns); failure != nil {
		t.Fatalf("inspect remote logout column after rollback: %v", failure)
	}
	if remoteLogoutColumns != 0 {
		t.Fatalf("remote logout column count after rollback = %d, want 0", remoteLogoutColumns)
	}

	var baselineConstraintCount int
	if failure = database.QueryRowContext(context, `
		SELECT count(*)
		FROM pg_constraint
		WHERE conrelid = 'operator_accounts'::regclass
		  AND conname = 'ck_operator_accounts_timestamp_order'`).Scan(&baselineConstraintCount); failure != nil {
		t.Fatalf("inspect baseline account constraint after rollback: %v", failure)
	}
	if baselineConstraintCount != 1 {
		t.Fatalf("baseline account constraint count after rollback = %d, want 1", baselineConstraintCount)
	}

	var phoneIndexCount int
	if failure = database.QueryRowContext(context, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'operator_accounts'
		  AND indexname = 'uq_operator_accounts_operator_phone'`).Scan(&phoneIndexCount); failure != nil {
		t.Fatalf("inspect baseline phone index after rollback: %v", failure)
	}
	if phoneIndexCount != 1 {
		t.Fatalf("baseline phone index count after rollback = %d, want 1", phoneIndexCount)
	}

	if _, failure = provider.ApplyVersion(context, remoteLogoutVersion, true); failure != nil {
		t.Fatalf("reapply remote logout migration: %v", failure)
	}
	assertOperatorAccountCatalog(t, context, database)
}

func TestOperatorAccountPhoneIndexRepairRollbackPreservesBaselinePostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	applyCurrentMigrations(t, context, databaseURL)

	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPathForTest(t)))
	if failure != nil {
		t.Fatalf("create migration provider: %v", failure)
	}
	defer provider.Close()
	if _, failure = provider.ApplyVersion(context, phoneIndexRepairVersion, false); failure != nil {
		t.Fatalf("roll back phone index repair migration: %v", failure)
	}

	var indexCount int
	if failure = database.QueryRowContext(context, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'operator_accounts'
		  AND indexname = 'uq_operator_accounts_operator_phone'`).Scan(&indexCount); failure != nil {
		t.Fatalf("inspect phone index after repair rollback: %v", failure)
	}
	if indexCount != 1 {
		t.Fatalf("phone index count after repair rollback = %d, want 1", indexCount)
	}

	operatorID := uuid.New()
	if _, failure = database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "rollback-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert rollback operator: %v", failure)
	}
	var accountID uuid.UUID
	if failure = database.QueryRowContext(context, `
		INSERT INTO operator_accounts (
			operator_id,
			phone,
			status,
			status_version,
			auth_expires_at
		)
		VALUES ($1, $2, 'authenticating', 2, $3)
		ON CONFLICT (operator_id, phone) WHERE phone IS NOT NULL DO NOTHING
		RETURNING id`, operatorID, "+12025550303", time.Now().Add(time.Hour)).Scan(&accountID); failure != nil {
		t.Fatalf("execute operator account phone conflict insert after repair rollback: %v", failure)
	}
	if accountID == uuid.Nil {
		t.Fatal("operator account phone conflict insert returned a zero ID after repair rollback")
	}
}

func TestOperatorAccountPhoneIndexMigrationRepairPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()

	baselinePath := t.TempDir()
	baseline, failure := os.ReadFile(filepath.Join(migrationsPathForTest(t), "20260801000000_current_schema.sql"))
	if failure != nil {
		t.Fatalf("read current baseline: %v", failure)
	}
	if failure = os.WriteFile(filepath.Join(baselinePath, "20260801000000_current_schema.sql"), baseline, 0o600); failure != nil {
		t.Fatalf("write temporary current baseline: %v", failure)
	}

	baselineDatabase := openMigrationDatabase(t, context, databaseURL)
	defer baselineDatabase.Close()
	baselineProvider, failure := goose.NewProvider(goose.DialectPostgres, baselineDatabase, os.DirFS(baselinePath))
	if failure != nil {
		t.Fatalf("create baseline migration provider: %v", failure)
	}
	defer baselineProvider.Close()
	if _, failure = baselineProvider.Up(context); failure != nil {
		t.Fatalf("apply temporary current baseline: %v", failure)
	}

	if _, failure = database.ExecContext(context, `DROP INDEX IF EXISTS uq_operator_accounts_operator_phone`); failure != nil {
		t.Fatalf("drop baseline phone index: %v", failure)
	}

	applyCurrentMigrations(t, context, databaseURL)

	assertOperatorAccountCatalog(t, context, database)
	operatorID := uuid.New()
	if _, failure = database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "repair-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert repair operator: %v", failure)
	}
	var accountID uuid.UUID
	if failure = database.QueryRowContext(context, `
		INSERT INTO operator_accounts (
			operator_id,
			phone,
			status,
			status_version,
			auth_expires_at
		)
		VALUES ($1, $2, 'authenticating', 2, $3)
		ON CONFLICT (operator_id, phone) WHERE phone IS NOT NULL DO NOTHING
		RETURNING id`, operatorID, "+12025550302", time.Now().Add(time.Hour)).Scan(&accountID); failure != nil {
		t.Fatalf("execute operator account phone conflict insert: %v", failure)
	}
	if accountID == uuid.Nil {
		t.Fatal("operator account phone conflict insert returned a zero ID")
	}

	applyCurrentMigrations(t, context, databaseURL)
	rows, failure := database.QueryContext(context, `SELECT version_id, is_applied FROM goose_db_version ORDER BY id`)
	if failure != nil {
		t.Fatalf("read repaired migration history: %v", failure)
	}
	defer rows.Close()
	wantHistory := []struct {
		version int64
		applied bool
	}{
		{version: 0, applied: true},
		{version: currentBaselineVersion, applied: true},
		{version: phoneIndexRepairVersion, applied: true},
		{version: remoteLogoutVersion, applied: true},
	}
	for index, want := range wantHistory {
		if !rows.Next() {
			t.Fatalf("repaired migration history ended at row %d, want version %d", index, want.version)
		}
		var (
			version int64
			applied bool
		)
		if failure = rows.Scan(&version, &applied); failure != nil {
			t.Fatalf("read repaired migration history row %d: %v", index, failure)
		}
		if version != want.version || applied != want.applied {
			t.Fatalf("repaired migration history row %d = (%d, %t), want (%d, %t)", index, version, applied, want.version, want.applied)
		}
	}
	if rows.Next() {
		t.Fatal("repaired migration history contains an unexpected row")
	}
	if failure = rows.Err(); failure != nil {
		t.Fatalf("iterate repaired migration history: %v", failure)
	}
}

func TestOperatorAccountConstraintsPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	applyCurrentMigrations(t, context, databaseURL)
	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()

	operatorID := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "account-constraints-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert account constraint operator: %v", failure)
	}
	defaultAccount := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'disconnected', 1)`, defaultAccount, operatorID); failure != nil {
		t.Fatalf("insert account with remote logout default: %v", failure)
	}
	var remoteLogoutRequired bool
	if failure := database.QueryRowContext(context, `SELECT remote_logout_required FROM operator_accounts WHERE id = $1`, defaultAccount).Scan(&remoteLogoutRequired); failure != nil {
		t.Fatalf("read remote logout default: %v", failure)
	}
	if remoteLogoutRequired {
		t.Fatal("remote logout default = true, want false")
	}

	for _, test := range []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "zero version",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'disconnected', 0)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "zero identity",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id) VALUES ($1, $2, 'active', 1, $3)`,
			args:  []any{uuid.New(), operatorID, int64(0)},
		},
		{
			name:  "negative identity",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id) VALUES ($1, $2, 'active', 1, $3)`,
			args:  []any{uuid.New(), operatorID, int64(-1)},
		},
		{
			name:  "active without identity",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'active', 1)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "reauthentication without identity",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, failure_code) VALUES ($1, $2, 'reauth_required', 1, 'auth_expired')`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "reauthentication without failure code",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id) VALUES ($1, $2, 'reauth_required', 1, 2001)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "reauthentication with unsupported failure code",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id, failure_code) VALUES ($1, $2, 'reauth_required', 1, 2002, 'unsupported')`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "failure code outside reauthentication",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, failure_code) VALUES ($1, $2, 'disconnected', 1, 'auth_expired')`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "authentication expiry outside authenticating",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, auth_expires_at) VALUES ($1, $2, 'disconnected', 1, CURRENT_TIMESTAMP)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "authenticating without expiry",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'authenticating', 1)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "remote logout required outside disconnecting",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, remote_logout_required) VALUES ($1, $2, 'disconnected', 1, TRUE)`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "invalid non-null phone",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, phone) VALUES ($1, $2, 'disconnected', 1, 'not-a-phone')`,
			args:  []any{uuid.New(), operatorID},
		},
		{
			name:  "timestamp inversion",
			query: `INSERT INTO operator_accounts (id, operator_id, status, status_version, created_at, updated_at) VALUES ($1, $2, 'disconnected', 1, $3, $4)`,
			args: []any{
				uuid.New(),
				operatorID,
				time.Date(2026, time.August, 1, 12, 0, 1, 0, time.UTC),
				time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, failure := database.ExecContext(context, test.query, test.args...); failure == nil {
				t.Fatal("invalid operator account state was accepted")
			}
		})
	}

	validRemoteLogoutAccount := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version, remote_logout_required) VALUES ($1, $2, 'disconnecting', 1, TRUE)`, validRemoteLogoutAccount, operatorID); failure != nil {
		t.Fatalf("insert disconnecting account with remote logout required: %v", failure)
	}
	if failure := database.QueryRowContext(context, `SELECT remote_logout_required FROM operator_accounts WHERE id = $1`, validRemoteLogoutAccount).Scan(&remoteLogoutRequired); failure != nil {
		t.Fatalf("read accepted remote logout requirement: %v", failure)
	}
	if !remoteLogoutRequired {
		t.Fatal("accepted remote logout requirement = false, want true")
	}

	firstIdentity := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id) VALUES ($1, $2, 'active', 1, 3001)`, firstIdentity, operatorID); failure != nil {
		t.Fatalf("insert first identified account: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id) VALUES ($1, $2, 'active', 1, 3001)`, uuid.New(), operatorID); failure == nil {
		t.Fatal("duplicate Telegram identity was accepted")
	}
	phoneAccount := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, phone, status, status_version) VALUES ($1, $2, '+12025550301', 'disconnected', 1)`, phoneAccount, operatorID); failure != nil {
		t.Fatalf("insert first normalized phone account: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, phone, status, status_version) VALUES ($1, $2, '+12025550301', 'disconnected', 1)`, uuid.New(), operatorID); failure == nil {
		t.Fatal("duplicate operator phone was accepted")
	}
	for range 2 {
		if _, failure := database.ExecContext(context, `INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES ($1, $2, 'disconnected', 1)`, uuid.New(), operatorID); failure != nil {
			t.Fatalf("insert null phone account: %v", failure)
		}
	}
}

func TestCurrentDeliveryAndSessionTriggersPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	applyCurrentMigrations(t, context, databaseURL)
	database := openMigrationDatabase(t, context, databaseURL)
	defer database.Close()

	operatorID := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "trigger-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert trigger operator: %v", failure)
	}
	mailingID := uuid.New()
	recipientID := uuid.New()
	runID := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO mailings (id, operator_id, name, message_text) VALUES ($1, $2, 'trigger mailing', 'trigger message')`, mailingID, operatorID); failure != nil {
		t.Fatalf("insert trigger mailing: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, 0)`, mailingID, recipientID); failure != nil {
		t.Fatalf("insert trigger recipient: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO mailing_runs (mailing_id, id, number) VALUES ($1, $2, 1)`, mailingID, runID); failure != nil {
		t.Fatalf("insert trigger run: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO mailing_deliveries (mailing_id, run_id, recipient_id) VALUES ($1, $2, $3)`, mailingID, runID, recipientID); failure != nil {
		t.Fatalf("insert trigger delivery: %v", failure)
	}
	if _, failure := database.ExecContext(context, `INSERT INTO telegram_mailing_deliveries (mailing_id, run_id, recipient_id, random_id) VALUES ($1, $2, $3, 7001)`, mailingID, runID, recipientID); failure != nil {
		t.Fatalf("insert Telegram delivery proof: %v", failure)
	}
	if _, failure := database.ExecContext(context, `UPDATE telegram_mailing_deliveries SET random_id = random_id + 1 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`, mailingID, runID, recipientID); failure == nil || !strings.Contains(strings.ToLower(failure.Error()), "immutable") {
		t.Fatalf("random ID update error = %v, want immutable trigger failure", failure)
	}

	sessionOperatorID := uuid.New()
	if _, failure := database.ExecContext(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, sessionOperatorID, "session-trigger-"+sessionOperatorID.String()[:8]); failure != nil {
		t.Fatalf("insert session trigger operator: %v", failure)
	}
	tokenHash := bytes.Repeat([]byte{7}, 32)
	if _, failure := database.ExecContext(context, `
		INSERT INTO operator_web_sessions (
			operator_id, token_hash, created_at, last_seen_at, idle_expires_at, absolute_expires_at
		) VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
		          CURRENT_TIMESTAMP + interval '1 hour', CURRENT_TIMESTAMP + interval '2 hours')`, sessionOperatorID, tokenHash); failure != nil {
		t.Fatalf("insert web session: %v", failure)
	}
	if _, failure := database.ExecContext(context, `UPDATE operators SET tokens_invalid_before = tokens_invalid_before + interval '1 second' WHERE id = $1`, sessionOperatorID); failure != nil {
		t.Fatalf("advance operator credential boundary: %v", failure)
	}
	var revokedAt sql.NullTime
	if failure := database.QueryRowContext(context, `SELECT revoked_at FROM operator_web_sessions WHERE operator_id = $1`, sessionOperatorID).Scan(&revokedAt); failure != nil {
		t.Fatalf("read revoked web session: %v", failure)
	}
	if !revokedAt.Valid {
		t.Fatal("credential boundary update did not revoke web session")
	}
}

func TestExecuteMigrationsReturnsInvalidUpErrorWithoutSuccessLogPostgres(t *testing.T) {
	context, databaseURL := newMigrationTestDatabase(t)
	migrationsPath := t.TempDir()
	invalidMigration := []byte(`-- +goose Up
CREATE TABLE migration_failure_probe (id uuid NOT NULL);
CREATE TABLE migration_failure_probe (id uuid NOT NULL);
`)
	if failure := os.WriteFile(filepath.Join(migrationsPath, "20260801000000_invalid.sql"), invalidMigration, 0o600); failure != nil {
		t.Fatalf("write invalid migration: %v", failure)
	}

	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	failure := ExecuteMigrations(context, &config.Config{DbUrl: databaseURL}, logger, migrationsPath)
	if failure == nil || !strings.Contains(failure.Error(), "execute migrations: apply migrations") {
		t.Fatalf("invalid migration error = %v, want contextual apply-migrations error", failure)
	}
	if strings.Contains(logOutput.String(), "Migrations applied successfully") {
		t.Fatal("migration success was logged after an invalid Up")
	}
}

func newMigrationTestDatabase(t *testing.T) (context.Context, string) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		t.Fatalf("open PostgreSQL admin database: %v", failure)
	}
	schema := "current_baseline_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if failure = admin.PingContext(ctx); failure != nil {
		admin.Close()
		t.Fatalf("ping PostgreSQL admin database: %v", failure)
	}
	if _, failure = admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); failure != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", failure)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := admin.ExecContext(cleanupContext, "DROP SCHEMA "+quotedSchema+" CASCADE"); cleanupFailure != nil {
			t.Errorf("drop isolated schema %q: %v", schema, cleanupFailure)
		}
		admin.Close()
	})

	isolatedURL, failure := telegramPeerLookupDatabaseURL(databaseURL, schema)
	if failure != nil {
		t.Fatal(failure)
	}
	return ctx, isolatedURL
}

func openMigrationDatabase(t *testing.T, context context.Context, databaseURL string) *sql.DB {
	t.Helper()
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		t.Fatalf("open isolated database: %v", failure)
	}
	if failure = database.PingContext(context); failure != nil {
		database.Close()
		t.Fatalf("ping isolated database: %v", failure)
	}
	return database
}

func applyCurrentMigrations(t *testing.T, context context.Context, databaseURL string) {
	t.Helper()
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		t.Fatalf("open migration database: %v", failure)
	}
	defer database.Close()
	if failure = database.PingContext(context); failure != nil {
		t.Fatalf("ping migration database: %v", failure)
	}
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPathForTest(t)))
	if failure != nil {
		t.Fatalf("create migration provider: %v", failure)
	}
	defer provider.Close()
	if _, failure = provider.Up(context); failure != nil {
		t.Fatalf("apply current migrations: %v", failure)
	}
}

func assertOperatorAccountCatalog(t *testing.T, context context.Context, database *sql.DB) {
	t.Helper()
	for _, constraint := range []string{
		"ck_operator_accounts_timestamp_order",
		"ck_operator_accounts_status_version_positive",
		"ck_operator_accounts_telegram_user_id_positive",
		"ck_operator_accounts_identity_required",
		"ck_operator_accounts_auth_expiry",
		"ck_operator_accounts_failure_code",
		"ck_operator_accounts_remote_logout_required",
	} {
		var count int
		if failure := database.QueryRowContext(context, `
			SELECT count(*)
			FROM pg_constraint
			WHERE conrelid = 'operator_accounts'::regclass
			  AND conname = $1`, constraint).Scan(&count); failure != nil {
			t.Fatalf("inspect operator account constraint %q: %v", constraint, failure)
		}
		if count != 1 {
			t.Fatalf("operator account constraint %q count = %d, want 1", constraint, count)
		}
	}
	for _, index := range []string{
		"ix_operator_accounts_operator_status",
		"uq_operator_accounts_telegram_user_id",
		"uq_operator_accounts_operator_phone",
	} {
		var count int
		if failure := database.QueryRowContext(context, `
			SELECT count(*)
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND tablename = 'operator_accounts'
			  AND indexname = $1`, index).Scan(&count); failure != nil {
			t.Fatalf("inspect operator account index %q: %v", index, failure)
		}
		if count != 1 {
			t.Fatalf("operator account index %q count = %d, want 1", index, count)
		}
	}
	var phoneIndexDefinition string
	if failure := database.QueryRowContext(context, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'operator_accounts'
		  AND indexname = 'uq_operator_accounts_operator_phone'`).Scan(&phoneIndexDefinition); failure != nil {
		t.Fatalf("inspect operator account phone index definition: %v", failure)
	}
	phoneIndexDefinition = strings.ToLower(phoneIndexDefinition)
	for _, fragment := range []string{"create unique index", "(operator_id, phone)", "where (phone is not null)"} {
		if !strings.Contains(phoneIndexDefinition, fragment) {
			t.Fatalf("operator account phone index definition = %q, missing %q", phoneIndexDefinition, fragment)
		}
	}
}

func assertCurrentDeliverySecurityCatalog(t *testing.T, context context.Context, database *sql.DB) {
	t.Helper()
	for _, constraint := range []string{
		"ck_operators_password_hash_state",
		"ck_operators_password_hash_format",
		"ck_sessions_format_version",
		"ck_sessions_key_id",
		"ck_sessions_nonce",
		"ck_sessions_ciphertext",
		"ck_operator_web_sessions_token_hash_length",
		"ck_operator_web_sessions_expiry_order",
		"ck_mailing_deliveries_lease",
		"ck_mailing_deliveries_sending_lease",
		"ck_mailing_deliveries_terminal_lease",
		"ck_mailing_deliveries_unknown_evidence",
		"ck_mailing_deliveries_sending_evidence",
	} {
		var count int
		if failure := database.QueryRowContext(context, `
			SELECT count(*)
			FROM pg_constraint AS catalog_constraint
			JOIN pg_namespace AS catalog_namespace
			  ON catalog_namespace.oid = catalog_constraint.connamespace
			WHERE catalog_namespace.nspname = current_schema()
			  AND catalog_constraint.conname = $1`, constraint).Scan(&count); failure != nil {
			t.Fatalf("inspect security constraint %q: %v", constraint, failure)
		}
		if count != 1 {
			t.Fatalf("security constraint %q count = %d, want 1", constraint, count)
		}
	}

	for _, index := range []string{
		"uq_mailing_runs_active",
		"ix_mailing_deliveries_claim",
		"ix_mailing_deliveries_expired_sending_reaper",
	} {
		var count int
		if failure := database.QueryRowContext(context, `
			SELECT count(*)
			FROM pg_class AS catalog_index
			JOIN pg_namespace AS catalog_namespace
			  ON catalog_namespace.oid = catalog_index.relnamespace
			WHERE catalog_namespace.nspname = current_schema()
			  AND catalog_index.relname = $1
			  AND catalog_index.relkind = 'i'`, index).Scan(&count); failure != nil {
			t.Fatalf("inspect security index %q: %v", index, failure)
		}
		if count != 1 {
			t.Fatalf("security index %q count = %d, want 1", index, count)
		}
	}

	for _, trigger := range []string{
		"operators_revoke_web_sessions_after_credential_change",
		"trg_telegram_mailing_deliveries_random_id_immutable",
	} {
		var count int
		if failure := database.QueryRowContext(context, `
			SELECT count(*)
			FROM pg_trigger AS catalog_trigger
			JOIN pg_class AS catalog_table
			  ON catalog_table.oid = catalog_trigger.tgrelid
			JOIN pg_namespace AS catalog_namespace
			  ON catalog_namespace.oid = catalog_table.relnamespace
			WHERE catalog_namespace.nspname = current_schema()
			  AND catalog_trigger.tgname = $1
			  AND NOT catalog_trigger.tgisinternal`, trigger).Scan(&count); failure != nil {
			t.Fatalf("inspect security trigger %q: %v", trigger, failure)
		}
		if count != 1 {
			t.Fatalf("security trigger %q count = %d, want 1", trigger, count)
		}
	}
}

func migrationsPathForTest(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration integration test")
	}
	return filepath.Join(filepath.Dir(filename), "../../../../migrations")
}
