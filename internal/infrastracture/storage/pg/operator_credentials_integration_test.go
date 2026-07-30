package pg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const (
	previousOperatorCredentialMigrationVersion int64 = 20260729000200
	secureOperatorCredentialMigrationVersion   int64 = 20260729000300
	canonicalOperatorCredentialPHC                   = "$argon2id$v=19$m=8192,t=1,p=1$QkJCQkJCQkJCQkJCQkJCQg$v4joKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg"
)

func TestOperatorCredentialStorePostgresBootstrapResetAtomicity(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, _, failure := newIsolatedTelegramPeerLookupDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}

	store := NewOperatorCredentialStore(database)
	operator, failure := store.BootstrapOrReset(context, "bootstrap-admin", canonicalOperatorCredentialPHC)
	if failure != nil {
		t.Fatalf("bootstrap operator: %v", failure)
	}
	var (
		firstID                uuid.UUID
		firstHash              string
		firstBoundary          time.Time
		firstPasswordChangedAt time.Time
	)
	if failure = database.QueryRow(context, `SELECT id, password_hash, password_changed_at, tokens_invalid_before FROM operators WHERE username = $1`, "bootstrap-admin").Scan(&firstID, &firstHash, &firstPasswordChangedAt, &firstBoundary); failure != nil {
		t.Fatalf("read bootstrapped operator: %v", failure)
	}
	if firstID != operator.ID || firstHash != canonicalOperatorCredentialPHC || firstPasswordChangedAt.IsZero() || firstBoundary.IsZero() {
		t.Fatalf("unexpected bootstrapped operator state: id-matches=%t hash-matches=%t changed-at-set=%t boundary-set=%t", firstID == operator.ID, firstHash == canonicalOperatorCredentialPHC, !firstPasswordChangedAt.IsZero(), !firstBoundary.IsZero())
	}

	reset, failure := store.BootstrapOrReset(context, "bootstrap-admin", canonicalOperatorCredentialPHC)
	if failure != nil {
		t.Fatalf("reset operator: %v", failure)
	}
	if reset.ID != firstID {
		t.Fatalf("reset created a new operator: id-preserved=%t", reset.ID == firstID)
	}
	var (
		resetHash           string
		passwordChangedAt   time.Time
		updatedAt           time.Time
		tokensInvalidBefore time.Time
	)
	if failure = database.QueryRow(
		context,
		`SELECT password_hash, password_changed_at, updated_at, tokens_invalid_before
		 FROM operators WHERE id = $1`,
		firstID,
	).Scan(&resetHash, &passwordChangedAt, &updatedAt, &tokensInvalidBefore); failure != nil {
		t.Fatalf("read reset operator: %v", failure)
	}
	if resetHash != canonicalOperatorCredentialPHC || passwordChangedAt.IsZero() || updatedAt.IsZero() || tokensInvalidBefore.Before(firstBoundary) {
		t.Fatalf("reset did not update credential metadata and preserve a monotonic boundary: hash-matches=%t changed-at-set=%t updated-at-set=%t boundary-monotonic=%t", resetHash == canonicalOperatorCredentialPHC, !passwordChangedAt.IsZero(), !updatedAt.IsZero(), !tokensInvalidBefore.Before(firstBoundary))
	}

	// A boundary from a clock ahead of the database clock must never be moved
	// backwards by a reset. This exercises the GREATEST(existing, new) branch.
	futureBoundary := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	if _, failure = database.Exec(context, `UPDATE operators SET tokens_invalid_before = $1 WHERE id = $2`, futureBoundary, firstID); failure != nil {
		t.Fatalf("seed future token boundary: %v", failure)
	}
	if _, failure = store.BootstrapOrReset(context, "bootstrap-admin", canonicalOperatorCredentialPHC); failure != nil {
		t.Fatalf("reset with future token boundary: %v", failure)
	}
	var retainedBoundary time.Time
	if failure = database.QueryRow(context, `SELECT tokens_invalid_before FROM operators WHERE id = $1`, firstID).Scan(&retainedBoundary); failure != nil {
		t.Fatalf("read retained token boundary: %v", failure)
	}
	if !retainedBoundary.Equal(futureBoundary) {
		t.Fatalf("reset moved a future token boundary: retained=%v expected=%v", retainedBoundary, futureBoundary)
	}

	const concurrentOperator = "concurrent-bootstrap-admin"
	if _, failure = store.BootstrapOrReset(context, concurrentOperator, canonicalOperatorCredentialPHC); failure != nil {
		t.Fatalf("seed concurrent reset operator: %v", failure)
	}
	var concurrentInitialBoundary time.Time
	if failure = database.QueryRow(context, `SELECT tokens_invalid_before FROM operators WHERE username = $1`, concurrentOperator).Scan(&concurrentInitialBoundary); failure != nil {
		t.Fatalf("read concurrent reset boundary: %v", failure)
	}
	var wait sync.WaitGroup
	errors := make(chan error, 8)
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, failure := store.BootstrapOrReset(context, concurrentOperator, canonicalOperatorCredentialPHC); failure != nil {
				errors <- failure
			}
		}()
	}
	wait.Wait()
	close(errors)
	for failure := range errors {
		t.Fatalf("concurrent bootstrap/reset failed: %v", failure)
	}
	var (
		count              int
		concurrentHash     string
		concurrentBoundary time.Time
	)
	if failure = database.QueryRow(context, `SELECT count(*), max(password_hash), max(tokens_invalid_before) FROM operators WHERE username = $1`, concurrentOperator).Scan(&count, &concurrentHash, &concurrentBoundary); failure != nil {
		t.Fatalf("read concurrent bootstrap result: %v", failure)
	}
	if count != 1 || concurrentHash != canonicalOperatorCredentialPHC || concurrentBoundary.IsZero() || concurrentBoundary.Before(concurrentInitialBoundary) {
		t.Fatalf("concurrent reset was not atomic and monotonic: count=%d hash-matches=%t boundary-set=%t boundary-monotonic=%t", count, concurrentHash == canonicalOperatorCredentialPHC, !concurrentBoundary.IsZero(), !concurrentBoundary.Before(concurrentInitialBoundary))
	}
}

func TestOperatorCredentialStateConstraintsPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, _, failure := newIsolatedTelegramPeerLookupDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}

	if _, failure = database.Exec(context, `INSERT INTO operators (username) VALUES ($1)`, "unprovisioned-state"); failure != nil {
		t.Fatalf("null/null unprovisioned state was rejected: %v", failure)
	}
	changedAt := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	if _, failure = database.Exec(context, `INSERT INTO operators (username, password_hash, password_changed_at) VALUES ($1, $2, $3)`, "provisioned-state", canonicalOperatorCredentialPHC, changedAt); failure != nil {
		t.Fatalf("valid PHC provisioned state was rejected: %v", failure)
	}

	for _, test := range []struct {
		name string
		hash any
		at   any
	}{
		{name: "hash without changed timestamp", hash: canonicalOperatorCredentialPHC, at: nil},
		{name: "changed timestamp without hash", hash: nil, at: changedAt},
		{name: "malformed PHC", hash: "not-a-phc", at: changedAt},
		{name: "unsupported PHC", hash: "$bcrypt$v=19$m=8192,t=1,p=1$QkJCQkJCQkJCQkJCQkJCQg$v4joKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg", at: changedAt},
		{name: "canonical PHC with trailing junk", hash: canonicalOperatorCredentialPHC + "-trailing-junk", at: changedAt},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Exec(context, `INSERT INTO operators (username, password_hash, password_changed_at) VALUES ($1, $2, $3)`, "invalid-state-"+strings.ReplaceAll(test.name, " ", "-"), test.hash, test.at)
			if err == nil {
				t.Fatal("invalid credential state was accepted")
			}
		})
	}
}

func TestOperatorCredentialMigrationDestructiveUpgradePostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, isolatedURL, failure := newPreCutoverOperatorCredentialDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare pre-cutover PostgreSQL database: %v", failure)
	}
	legacyID := uuid.New()
	legacyBoundary := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	if _, failure = database.Exec(context, `INSERT INTO operators (id, username, password, tokens_invalid_before) VALUES ($1, $2, $3, $4)`, legacyID, "legacy-operator", "legacy-password", legacyBoundary); failure != nil {
		t.Fatalf("insert legacy operator fixture: %v", failure)
	}
	if failure = applyOperatorCredentialMigration(context, isolatedURL, secureOperatorCredentialMigrationVersion); failure != nil {
		t.Fatalf("apply secure operator credential cutover: %v", failure)
	}

	var (
		rowID         uuid.UUID
		username      string
		passwordHash  *string
		passwordAt    *time.Time
		invalidBefore time.Time
	)
	if failure = database.QueryRow(context, `SELECT id, username, password_hash, password_changed_at, tokens_invalid_before FROM operators WHERE id = $1`, legacyID).Scan(&rowID, &username, &passwordHash, &passwordAt, &invalidBefore); failure != nil {
		t.Fatalf("read migrated legacy operator: %v", failure)
	}
	if rowID != legacyID || username != "legacy-operator" || passwordHash != nil || passwordAt != nil || !invalidBefore.After(legacyBoundary) {
		t.Fatalf("destructive cutover state incorrect: row-preserved=%t username-preserved=%t unprovisioned=%t boundary-advanced=%t", rowID == legacyID, username == "legacy-operator", passwordHash == nil && passwordAt == nil, invalidBefore.After(legacyBoundary))
	}
	var passwordColumnCount int
	if failure = database.QueryRow(context, `SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'operators' AND column_name = 'password'`).Scan(&passwordColumnCount); failure != nil {
		t.Fatalf("inspect removed password column: %v", failure)
	}
	if passwordColumnCount != 0 {
		t.Fatal("legacy operators.password column still exists after cutover")
	}
}

func TestOperatorCredentialMigrationCatalogPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, _, failure := newIsolatedTelegramPeerLookupDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	var passwordColumnCount int
	if failure = database.QueryRow(context, `SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'operators' AND column_name = 'password'`).Scan(&passwordColumnCount); failure != nil {
		t.Fatalf("inspect removed password column: %v", failure)
	}
	if passwordColumnCount != 0 {
		t.Fatal("legacy operators.password column still exists")
	}
	var hashColumnNullable string
	if failure = database.QueryRow(context, `SELECT is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'operators' AND column_name = 'password_hash'`).Scan(&hashColumnNullable); failure != nil {
		t.Fatalf("inspect password_hash column: %v", failure)
	}
	if hashColumnNullable != "YES" {
		t.Fatalf("password_hash must be nullable for unprovisioned operators, got nullable=%t", hashColumnNullable == "YES")
	}
	var passwordChangedColumnNullable string
	if failure = database.QueryRow(context, `SELECT is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'operators' AND column_name = 'password_changed_at'`).Scan(&passwordChangedColumnNullable); failure != nil {
		t.Fatalf("inspect password_changed_at column: %v", failure)
	}
	if passwordChangedColumnNullable != "YES" {
		t.Fatalf("password_changed_at must be nullable for unprovisioned operators, got nullable=%t", passwordChangedColumnNullable == "YES")
	}
}

func newPreCutoverOperatorCredentialDatabase(ctx context.Context, t *testing.T, databaseURL string) (*pgxpool.Pool, string, error) {
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
	schema := "operator_credentials_upgrade_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	isolatedURL, failure := telegramPeerLookupDatabaseURL(databaseURL, schema)
	if failure != nil {
		return nil, "", failure
	}
	if failure = applyIntegrationMigrationsThrough(ctx, isolatedURL, previousOperatorCredentialMigrationVersion); failure != nil {
		return nil, "", fmt.Errorf("apply migrations through previous version: %w", failure)
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
		return nil, "", fmt.Errorf("open isolated PostgreSQL pool: %w", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		database.Close()
		database = nil
		return nil, "", fmt.Errorf("ping isolated PostgreSQL pool: %w", failure)
	}
	return database, isolatedURL, nil
}

func applyIntegrationMigrationsThrough(ctx context.Context, databaseURL string, target int64) error {
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		return failure
	}
	defer database.Close()
	if failure = database.PingContext(ctx); failure != nil {
		return failure
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate integration test source")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPath), goose.WithAllowOutofOrder(true))
	if failure != nil {
		return failure
	}
	defer provider.Close()
	if _, failure = provider.UpTo(ctx, target); failure != nil {
		if _, ackFailure := database.ExecContext(ctx, `INSERT INTO delivery_execution_v2_cutover_ack (acknowledgement_id, acknowledged_by) VALUES (TRUE, current_user)`); ackFailure != nil {
			return fmt.Errorf("acknowledge delivery execution v2 cutover: %w", ackFailure)
		}
		_, failure = provider.UpTo(ctx, target)
	}
	return failure
}

func applyOperatorCredentialMigration(ctx context.Context, databaseURL string, version int64) error {
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		return failure
	}
	defer database.Close()
	if failure = database.PingContext(ctx); failure != nil {
		return failure
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate integration test source")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	provider, failure := goose.NewProvider(goose.DialectPostgres, database, os.DirFS(migrationsPath), goose.WithAllowOutofOrder(true))
	if failure != nil {
		return failure
	}
	defer provider.Close()
	if current, _, versionFailure := provider.GetVersions(ctx); versionFailure != nil {
		return versionFailure
	} else if current != previousOperatorCredentialMigrationVersion {
		return fmt.Errorf("unexpected pre-cutover migration version: %d", current)
	}
	_, failure = provider.ApplyVersion(ctx, version, true)
	return failure
}
