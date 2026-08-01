package pg

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const canonicalOperatorCredentialPHC = "$argon2id$v=19$m=8192,t=1,p=1$QkJCQkJCQkJCQkJCQkJCQg$v4joKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg"

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
