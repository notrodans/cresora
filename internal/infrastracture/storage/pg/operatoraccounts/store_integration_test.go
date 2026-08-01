package operatoraccounts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/config"
	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	pgstorage "github.com/notrodans/cresora/internal/infrastracture/storage/pg"
	"github.com/notrodans/cresora/internal/transport/telegram"
)

func TestOperatorAccountStorePostgresLifecycleOwnershipAndSessionDeletion(t *testing.T) {
	context, database := newOperatorAccountsIntegrationDatabase(t)
	operatorA := uuid.New()
	operatorB := uuid.New()
	accountA := uuid.New()
	accountB := uuid.New()
	if _, failure := database.Exec(
		context,
		`INSERT INTO operators (id, username) VALUES ($1, $2), ($3, $4)`,
		operatorA,
		"operator-account-a-"+operatorA.String()[:8],
		operatorB,
		"operator-account-b-"+operatorB.String()[:8],
	); failure != nil {
		t.Fatalf("insert operators: %v", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO operator_accounts (id, operator_id, status) VALUES
			($1, $2, 'disconnected'),
			($3, $4, 'disconnected')`,
		accountA,
		operatorA,
		accountB,
		operatorB,
	); failure != nil {
		t.Fatalf("insert operator accounts: %v", failure)
	}

	store := New(database)
	actorA := application.Actor{OperatorID: operatorA}
	actorB := application.Actor{OperatorID: operatorB}
	sessionStore, failure := pgstorage.NewTelegramSessionStore(
		database,
		"current",
		[]byte("01234567890123456789012345678901"),
	)
	if failure != nil {
		t.Fatalf("create Telegram session store: %v", failure)
	}

	loaded, failure := store.LoadAccount(context, actorA, operatoraccount.Identity(accountA))
	if failure != nil {
		t.Fatalf("load disconnected account: %v", failure)
	}
	if loaded.Status() != operatoraccount.StatusDisconnected || loaded.Version() != operatoraccount.InitialVersion {
		t.Fatalf("loaded account = status %q version %d, want disconnected version 1", loaded.Status(), loaded.Version())
	}

	ownedScope := telegram.SessionScope{OperatorID: operatorA, AccountID: accountA}
	foreignScope := telegram.SessionScope{OperatorID: operatorA, AccountID: accountB}
	unknownScope := telegram.SessionScope{OperatorID: operatorA, AccountID: uuid.New()}
	if failure := sessionStore.Store(context, ownedScope, []byte("not allowed while disconnected")); !errors.Is(failure, telegram.ErrSessionUnauthorized) {
		t.Fatalf("store disconnected session: error = %v, want %v", failure, telegram.ErrSessionUnauthorized)
	}
	foreignStoreFailure := sessionStore.Store(context, foreignScope, []byte("foreign"))
	unknownStoreFailure := sessionStore.Store(context, unknownScope, []byte("unknown"))
	if !errors.Is(foreignStoreFailure, telegram.ErrSessionUnauthorized) || !errors.Is(unknownStoreFailure, telegram.ErrSessionUnauthorized) || foreignStoreFailure.Error() != unknownStoreFailure.Error() {
		t.Fatalf("foreign and random session stores differ: foreign=%v random=%v", foreignStoreFailure, unknownStoreFailure)
	}

	authenticating := loaded
	if failure := authenticating.BeginAuthentication(testAuthenticationExpiry()); failure != nil {
		t.Fatalf("begin authentication: %v", failure)
	}
	concurrentResults := make(chan error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func(snapshot operatoraccount.Account) {
			defer wait.Done()
			<-start
			concurrentResults <- store.PersistLifecycle(context, actorA, snapshot, loaded.Version())
		}(authenticating)
	}
	close(start)
	wait.Wait()
	close(concurrentResults)

	var successes, conflicts int
	for failure := range concurrentResults {
		switch {
		case failure == nil:
			successes++
		case errors.Is(failure, applicationoperatoraccounts.ErrAccountVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent lifecycle transition: unexpected error %v", failure)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent lifecycle transition: successes=%d conflicts=%d, want one each", successes, conflicts)
	}

	if failure := sessionStore.Store(context, ownedScope, []byte("encrypted session")); failure != nil {
		t.Fatalf("store authenticating session: %v", failure)
	}
	active := mustLoadAccount(t, context, store, actorA, accountA)
	if failure := active.Activate(7001); failure != nil {
		t.Fatalf("activate account: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorA, active, active.Version()-1); failure != nil {
		t.Fatalf("persist active lifecycle: %v", failure)
	}
	if failure := store.DeleteSession(context, actorA, operatoraccount.Identity(accountA)); !errors.Is(failure, applicationoperatoraccounts.ErrAccountStateConflict) {
		t.Fatalf("delete active account session: error = %v, want %v", failure, applicationoperatoraccounts.ErrAccountStateConflict)
	}
	activeAfterRejectedDelete := mustLoadAccount(t, context, store, actorA, accountA)
	if activeAfterRejectedDelete.Status() != operatoraccount.StatusActive || activeAfterRejectedDelete.Version() != active.Version() {
		t.Fatalf("account after rejected active deletion = status %q version %d, want active version %d", activeAfterRejectedDelete.Status(), activeAfterRejectedDelete.Version(), active.Version())
	}
	var activeSessionCount int
	if failure := database.QueryRow(context, `SELECT count(*) FROM sessions WHERE account_id = $1`, accountA).Scan(&activeSessionCount); failure != nil {
		t.Fatalf("count session after rejected active deletion: %v", failure)
	}
	if activeSessionCount != 1 {
		t.Fatalf("session count after rejected active deletion = %d, want 1", activeSessionCount)
	}

	second := mustLoadAccount(t, context, store, actorB, accountB)
	if failure := second.BeginAuthentication(testAuthenticationExpiry()); failure != nil {
		t.Fatalf("begin second authentication: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorB, second, second.Version()-1); failure != nil {
		t.Fatalf("persist second authenticating lifecycle: %v", failure)
	}
	secondActive := second
	if failure := secondActive.Activate(7001); failure != nil {
		t.Fatalf("activate second account: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorB, secondActive, secondActive.Version()-1); !errors.Is(failure, applicationoperatoraccounts.ErrSessionNotFound) {
		t.Fatalf("activate without encrypted session: error = %v, want %v", failure, applicationoperatoraccounts.ErrSessionNotFound)
	}
	second = mustLoadAccount(t, context, store, actorB, accountB)
	if failure := sessionStore.Store(context, telegram.SessionScope{OperatorID: operatorB, AccountID: accountB}, []byte("second session")); failure != nil {
		t.Fatalf("store second authenticating session: %v", failure)
	}
	secondActive = second
	if failure := secondActive.Activate(7001); failure != nil {
		t.Fatalf("activate second account with session: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorB, secondActive, secondActive.Version()-1); failure == nil {
		t.Fatal("duplicate Telegram identity was accepted by lifecycle adapter")
	}

	active = mustLoadAccount(t, context, store, actorA, accountA)
	if failure := active.BeginDisconnect(); failure != nil {
		t.Fatalf("begin disconnect: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorA, active, active.Version()-1); failure != nil {
		t.Fatalf("persist disconnecting lifecycle: %v", failure)
	}
	disconnected := mustLoadAccount(t, context, store, actorA, accountA)
	if failure := disconnected.MarkDisconnected(); failure != nil {
		t.Fatalf("mark disconnected: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actorA, disconnected, disconnected.Version()-1); failure != nil {
		t.Fatalf("persist disconnected lifecycle: %v", failure)
	}
	var sessionCount int
	if failure := database.QueryRow(context, `SELECT count(*) FROM sessions WHERE account_id = $1`, accountA).Scan(&sessionCount); failure != nil {
		t.Fatalf("count disconnected sessions: %v", failure)
	}
	if sessionCount != 0 {
		t.Fatalf("disconnected session count = %d, want 0", sessionCount)
	}

	if failure := store.DeleteSession(context, actorA, operatoraccount.Identity(accountA)); failure != nil {
		t.Fatalf("delete already-missing owned session: %v", failure)
	}
	foreignDeleteFailure := store.DeleteSession(context, actorA, operatoraccount.Identity(accountB))
	unknownDeleteFailure := store.DeleteSession(context, actorA, operatoraccount.Identity(uuid.New()))
	if !errors.Is(foreignDeleteFailure, applicationoperatoraccounts.ErrAccountNotFound) || !errors.Is(unknownDeleteFailure, applicationoperatoraccounts.ErrAccountNotFound) || foreignDeleteFailure.Error() != unknownDeleteFailure.Error() {
		t.Fatalf("foreign and random session deletes differ: foreign=%v random=%v", foreignDeleteFailure, unknownDeleteFailure)
	}
}

func mustLoadAccount(
	t *testing.T,
	context context.Context,
	store *Store,
	actor application.Actor,
	accountID uuid.UUID,
) operatoraccount.Account {
	t.Helper()
	account, failure := store.LoadAccount(context, actor, operatoraccount.Identity(accountID))
	if failure != nil {
		t.Fatalf("load account %s: %v", accountID, failure)
	}
	return account
}

func newOperatorAccountsIntegrationDatabase(t *testing.T) (context.Context, *pgxpool.Pool) {
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
	schema := "operator_accounts_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, failure = admin.Exec(ctx, "CREATE SCHEMA "+schema); failure != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", failure)
	}
	isolatedURL, failure := isolatedPostgresURL(databaseURL, schema)
	if failure != nil {
		admin.Close()
		t.Fatalf("create isolated database URL: %v", failure)
	}
	if failure = pgstorage.ExecuteMigrations(
		ctx,
		&config.Config{DbUrl: isolatedURL},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		migrationsPath(t),
	); failure != nil {
		admin.Close()
		t.Fatalf("apply current baseline: %v", failure)
	}
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

func isolatedPostgresURL(databaseURL, schema string) (string, error) {
	parsed, failure := url.Parse(databaseURL)
	if failure != nil {
		return "", fmt.Errorf("parse PostgreSQL URL: %w", failure)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", errors.New("TEST_DATABASE_URL must be a PostgreSQL URL")
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
		t.Fatal("locate operator account integration test")
	}
	return filepath.Join(filepath.Dir(filename), "../../../../../migrations")
}
