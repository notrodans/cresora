package operatoraccounts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

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
		`INSERT INTO operator_accounts (id, operator_id, status, status_version) VALUES
			($1, $2, 'disconnected', 1),
			($3, $4, 'disconnected', 1)`,
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

func TestOperatorAccountStorePostgresRemoteLogoutIntentLifecycle(t *testing.T) {
	context, database := newOperatorAccountsIntegrationDatabase(t)
	operatorID := uuid.New()
	activeID := uuid.New()
	reauthID := uuid.New()
	authenticatingID := uuid.New()
	localDisconnectingID := uuid.New()
	if _, failure := database.Exec(
		context,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		operatorID,
		"operator-account-intent-"+operatorID.String()[:8],
	); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO operator_accounts (id, operator_id, status, status_version, telegram_user_id, auth_expires_at, failure_code) VALUES
			($1, $2, 'active', 1, 7101, NULL, NULL),
			($3, $2, 'reauth_required', 4, 7102, NULL, 'session_invalid'),
			($4, $2, 'authenticating', 6, NULL, CURRENT_TIMESTAMP + interval '1 hour', NULL),
			($5, $2, 'disconnecting', 3, NULL, NULL, NULL)`,
		activeID,
		operatorID,
		reauthID,
		authenticatingID,
		localDisconnectingID,
	); failure != nil {
		t.Fatalf("insert operator accounts: %v", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO sessions (account_id, format_version, key_id, nonce, ciphertext)
		 VALUES ($1, 1, 'test', $2, $3)`,
		activeID,
		[]byte("012345678901"),
		[]byte("0123456789012345"),
	); failure != nil {
		t.Fatalf("insert encrypted session: %v", failure)
	}

	store := New(database)
	actor := application.Actor{OperatorID: operatorID}
	active := mustLoadAccount(t, context, store, actor, activeID)
	if active.RemoteLogoutRequired() {
		t.Fatal("active account loaded with a remote logout intent")
	}
	staleActive := active
	if failure := staleActive.BeginDisconnect(); failure != nil {
		t.Fatalf("begin stale active disconnect: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actor, staleActive, active.Version()); failure != nil {
		t.Fatalf("persist active remote logout intent: %v", failure)
	}
	conflictingActive := active
	if failure := conflictingActive.BeginDisconnect(); failure != nil {
		t.Fatalf("begin conflicting active disconnect: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actor, conflictingActive, active.Version()); !errors.Is(failure, applicationoperatoraccounts.ErrAccountVersionConflict) {
		t.Fatalf("persist conflicting active intent error = %v, want %v", failure, applicationoperatoraccounts.ErrAccountVersionConflict)
	}
	assertRemoteLogoutIntentAndSession(t, context, database, activeID, true, true)
	assertRemoteLogoutTargets(t, context, store, []uuid.UUID{activeID})

	activeDisconnecting := mustLoadAccount(t, context, store, actor, activeID)
	if !activeDisconnecting.RemoteLogoutRequired() || activeDisconnecting.Status() != operatoraccount.StatusDisconnecting {
		t.Fatalf("active disconnecting account = status %q remote=%t, want disconnecting/true", activeDisconnecting.Status(), activeDisconnecting.RemoteLogoutRequired())
	}
	if failure := activeDisconnecting.MarkDisconnected(); failure != nil {
		t.Fatalf("mark active account disconnected: %v", failure)
	}
	if failure := store.PersistLifecycle(context, actor, activeDisconnecting, activeDisconnecting.Version()-1); failure != nil {
		t.Fatalf("persist completed active disconnect: %v", failure)
	}
	assertRemoteLogoutIntentAndSession(t, context, database, activeID, false, false)
	if restored := mustLoadAccount(t, context, store, actor, activeID); restored.RemoteLogoutRequired() {
		t.Fatal("disconnected account retained a remote logout intent")
	}

	reauth := mustLoadAccount(t, context, store, actor, reauthID)
	if reauth.RemoteLogoutRequired() {
		t.Fatal("reauthentication-required account loaded with a remote logout intent")
	}
	if failure := reauth.BeginDisconnect(); failure != nil {
		t.Fatalf("begin reauthentication-required disconnect: %v", failure)
	}
	if !reauth.RemoteLogoutRequired() {
		t.Fatal("reauthentication-required disconnect did not create a remote logout intent")
	}
	if failure := store.PersistLifecycle(context, actor, reauth, reauth.Version()-1); failure != nil {
		t.Fatalf("persist reauthentication-required remote logout intent: %v", failure)
	}
	assertRemoteLogoutTargets(t, context, store, []uuid.UUID{reauthID})

	authenticating := mustLoadAccount(t, context, store, actor, authenticatingID)
	if authenticating.RemoteLogoutRequired() {
		t.Fatal("authenticating account loaded with a remote logout intent")
	}
	if failure := authenticating.BeginDisconnect(); failure != nil {
		t.Fatalf("begin local authenticating disconnect: %v", failure)
	}
	if authenticating.RemoteLogoutRequired() {
		t.Fatal("local authenticating disconnect created a remote logout intent")
	}
	if failure := store.PersistLifecycle(context, actor, authenticating, authenticating.Version()-1); failure != nil {
		t.Fatalf("persist local authenticating disconnect: %v", failure)
	}
	assertRemoteLogoutTargets(t, context, store, []uuid.UUID{reauthID})

	var localIntent bool
	if failure := database.QueryRow(context, `SELECT remote_logout_required FROM operator_accounts WHERE id = $1`, localDisconnectingID).Scan(&localIntent); failure != nil {
		t.Fatalf("read local disconnecting remote intent: %v", failure)
	}
	if localIntent {
		t.Fatal("local disconnecting account was included as a remote logout intent")
	}
}

func assertRemoteLogoutIntentAndSession(
	t *testing.T,
	context context.Context,
	database *pgxpool.Pool,
	accountID uuid.UUID,
	wantIntent bool,
	wantSession bool,
) {
	t.Helper()
	var remoteLogoutRequired bool
	if failure := database.QueryRow(context, `SELECT remote_logout_required FROM operator_accounts WHERE id = $1`, accountID).Scan(&remoteLogoutRequired); failure != nil {
		t.Fatalf("read remote logout intent for %s: %v", accountID, failure)
	}
	if remoteLogoutRequired != wantIntent {
		t.Fatalf("remote logout intent for %s = %t, want %t", accountID, remoteLogoutRequired, wantIntent)
	}
	var sessionCount int
	if failure := database.QueryRow(context, `SELECT count(*) FROM sessions WHERE account_id = $1`, accountID).Scan(&sessionCount); failure != nil {
		t.Fatalf("count encrypted sessions for %s: %v", accountID, failure)
	}
	if (sessionCount > 0) != wantSession {
		t.Fatalf("encrypted session presence for %s = %t, want %t", accountID, sessionCount > 0, wantSession)
	}
}

func assertRemoteLogoutTargets(
	t *testing.T,
	context context.Context,
	store *Store,
	wantAccountIDs []uuid.UUID,
) {
	t.Helper()
	targets, failure := store.ListRemoteLogoutIntents(context)
	if failure != nil {
		t.Fatalf("list remote logout intents: %v", failure)
	}
	if len(targets) != len(wantAccountIDs) {
		t.Fatalf("remote logout intent count = %d, want %d (%+v)", len(targets), len(wantAccountIDs), targets)
	}
	for index, target := range targets {
		if target.Actor.OperatorID == uuid.Nil || target.AccountID.UUID() != wantAccountIDs[index] || target.Status != operatoraccount.StatusDisconnecting || target.Version == 0 {
			t.Fatalf("remote logout target[%d] = %+v, want actor, account %s, disconnecting, positive version", index, target, wantAccountIDs[index])
		}
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
