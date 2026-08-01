package operatoraccounts

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	pgstorage "github.com/notrodans/cresora/internal/infrastracture/storage/pg"
	"github.com/notrodans/cresora/internal/transport/telegram"
)

func TestOperatorAccountAuthenticationAdmissionIsDurableAndConcurrent(t *testing.T) {
	context, database := newOperatorAccountsIntegrationDatabase(t)
	operatorID := uuid.New()
	if _, failure := database.Exec(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "auth-admission-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}

	store := New(database)
	actor := application.Actor{OperatorID: operatorID}
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	results := make(chan struct {
		result  applicationoperatoraccountauth.BeginResult
		failure error
	}, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			<-start
			result, failure := store.BeginOrResume(context, actor, "+1 (202) 555-0101", expiresAt)
			results <- struct {
				result  applicationoperatoraccountauth.BeginResult
				failure error
			}{result: result, failure: failure}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var (
		started    int
		inProgress int
		accountID  uuid.UUID
	)
	for result := range results {
		if result.failure != nil {
			t.Fatalf("concurrent authentication admission: %v", result.failure)
		}
		if failure := result.result.Validate(); failure != nil {
			t.Fatalf("validate concurrent admission result: %v", failure)
		}
		switch result.result.Outcome {
		case applicationoperatoraccountauth.BeginStarted:
			started++
		case applicationoperatoraccountauth.BeginInProgress:
			inProgress++
		default:
			t.Fatalf("concurrent admission outcome = %q, want started or in_progress", result.result.Outcome)
		}
		if accountID == uuid.Nil {
			accountID = result.result.Account.ID
		} else if result.result.Account.ID != accountID {
			t.Fatalf("concurrent admission account IDs differ: %s and %s", accountID, result.result.Account.ID)
		}
		if result.result.Account.Phone != "+12025550101" {
			t.Fatalf("normalized admission phone = %q, want +12025550101", result.result.Account.Phone)
		}
		if result.result.Account.Status != operatoraccount.StatusAuthenticating || result.result.Account.Version != operatoraccount.InitialVersion+1 {
			t.Fatalf("admitted account = status %q version %d, want authenticating version 2", result.result.Account.Status, result.result.Account.Version)
		}
		if !result.result.AuthExpiresAt.Equal(expiresAt) {
			t.Fatalf("admitted authentication expiry = %s, want %s", result.result.AuthExpiresAt, expiresAt)
		}
	}
	if started != 1 || inProgress != 1 {
		t.Fatalf("concurrent admission outcomes = started %d in_progress %d, want one each", started, inProgress)
	}

	var count int
	if failure := database.QueryRow(context, `SELECT count(*) FROM operator_accounts WHERE operator_id = $1 AND phone = $2`, operatorID, "+12025550101").Scan(&count); failure != nil {
		t.Fatalf("count admitted accounts: %v", failure)
	}
	if count != 1 {
		t.Fatalf("admitted account count = %d, want 1", count)
	}
	var storedExpiry time.Time
	if failure := database.QueryRow(context, `SELECT auth_expires_at FROM operator_accounts WHERE id = $1`, accountID).Scan(&storedExpiry); failure != nil {
		t.Fatalf("read admitted authentication expiry: %v", failure)
	}
	if !storedExpiry.Equal(expiresAt) {
		t.Fatalf("stored authentication expiry = %s, want %s", storedExpiry, expiresAt)
	}
	inProgressResult, failure := store.BeginOrResume(context, actor, "+12025550101", expiresAt.Add(2*time.Hour))
	if failure != nil {
		t.Fatalf("begin existing authentication: %v", failure)
	}
	if inProgressResult.Outcome != applicationoperatoraccountauth.BeginInProgress || inProgressResult.Account.Version != operatoraccount.InitialVersion+1 || !inProgressResult.AuthExpiresAt.Equal(expiresAt) {
		t.Fatalf("existing authentication result = outcome %q version %d, want in_progress version 2", inProgressResult.Outcome, inProgressResult.Account.Version)
	}
	if failure := inProgressResult.Validate(); failure != nil {
		t.Fatalf("validate in-progress admission result: %v", failure)
	}
	var unchangedExpiry time.Time
	if failure := database.QueryRow(context, `SELECT auth_expires_at FROM operator_accounts WHERE id = $1`, accountID).Scan(&unchangedExpiry); failure != nil {
		t.Fatalf("read unchanged authentication expiry: %v", failure)
	}
	if !unchangedExpiry.Equal(expiresAt) {
		t.Fatalf("in-progress authentication expiry = %s, want unchanged %s", unchangedExpiry, expiresAt)
	}

	list, failure := store.ListAccounts(context, actor)
	if failure != nil {
		t.Fatalf("list actor accounts: %v", failure)
	}
	if len(list) != 1 || list[0].ID != accountID {
		t.Fatalf("actor account list = %+v, want one admitted account %s", list, accountID)
	}
	orphans, failure := store.ListOrphanAuthenticationLifecycles(context)
	if failure != nil {
		t.Fatalf("list orphan authentications: %v", failure)
	}
	if len(orphans) != 1 || orphans[0].AccountID.UUID() != accountID || orphans[0].Status != operatoraccount.StatusAuthenticating || orphans[0].Version != operatoraccount.InitialVersion+1 {
		t.Fatalf("orphan authentication list = %+v, want account %s version 2", orphans, accountID)
	}
}

func TestOperatorAccountAuthenticationAbortRetriesAfterAmbiguousComplete(t *testing.T) {
	context, database := newOperatorAccountsIntegrationDatabase(t)
	operatorID := uuid.New()
	if _, failure := database.Exec(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "auth-abort-ambiguous-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}

	injectedDatabase := &commitResponseErrorDatabase{database: database}
	store := &Store{database: injectedDatabase}
	actor := application.Actor{OperatorID: operatorID}
	started, failure := store.BeginOrResume(context, actor, "+12025550105", time.Now().UTC().Add(time.Hour))
	if failure != nil {
		t.Fatalf("begin authentication: %v", failure)
	}
	authenticatingVersion := started.Account.Version
	disconnectingVersion, failure := store.BeginAbort(context, actor, operatoraccount.Identity(started.Account.ID), authenticatingVersion)
	if failure != nil {
		t.Fatalf("begin abort: %v", failure)
	}
	if disconnectingVersion != authenticatingVersion+1 {
		t.Fatalf("begin abort version = %d, want %d", disconnectingVersion, authenticatingVersion+1)
	}

	// Model Cancel's CompleteAbort response being lost after PostgreSQL has
	// committed the disconnect.
	injectedDatabase.failNextCommit.Store(true)
	if failure = store.CompleteAbort(context, actor, operatoraccount.Identity(started.Account.ID), disconnectingVersion); !errors.Is(failure, errInjectedCommitResponse) {
		t.Fatalf("first complete abort error = %v, want injected response error", failure)
	}

	var (
		statusBeforeRetry  string
		versionBeforeRetry int64
	)
	if failure = database.QueryRow(context, `SELECT status::text, status_version FROM operator_accounts WHERE id = $1`, started.Account.ID).Scan(&statusBeforeRetry, &versionBeforeRetry); failure != nil {
		t.Fatalf("read state after ambiguous complete abort: %v", failure)
	}
	if statusBeforeRetry != string(operatoraccount.StatusDisconnected) || versionBeforeRetry != int64(authenticatingVersion)+2 {
		t.Fatalf("state after ambiguous complete abort = status %q version %d, want disconnected version %d", statusBeforeRetry, versionBeforeRetry, authenticatingVersion+2)
	}

	// A retry from Cancel or Shutdown repeats the durable abort protocol. The
	// already-complete state must produce the old fencing version without
	// advancing the lifecycle again.
	retryVersion, failure := store.BeginAbort(context, actor, operatoraccount.Identity(started.Account.ID), authenticatingVersion)
	if failure != nil {
		t.Fatalf("retry begin abort after ambiguous completion: %v", failure)
	}
	if retryVersion != disconnectingVersion {
		t.Fatalf("retry begin abort version = %d, want synthetic disconnecting version %d", retryVersion, disconnectingVersion)
	}
	if failure = store.CompleteAbort(context, actor, operatoraccount.Identity(started.Account.ID), retryVersion); failure != nil {
		t.Fatalf("retry complete abort: %v", failure)
	}

	var (
		statusAfterRetry  string
		versionAfterRetry int64
	)
	if failure = database.QueryRow(context, `SELECT status::text, status_version FROM operator_accounts WHERE id = $1`, started.Account.ID).Scan(&statusAfterRetry, &versionAfterRetry); failure != nil {
		t.Fatalf("read state after abort retry: %v", failure)
	}
	if statusAfterRetry != statusBeforeRetry || versionAfterRetry != versionBeforeRetry {
		t.Fatalf("state after abort retry = status %q version %d, want unchanged status %q version %d", statusAfterRetry, versionAfterRetry, statusBeforeRetry, versionBeforeRetry)
	}

	_, foreignFailure := store.BeginAbort(context, application.Actor{OperatorID: uuid.New()}, operatoraccount.Identity(started.Account.ID), authenticatingVersion)
	_, unknownFailure := store.BeginAbort(context, actor, operatoraccount.Identity(uuid.New()), authenticatingVersion)
	if foreignFailure == nil || unknownFailure == nil ||
		!errors.Is(foreignFailure, applicationoperatoraccountauth.ErrAccountNotFound) ||
		!errors.Is(unknownFailure, applicationoperatoraccountauth.ErrAccountNotFound) ||
		foreignFailure.Error() != unknownFailure.Error() {
		t.Fatalf("foreign and unknown aborts disclose account state: foreign=%v unknown=%v", foreignFailure, unknownFailure)
	}
}

func TestOperatorAccountAuthenticationFinalizeAndAbortAreConditional(t *testing.T) {
	context, database := newOperatorAccountsIntegrationDatabase(t)
	operatorID := uuid.New()
	if _, failure := database.Exec(context, `INSERT INTO operators (id, username) VALUES ($1, $2)`, operatorID, "auth-finalize-"+operatorID.String()[:8]); failure != nil {
		t.Fatalf("insert operator: %v", failure)
	}
	store := New(database)
	actor := application.Actor{OperatorID: operatorID}
	sessionStore, failure := pgstorage.NewTelegramSessionStore(database, "current", []byte("01234567890123456789012345678901"))
	if failure != nil {
		t.Fatalf("create Telegram session store: %v", failure)
	}

	first, failure := store.BeginOrResume(context, actor, "+12025550102", time.Now().UTC().Add(time.Hour))
	if failure != nil {
		t.Fatalf("begin first authentication: %v", failure)
	}
	firstID := operatoraccount.Identity(first.Account.ID)
	profile := applicationoperatoraccountauth.Profile{UserID: 88001, Username: "first_user", FirstName: "First", LastName: "Account"}
	if _, failure = store.Finalize(context, actor, operatoraccount.Identity(uuid.New()), first.Account.Version, profile); !errors.Is(failure, applicationoperatoraccountauth.ErrAccountNotFound) {
		t.Fatalf("foreign/random finalization error = %v, want account not found", failure)
	}
	if _, failure = store.Finalize(context, actor, firstID, first.Account.Version, profile); !errors.Is(failure, applicationoperatoraccountauth.ErrAccountStateConflict) {
		t.Fatalf("finalization without session error = %v, want state conflict", failure)
	}
	if failure = sessionStore.Store(context, telegram.SessionScope{OperatorID: operatorID, AccountID: first.Account.ID}, []byte("encrypted first session")); failure != nil {
		t.Fatalf("store first authentication session: %v", failure)
	}
	finalized, failure := store.Finalize(context, actor, firstID, first.Account.Version, profile)
	if failure != nil {
		t.Fatalf("finalize first authentication: %v", failure)
	}
	if finalized.Status != operatoraccount.StatusActive || finalized.Version != first.Account.Version+1 || finalized.UserID != profile.UserID || finalized.FirstName != profile.FirstName {
		t.Fatalf("finalized account = %+v, want active identity/profile at version %d", finalized, first.Account.Version+1)
	}
	duplicate, failure := store.Finalize(context, actor, firstID, first.Account.Version, profile)
	if failure != nil {
		t.Fatalf("duplicate finalization: %v", failure)
	}
	if duplicate != finalized {
		t.Fatalf("duplicate finalization = %+v, want existing account %+v", duplicate, finalized)
	}
	activeBegin, failure := store.BeginOrResume(context, actor, "+12025550102", time.Now().UTC().Add(time.Hour))
	if failure != nil {
		t.Fatalf("begin already-active account: %v", failure)
	}
	if activeBegin.Outcome != applicationoperatoraccountauth.BeginAlreadyActive || activeBegin.Account.Status != operatoraccount.StatusActive || !activeBegin.AuthExpiresAt.IsZero() {
		t.Fatalf("already-active result = outcome %q status %q expiry %s, want already_active/active/zero", activeBegin.Outcome, activeBegin.Account.Status, activeBegin.AuthExpiresAt)
	}
	if failure := activeBegin.Validate(); failure != nil {
		t.Fatalf("validate already-active admission result: %v", failure)
	}

	expired, failure := store.BeginOrResume(context, actor, "+12025550104", time.Now().UTC().Add(-time.Hour))
	if failure != nil {
		t.Fatalf("begin expired authentication: %v", failure)
	}
	expiredID := operatoraccount.Identity(expired.Account.ID)
	if failure = sessionStore.Store(context, telegram.SessionScope{OperatorID: operatorID, AccountID: expired.Account.ID}, []byte("expired session")); failure != nil {
		t.Fatalf("store expired authentication session: %v", failure)
	}
	if _, failure = store.Finalize(context, actor, expiredID, expired.Account.Version, applicationoperatoraccountauth.Profile{UserID: 88002, FirstName: "Expired"}); !errors.Is(failure, applicationoperatoraccountauth.ErrAccountVersionConflict) {
		t.Fatalf("expired finalization error = %v, want version conflict", failure)
	}
	var expiredStatus string
	if failure = database.QueryRow(context, `SELECT status::text FROM operator_accounts WHERE id = $1`, expired.Account.ID).Scan(&expiredStatus); failure != nil {
		t.Fatalf("read expired account status: %v", failure)
	}
	if expiredStatus != string(operatoraccount.StatusAuthenticating) {
		t.Fatalf("expired account status = %q, want authenticating", expiredStatus)
	}

	second, failure := store.BeginOrResume(context, actor, "+12025550103", time.Now().UTC().Add(time.Hour))
	if failure != nil {
		t.Fatalf("begin second authentication: %v", failure)
	}
	secondID := operatoraccount.Identity(second.Account.ID)
	abortVersion, failure := store.BeginAbort(context, actor, secondID, second.Account.Version)
	if failure != nil || abortVersion != second.Account.Version+1 {
		t.Fatalf("begin abort = version %d error %v, want version %d", abortVersion, failure, second.Account.Version+1)
	}
	retryAbortVersion, failure := store.BeginAbort(context, actor, secondID, second.Account.Version)
	if failure != nil || retryAbortVersion != abortVersion {
		t.Fatalf("retry begin abort = version %d error %v, want version %d", retryAbortVersion, failure, abortVersion)
	}
	orphans, failure := store.ListOrphanAuthenticationLifecycles(context)
	if failure != nil {
		t.Fatalf("list orphan disconnecting authentication: %v", failure)
	}
	foundOrphanDisconnecting := false
	for _, orphan := range orphans {
		if orphan.AccountID.UUID() == second.Account.ID {
			foundOrphanDisconnecting = orphan.Status == operatoraccount.StatusDisconnecting && orphan.Version == abortVersion
			break
		}
	}
	if !foundOrphanDisconnecting {
		t.Fatalf("orphan disconnecting list = %+v, want account %s disconnecting version %d", orphans, second.Account.ID, abortVersion)
	}
	if failure = sessionStore.Store(context, telegram.SessionScope{OperatorID: operatorID, AccountID: second.Account.ID}, []byte("late session")); !errors.Is(failure, telegram.ErrSessionUnauthorized) {
		t.Fatalf("late session resurrection error = %v, want unauthorized", failure)
	}
	if failure = store.CompleteAbort(context, actor, secondID, abortVersion); failure != nil {
		t.Fatalf("complete abort: %v", failure)
	}
	if failure = store.CompleteAbort(context, actor, secondID, abortVersion); failure != nil {
		t.Fatalf("duplicate complete abort: %v", failure)
	}
	var (
		status      string
		sessionRows int
	)
	if failure = database.QueryRow(context, `SELECT status::text FROM operator_accounts WHERE id = $1`, second.Account.ID).Scan(&status); failure != nil {
		t.Fatalf("read aborted account status: %v", failure)
	}
	if failure = database.QueryRow(context, `SELECT count(*) FROM sessions WHERE account_id = $1`, second.Account.ID).Scan(&sessionRows); failure != nil {
		t.Fatalf("count aborted sessions: %v", failure)
	}
	if status != string(operatoraccount.StatusDisconnected) || sessionRows != 0 {
		t.Fatalf("aborted account status/session count = %q/%d, want disconnected/0", status, sessionRows)
	}
}

var errInjectedCommitResponse = errors.New("injected commit response failure")

type commitResponseErrorDatabase struct {
	database
	failNextCommit atomic.Bool
}

func (database *commitResponseErrorDatabase) Begin(context context.Context) (pgx.Tx, error) {
	transaction, failure := database.database.Begin(context)
	if failure != nil {
		return nil, failure
	}
	return &commitResponseErrorTransaction{Tx: transaction, database: database}, nil
}

type commitResponseErrorTransaction struct {
	pgx.Tx
	database *commitResponseErrorDatabase
}

func (transaction *commitResponseErrorTransaction) Commit(context context.Context) error {
	if failure := transaction.Tx.Commit(context); failure != nil {
		return failure
	}
	if transaction.database.failNextCommit.CompareAndSwap(true, false) {
		return errInjectedCommitResponse
	}
	return nil
}
