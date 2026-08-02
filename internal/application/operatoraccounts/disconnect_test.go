package operatoraccounts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func TestServiceDisconnectsActiveAndReauthenticationRequiredAccounts(t *testing.T) {
	tests := []struct {
		name   string
		status operatoraccount.Status
		code   operatoraccount.FailureCode
	}{
		{name: "active", status: operatoraccount.StatusActive},
		{name: "reauthentication required", status: operatoraccount.StatusReauthRequired, code: operatoraccount.FailureCodeSessionInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actor := testActor()
			accountID := testAccountID(t)
			account := restoreAccount(t, accountID, test.status, 11, test.code, 4242, false)
			persistence := newDisconnectPersistence(actor, account)
			runtime := &revokeRuntime{}
			service := NewService(persistence, runtime)

			result, err := service.Disconnect(context.Background(), actor, accountID)
			if err != nil {
				t.Fatalf("Disconnect() error = %v", err)
			}
			if result.Outcome != DisconnectCompleted {
				t.Fatalf("Disconnect() outcome = %q, want %q", result.Outcome, DisconnectCompleted)
			}
			stored := persistence.account(actor, accountID)
			if stored.Status() != operatoraccount.StatusDisconnected {
				t.Fatalf("stored status = %q, want %q", stored.Status(), operatoraccount.StatusDisconnected)
			}
			if stored.Version() != 13 {
				t.Fatalf("stored version = %d, want 13", stored.Version())
			}
			if len(runtime.calls) != 1 {
				t.Fatalf("runtime calls = %d, want 1", len(runtime.calls))
			}
			target := runtime.calls[0]
			if target.Status != operatoraccount.StatusDisconnecting || target.Version != 12 {
				t.Fatalf("runtime target = %#v, want disconnecting version 12", target)
			}
			if len(persistence.writes) != 2 || persistence.writes[0].expected != 11 || persistence.writes[1].expected != 12 {
				t.Fatalf("lifecycle writes = %#v, want expected versions 11 and 12", persistence.writes)
			}
		})
	}
}

func TestServiceDisconnectBeginConflictDoesNotCallRuntime(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	persistence := newDisconnectPersistence(actor, restoreAccount(t, accountID, operatoraccount.StatusActive, 3, operatoraccount.NoFailure, 7, false))
	persistence.persistFailures = []error{ErrAccountVersionConflict}
	runtime := &revokeRuntime{}

	result, err := NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
	if !errors.Is(err, ErrAccountVersionConflict) {
		t.Fatalf("Disconnect() error = %v, want ErrAccountVersionConflict", err)
	}
	if result != (DisconnectResult{}) {
		t.Fatalf("Disconnect() result = %#v, want zero result", result)
	}
	if len(runtime.calls) != 0 {
		t.Fatalf("runtime calls = %d, want 0 after begin conflict", len(runtime.calls))
	}
	if stored := persistence.account(actor, accountID); stored.Status() != operatoraccount.StatusActive {
		t.Fatalf("stored status = %q, want active", stored.Status())
	}
}

func TestServiceDisconnectResumesRemoteIntent(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusDisconnecting, 8, operatoraccount.NoFailure, 17, true)
	persistence := newDisconnectPersistence(actor, account)
	runtime := &revokeRuntime{}

	result, err := NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
	if err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
	if result.Outcome != DisconnectCompleted {
		t.Fatalf("Disconnect() outcome = %q, want %q", result.Outcome, DisconnectCompleted)
	}
	if len(runtime.calls) != 1 || runtime.calls[0].Version != 8 || runtime.calls[0].Status != operatoraccount.StatusDisconnecting {
		t.Fatalf("runtime calls = %#v, want one persisted disconnecting version 8 target", runtime.calls)
	}
	if len(persistence.writes) != 1 || persistence.writes[0].expected != 8 {
		t.Fatalf("lifecycle writes = %#v, want only completion at version 8", persistence.writes)
	}
	if stored := persistence.account(actor, accountID); stored.Status() != operatoraccount.StatusDisconnected {
		t.Fatalf("stored status = %q, want disconnected", stored.Status())
	}
}

func TestServiceDisconnectDisconnectedIsIdempotent(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	persistence := newDisconnectPersistence(actor, restoreAccount(t, accountID, operatoraccount.StatusDisconnected, 5, operatoraccount.NoFailure, 99, false))
	runtime := &revokeRuntime{}

	result, err := NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
	if err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
	if result.Outcome != DisconnectAlreadyDisconnected {
		t.Fatalf("Disconnect() outcome = %q, want %q", result.Outcome, DisconnectAlreadyDisconnected)
	}
	if len(runtime.calls) != 0 || len(persistence.writes) != 0 {
		t.Fatalf("runtime calls = %d, lifecycle writes = %d, want both zero", len(runtime.calls), len(persistence.writes))
	}
}

func TestServiceDisconnectRejectsAuthenticatingAndLocalOnlyDisconnecting(t *testing.T) {
	tests := []struct {
		name    string
		status  operatoraccount.Status
		remote  bool
		expires bool
	}{
		{name: "authenticating", status: operatoraccount.StatusAuthenticating, expires: true},
		{name: "local only", status: operatoraccount.StatusDisconnecting},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actor := testActor()
			accountID := testAccountID(t)
			var expiry time.Time
			if test.expires {
				expiry = time.Unix(100, 0)
			}
			account := restoreAccount(t, accountID, test.status, 4, operatoraccount.NoFailure, 33, test.remote, expiry)
			persistence := newDisconnectPersistence(actor, account)
			runtime := &revokeRuntime{}

			_, err := NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
			if !errors.Is(err, ErrAccountStateConflict) {
				t.Fatalf("Disconnect() error = %v, want ErrAccountStateConflict", err)
			}
			if len(runtime.calls) != 0 || len(persistence.writes) != 0 {
				t.Fatalf("runtime calls = %d, lifecycle writes = %d, want both zero", len(runtime.calls), len(persistence.writes))
			}
		})
	}
}

func TestServiceDisconnectRemoteFailureRetainsIntentWithoutCompletion(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	persistence := newDisconnectPersistence(actor, restoreAccount(t, accountID, operatoraccount.StatusActive, 6, operatoraccount.NoFailure, 18, false))
	runtime := &revokeRuntime{outcomes: map[operatoraccount.ID][]RevokeOutcome{
		accountID: {revokeFailureOutcome(t, RemoteLogoutFailureTransient, 0)},
	}}

	result, err := NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
	if !errors.Is(err, ErrRemoteLogoutNotConverged) || !errors.Is(err, ErrRemoteLogoutTransient) {
		t.Fatalf("Disconnect() error = %v, want non-converged retryable error", err)
	}
	if result.Outcome != DisconnectPending {
		t.Fatalf("Disconnect() outcome = %q, want %q", result.Outcome, DisconnectPending)
	}
	stored := persistence.account(actor, accountID)
	if stored.Status() != operatoraccount.StatusDisconnecting || !stored.RemoteLogoutRequired() || stored.Version() != 7 {
		t.Fatalf("stored intent = status %q version %d remote=%t, want disconnecting version 7 remote=true", stored.Status(), stored.Version(), stored.RemoteLogoutRequired())
	}
	if len(persistence.writes) != 1 || len(runtime.calls) != 1 {
		t.Fatalf("lifecycle writes = %d, runtime calls = %d, want one intent write and one runtime call", len(persistence.writes), len(runtime.calls))
	}
}

func TestServiceDisconnectCompletionErrorRetainsIntent(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	persistence := newDisconnectPersistence(actor, restoreAccount(t, accountID, operatoraccount.StatusActive, 12, operatoraccount.NoFailure, 23, false))
	completionErr := errors.New("completion unavailable")
	persistence.persistFailures = []error{nil, completionErr}

	result, err := NewService(persistence, &revokeRuntime{}).Disconnect(context.Background(), actor, accountID)
	if !errors.Is(err, completionErr) {
		t.Fatalf("Disconnect() error = %v, want completion error", err)
	}
	if result.Outcome != DisconnectPending {
		t.Fatalf("Disconnect() outcome = %q, want %q", result.Outcome, DisconnectPending)
	}
	stored := persistence.account(actor, accountID)
	if stored.Status() != operatoraccount.StatusDisconnecting || !stored.RemoteLogoutRequired() {
		t.Fatalf("stored account = status %q remote=%t, want retained remote intent", stored.Status(), stored.RemoteLogoutRequired())
	}
	if len(persistence.writes) != 2 {
		t.Fatalf("lifecycle writes = %d, want begin and completion attempts", len(persistence.writes))
	}
}

func TestServiceRecoveryProcessesOnlyRemoteIntentsAndCompletesSemanticConvergence(t *testing.T) {
	actor := testActor()
	remoteID := testAccountID(t)
	localID := testAccountID(t, "local")
	persistence := newDisconnectPersistence(actor,
		restoreAccount(t, remoteID, operatoraccount.StatusDisconnecting, 21, operatoraccount.NoFailure, 1, true),
	)
	local := restoreAccount(t, localID, operatoraccount.StatusDisconnecting, 9, operatoraccount.NoFailure, 2, false)
	persistence.putAccount(actor, local)
	persistence.intents = []RuntimeTarget{remoteTarget(actor, persistence.account(actor, remoteID))}
	runtime := &revokeRuntime{outcomes: map[operatoraccount.ID][]RevokeOutcome{
		remoteID: {RevokeAlreadyComplete()},
	}}

	result, err := NewService(persistence, runtime).Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if result.Attempted != 1 || result.Completed != 1 || result.Pending != 0 {
		t.Fatalf("RecoveryResult = %#v, want one completed attempt", result)
	}
	if len(runtime.calls) != 1 || runtime.calls[0].AccountID != remoteID {
		t.Fatalf("runtime calls = %#v, want one call for remote intent only", runtime.calls)
	}
	if stored := persistence.account(actor, remoteID); stored.Status() != operatoraccount.StatusDisconnected {
		t.Fatalf("remote stored status = %q, want disconnected", stored.Status())
	}
	if stored := persistence.account(actor, localID); stored.Status() != operatoraccount.StatusDisconnecting {
		t.Fatalf("local-only stored status = %q, want unchanged disconnecting", stored.Status())
	}
}

func TestServiceRecoveryContinuesAfterFailureAndCallsRuntimeAtMostOncePerAccount(t *testing.T) {
	actor := testActor()
	firstID := testAccountID(t)
	secondID := testAccountID(t, "second")
	persistence := newDisconnectPersistence(actor,
		restoreAccount(t, firstID, operatoraccount.StatusDisconnecting, 30, operatoraccount.NoFailure, 1, true),
	)
	persistence.putAccount(actor, restoreAccount(t, secondID, operatoraccount.StatusDisconnecting, 40, operatoraccount.NoFailure, 2, true))
	persistence.intents = []RuntimeTarget{
		remoteTarget(actor, persistence.account(actor, firstID)),
		remoteTarget(actor, persistence.account(actor, firstID)),
		remoteTarget(actor, persistence.account(actor, secondID)),
	}
	pendingFailure := revokeFailureOutcome(t, RemoteLogoutFailureTransient, 0)
	runtime := &revokeRuntime{outcomes: map[operatoraccount.ID][]RevokeOutcome{
		firstID: {pendingFailure},
	}}

	result, err := NewService(persistence, runtime).Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover() error = %v, want nil for account-local non-convergence", err)
	}
	if result.Attempted != 2 || result.Completed != 1 || result.Pending != 1 {
		t.Fatalf("RecoveryResult = %#v, want two attempts, one completed, one pending", result)
	}
	if result.PendingByKind[RemoteLogoutFailureTransient] != 1 {
		t.Fatalf("pending classes = %#v, want one transient failure", result.PendingByKind)
	}
	if len(runtime.calls) != 2 {
		t.Fatalf("runtime calls = %d, want one per distinct account", len(runtime.calls))
	}
	if stored := persistence.account(actor, firstID); stored.Status() != operatoraccount.StatusDisconnecting {
		t.Fatalf("failed account status = %q, want retained disconnecting intent", stored.Status())
	}
	if stored := persistence.account(actor, secondID); stored.Status() != operatoraccount.StatusDisconnected {
		t.Fatalf("successful account status = %q, want disconnected", stored.Status())
	}
}

func TestServiceRecoverySkipsStaleTargetsBeforeRuntime(t *testing.T) {
	tests := []struct {
		name          string
		account       operatoraccount.Account
		listedVersion operatoraccount.Version
		listedStatus  operatoraccount.Status
	}{
		{
			name:          "disconnected",
			account:       restoreAccount(t, testAccountID(t, "disconnected"), operatoraccount.StatusDisconnected, 12, operatoraccount.NoFailure, 1, false),
			listedVersion: 11,
			listedStatus:  operatoraccount.StatusDisconnecting,
		},
		{
			name:          "version advanced",
			account:       restoreAccount(t, testAccountID(t, "advanced"), operatoraccount.StatusDisconnecting, 14, operatoraccount.NoFailure, 1, true),
			listedVersion: 13,
			listedStatus:  operatoraccount.StatusDisconnecting,
		},
		{
			name:          "remote intent cleared",
			account:       restoreAccount(t, testAccountID(t, "cleared"), operatoraccount.StatusDisconnecting, 15, operatoraccount.NoFailure, 1, false),
			listedVersion: 15,
			listedStatus:  operatoraccount.StatusDisconnecting,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actor := testActor()
			persistence := newDisconnectPersistence(actor, test.account)
			persistence.intents = []RuntimeTarget{{
				Actor:     actor,
				AccountID: test.account.ID(),
				Status:    test.listedStatus,
				Version:   test.listedVersion,
			}}
			runtime := &revokeRuntime{}

			result, err := NewService(persistence, runtime).Recover(context.Background())
			if err != nil {
				t.Fatalf("Recover() error = %v", err)
			}
			if result.Attempted != 0 || result.Skipped != 1 {
				t.Fatalf("RecoveryResult = %#v, want zero attempts and one skip", result)
			}
			if len(runtime.calls) != 0 {
				t.Fatalf("runtime calls = %d, want 0 for stale target", len(runtime.calls))
			}
		})
	}
}

func TestServiceRecoveryReloadsAfterRuntimeBeforeCompletion(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusDisconnecting, 20, operatoraccount.NoFailure, 1, true)
	persistence := newDisconnectPersistence(actor, account)
	persistence.intents = []RuntimeTarget{remoteTarget(actor, account)}
	runtime := &revokeRuntime{}
	runtime.onCall = func(target RuntimeTarget) {
		persistence.putAccount(actor, restoreAccount(t, target.AccountID, operatoraccount.StatusDisconnected, target.Version+1, operatoraccount.NoFailure, 1, false))
	}

	result, err := NewService(persistence, runtime).Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if result.Attempted != 1 || result.Completed != 1 || result.Skipped != 0 {
		t.Fatalf("RecoveryResult = %#v, want one completed attempt", result)
	}
	if len(persistence.writes) != 0 {
		t.Fatalf("lifecycle writes = %d, want 0 after another owner completed", len(persistence.writes))
	}
}

func TestServiceRecoveryDurableEnumerationAndLoadErrorsAreFatal(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusDisconnecting, 25, operatoraccount.NoFailure, 1, true)

	t.Run("list error", func(t *testing.T) {
		persistence := newDisconnectPersistence(actor, account)
		persistence.listErr = errors.New("database unavailable")
		result, err := NewService(persistence, &revokeRuntime{}).Recover(context.Background())
		if !errors.Is(err, ErrStartupRecovery) || !errors.Is(err, persistence.listErr) {
			t.Fatalf("Recover() error = %v, want startup recovery wrapping list error", err)
		}
		if result.Attempted != 0 || result.Pending != 0 {
			t.Fatalf("RecoveryResult = %#v, want no work after list failure", result)
		}
	})

	t.Run("load error", func(t *testing.T) {
		persistence := newDisconnectPersistence(actor, account)
		persistence.intents = []RuntimeTarget{remoteTarget(actor, account)}
		loadErr := errors.New("corrupt durable snapshot")
		persistence.loadFailures = []error{loadErr}
		runtime := &revokeRuntime{}
		result, err := NewService(persistence, runtime).Recover(context.Background())
		if !errors.Is(err, ErrStartupRecovery) || !errors.Is(err, loadErr) {
			t.Fatalf("Recover() error = %v, want startup recovery wrapping load error", err)
		}
		if result.Attempted != 0 || len(runtime.calls) != 0 {
			t.Fatalf("RecoveryResult = %#v, runtime calls = %d, want no runtime attempt", result, len(runtime.calls))
		}
	})
}

type lifecycleWrite struct {
	account  operatoraccount.Account
	expected operatoraccount.Version
}

type persistenceAccountKey struct {
	actorID   uuid.UUID
	accountID operatoraccount.ID
}

type disconnectPersistence struct {
	accounts        map[persistenceAccountKey]operatoraccount.Account
	intents         []RuntimeTarget
	persistFailures []error
	listErr         error
	loadFailures    []error
	writes          []lifecycleWrite
}

func newDisconnectPersistence(actor application.Actor, account operatoraccount.Account) *disconnectPersistence {
	persistence := &disconnectPersistence{accounts: make(map[persistenceAccountKey]operatoraccount.Account)}
	persistence.putAccount(actor, account)
	return persistence
}

func (persistence *disconnectPersistence) LoadAccount(_ context.Context, actor application.Actor, accountID operatoraccount.ID) (operatoraccount.Account, error) {
	if len(persistence.loadFailures) != 0 {
		failure := persistence.loadFailures[0]
		persistence.loadFailures = persistence.loadFailures[1:]
		if failure != nil {
			return operatoraccount.Account{}, failure
		}
	}
	account, ok := persistence.accounts[persistenceAccountKey{actorID: actor.OperatorID, accountID: accountID}]
	if !ok {
		return operatoraccount.Account{}, ErrAccountNotFound
	}
	return account, nil
}

func (persistence *disconnectPersistence) PersistLifecycle(_ context.Context, actor application.Actor, account operatoraccount.Account, expected operatoraccount.Version) error {
	persistence.writes = append(persistence.writes, lifecycleWrite{account: account, expected: expected})
	if len(persistence.persistFailures) != 0 {
		failure := persistence.persistFailures[0]
		persistence.persistFailures = persistence.persistFailures[1:]
		if failure != nil {
			return failure
		}
	}
	key := persistenceAccountKey{actorID: actor.OperatorID, accountID: account.ID()}
	current, ok := persistence.accounts[key]
	if !ok {
		return ErrAccountNotFound
	}
	if current.Version() != expected {
		return ErrAccountVersionConflict
	}
	persistence.accounts[key] = account
	return nil
}

func (persistence *disconnectPersistence) ListRemoteLogoutIntents(context.Context) ([]RuntimeTarget, error) {
	if persistence.listErr != nil {
		return nil, persistence.listErr
	}
	return append([]RuntimeTarget(nil), persistence.intents...), nil
}

func (persistence *disconnectPersistence) putAccount(actor application.Actor, account operatoraccount.Account) {
	persistence.accounts[persistenceAccountKey{actorID: actor.OperatorID, accountID: account.ID()}] = account
}

func (persistence *disconnectPersistence) account(actor application.Actor, accountID operatoraccount.ID) operatoraccount.Account {
	return persistence.accounts[persistenceAccountKey{actorID: actor.OperatorID, accountID: accountID}]
}

type revokeRuntime struct {
	calls    []RuntimeTarget
	outcomes map[operatoraccount.ID][]RevokeOutcome
	onCall   func(RuntimeTarget)
}

func (runtime *revokeRuntime) RevokeAndStop(_ context.Context, target RuntimeTarget) RevokeOutcome {
	runtime.calls = append(runtime.calls, target)
	if runtime.onCall != nil {
		runtime.onCall(target)
	}
	outcomes := runtime.outcomes[target.AccountID]
	if len(outcomes) == 0 {
		return RevokeSucceeded()
	}
	outcome := outcomes[0]
	runtime.outcomes[target.AccountID] = outcomes[1:]
	return outcome
}

func restoreAccount(
	t *testing.T,
	id operatoraccount.ID,
	status operatoraccount.Status,
	version operatoraccount.Version,
	code operatoraccount.FailureCode,
	telegramUserID int64,
	remoteLogoutRequired bool,
	expiresAt ...time.Time,
) operatoraccount.Account {
	t.Helper()
	var expiry time.Time
	if len(expiresAt) == 1 {
		expiry = expiresAt[0]
	}
	account, err := operatoraccount.Restore(id, status, version, code, telegramUserID, expiry, remoteLogoutRequired)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	return account
}

func remoteTarget(actor application.Actor, account operatoraccount.Account) RuntimeTarget {
	return RuntimeTarget{
		Actor:     actor,
		AccountID: account.ID(),
		Status:    account.Status(),
		Version:   account.Version(),
	}
}

func revokeFailureOutcome(t *testing.T, kind RemoteLogoutFailureKind, retryAfter time.Duration) RevokeOutcome {
	t.Helper()
	failure, err := NewRemoteLogoutFailure(kind, retryAfter)
	if err != nil {
		t.Fatalf("NewRemoteLogoutFailure() error = %v", err)
	}
	outcome, err := NewRevokeFailure(failure)
	if err != nil {
		t.Fatalf("NewRevokeFailure() error = %v", err)
	}
	return outcome
}

func testActor() application.Actor {
	return application.Actor{OperatorID: uuid.MustParse("00000000-0000-0000-0000-000000000001")}
}

func testAccountID(t *testing.T, suffix ...string) operatoraccount.ID {
	t.Helper()
	name := t.Name()
	if len(suffix) == 1 {
		name += ":" + suffix[0]
	}
	return operatoraccount.Identity(uuid.NewSHA1(uuid.Nil, []byte(name)))
}
