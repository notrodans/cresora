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

func TestForceForgetOperatorAccountStopsExactTargetBeforePersisting(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusDisconnecting, 7, operatoraccount.NoFailure, 4242, true)
	persistence := newForceForgetPersistence(actor, account)
	runtime := &localStopperStub{onStop: func() {
		if len(persistence.writes) != 0 {
			t.Fatal("persistence write occurred before local runtime stop returned")
		}
	}}
	service := NewForceForgetOperatorAccountWithTimeout(persistence, runtime, time.Second)
	key := uuid.New()

	result, err := service.Execute(context.Background(), ForceForgetCommand{
		Actor:           actor,
		AccountID:       accountID,
		ExpectedVersion: account.Version(),
		Acknowledged:    true,
		IdempotencyKey:  key,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outcome != ForceForgetLocallyForgotten {
		t.Fatalf("Execute() outcome = %q, want %q", result.Outcome, ForceForgetLocallyForgotten)
	}
	if len(runtime.calls) != 1 {
		t.Fatalf("StopAccount() calls = %d, want 1", len(runtime.calls))
	}
	wantTarget := RuntimeTarget{Actor: actor, AccountID: accountID, Status: operatoraccount.StatusDisconnecting, Version: 7}
	if runtime.calls[0] != wantTarget {
		t.Fatalf("StopAccount() target = %#v, want %#v", runtime.calls[0], wantTarget)
	}
	if !runtime.sawDeadline {
		t.Fatal("StopAccount() context had no command-owned deadline")
	}
	if len(persistence.writes) != 1 {
		t.Fatalf("force-forget persistence writes = %d, want 1", len(persistence.writes))
	}
	stored := persistence.account(actor, accountID)
	if stored.Status() != operatoraccount.StatusDisconnected || stored.Version() != 8 || stored.RemoteLogoutRequired() {
		t.Fatalf("stored account = status %q version %d remote=%t, want disconnected version 8 remote=false", stored.Status(), stored.Version(), stored.RemoteLogoutRequired())
	}
	if stored.ID() != account.ID() || stored.TelegramUserID() != account.TelegramUserID() {
		t.Fatalf("stored identity = account %s Telegram user %d, want account %s Telegram user %d", stored.ID().UUID(), stored.TelegramUserID(), account.ID().UUID(), account.TelegramUserID())
	}
}

func TestForceForgetOperatorAccountRejectsIneligibleCommandsWithoutChanges(t *testing.T) {
	actor := testActor()
	tests := []struct {
		name      string
		account   operatoraccount.Account
		command   func(operatoraccount.Account) ForceForgetCommand
		wantError error
	}{
		{
			name:    "missing acknowledgement",
			account: restoreAccount(t, testAccountID(t, "ack"), operatoraccount.StatusDisconnecting, 3, operatoraccount.NoFailure, 1, true),
			command: func(account operatoraccount.Account) ForceForgetCommand {
				return forceForgetCommand(actor, account, false, account.Version())
			},
			wantError: ErrInvalidInput,
		},
		{
			name:    "stale version",
			account: restoreAccount(t, testAccountID(t, "stale"), operatoraccount.StatusDisconnecting, 3, operatoraccount.NoFailure, 1, true),
			command: func(account operatoraccount.Account) ForceForgetCommand {
				return forceForgetCommand(actor, account, true, account.Version()-1)
			},
			wantError: ErrAccountVersionConflict,
		},
		{
			name:    "active state",
			account: restoreAccount(t, testAccountID(t, "active"), operatoraccount.StatusActive, 3, operatoraccount.NoFailure, 1, false),
			command: func(account operatoraccount.Account) ForceForgetCommand {
				return forceForgetCommand(actor, account, true, account.Version())
			},
			wantError: ErrAccountStateConflict,
		},
		{
			name:    "local-only disconnecting state",
			account: restoreAccount(t, testAccountID(t, "local"), operatoraccount.StatusDisconnecting, 3, operatoraccount.NoFailure, 1, false),
			command: func(account operatoraccount.Account) ForceForgetCommand {
				return forceForgetCommand(actor, account, true, account.Version())
			},
			wantError: ErrAccountStateConflict,
		},
		{
			name:    "normal disconnected state",
			account: restoreAccount(t, testAccountID(t, "disconnected"), operatoraccount.StatusDisconnected, 4, operatoraccount.NoFailure, 1, false),
			command: func(account operatoraccount.Account) ForceForgetCommand {
				return forceForgetCommand(actor, account, true, account.Version())
			},
			wantError: ErrAccountStateConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			persistence := newForceForgetPersistence(actor, test.account)
			runtime := &localStopperStub{}
			_, err := NewForceForgetOperatorAccount(persistence, runtime).Execute(context.Background(), test.command(test.account))
			if !errors.Is(err, test.wantError) {
				t.Fatalf("Execute() error = %v, want %v", err, test.wantError)
			}
			if len(runtime.calls) != 0 || len(persistence.writes) != 0 {
				t.Fatalf("runtime calls = %d, persistence writes = %d, want both zero", len(runtime.calls), len(persistence.writes))
			}
			if stored := persistence.account(actor, test.account.ID()); stored != test.account {
				t.Fatalf("stored account changed from %#v to %#v", test.account, stored)
			}
		})
	}

	foreignAccount := restoreAccount(t, testAccountID(t, "foreign-account"), operatoraccount.StatusDisconnecting, 3, operatoraccount.NoFailure, 1, true)
	persistence := newForceForgetPersistence(actor, foreignAccount)
	foreignActor := application.Actor{OperatorID: uuid.New()}
	_, err := NewForceForgetOperatorAccount(persistence, &localStopperStub{}).Execute(context.Background(), forceForgetCommand(foreignActor, foreignAccount, true, foreignAccount.Version()))
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("foreign Execute() error = %v, want %v", err, ErrAccountNotFound)
	}
	if len(persistence.writes) != 0 {
		t.Fatalf("foreign persistence writes = %d, want 0", len(persistence.writes))
	}
}

func TestForceForgetOperatorAccountDoesNotPersistWhenLocalStopFails(t *testing.T) {
	actor := testActor()
	account := restoreAccount(t, testAccountID(t), operatoraccount.StatusDisconnecting, 9, operatoraccount.NoFailure, 7, true)
	persistence := newForceForgetPersistence(actor, account)
	stopFailure := errors.New("runtime drain failed")
	runtime := &localStopperStub{err: stopFailure}

	_, err := NewForceForgetOperatorAccount(persistence, runtime).Execute(context.Background(), forceForgetCommand(actor, account, true, account.Version()))
	if !errors.Is(err, stopFailure) {
		t.Fatalf("Execute() error = %v, want local stop failure", err)
	}
	if len(persistence.writes) != 0 {
		t.Fatalf("persistence writes = %d, want 0 after local stop failure", len(persistence.writes))
	}
	if stored := persistence.account(actor, account.ID()); stored != account {
		t.Fatalf("stored account = %#v, want unchanged %#v", stored, account)
	}
}

func TestForceForgetOperatorAccountRetriesByIdempotencyKeyWithoutAnotherApply(t *testing.T) {
	actor := testActor()
	account := restoreAccount(t, testAccountID(t), operatoraccount.StatusDisconnecting, 12, operatoraccount.NoFailure, 88, true)
	persistence := newForceForgetPersistence(actor, account)
	runtime := &localStopperStub{}
	service := NewForceForgetOperatorAccount(persistence, runtime)
	command := forceForgetCommand(actor, account, true, account.Version())

	first, err := service.Execute(context.Background(), command)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, err := service.Execute(context.Background(), command)
	if err != nil {
		t.Fatalf("retry Execute() error = %v", err)
	}
	if first.Outcome != ForceForgetLocallyForgotten || second.Outcome != ForceForgetAlreadyApplied {
		t.Fatalf("outcomes = %q, %q, want locally forgotten then already applied", first.Outcome, second.Outcome)
	}
	if len(runtime.calls) != 1 || len(persistence.writes) != 1 {
		t.Fatalf("runtime calls = %d, persistence writes = %d, want one of each", len(runtime.calls), len(persistence.writes))
	}
	if stored := persistence.account(actor, account.ID()); stored.Version() != account.Version()+1 {
		t.Fatalf("stored version = %d, want %d", stored.Version(), account.Version()+1)
	}
}

func TestForceForgetOperatorAccountPreservesReauthenticationEligibility(t *testing.T) {
	actor := testActor()
	account := restoreAccount(t, testAccountID(t), operatoraccount.StatusDisconnecting, 20, operatoraccount.NoFailure, 7007, true)
	persistence := newForceForgetPersistence(actor, account)
	result, err := NewForceForgetOperatorAccount(persistence, &localStopperStub{}).Execute(
		context.Background(),
		forceForgetCommand(actor, account, true, account.Version()),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	reattempt := result.Account
	if retryAccountID := reattempt.ID(); retryAccountID != account.ID() {
		t.Fatalf("reauthentication account ID = %s, want %s", retryAccountID.UUID(), account.ID().UUID())
	}
	if retryIdentity := reattempt.TelegramUserID(); retryIdentity != account.TelegramUserID() {
		t.Fatalf("reauthentication Telegram identity = %d, want %d", retryIdentity, account.TelegramUserID())
	}
	if err := reattempt.BeginAuthentication(time.Unix(500, 0)); err != nil {
		t.Fatalf("BeginAuthentication() after force forget error = %v", err)
	}
	if reattempt.Status() != operatoraccount.StatusAuthenticating {
		t.Fatalf("reauthentication status = %q, want %q", reattempt.Status(), operatoraccount.StatusAuthenticating)
	}
}

func forceForgetCommand(
	actor application.Actor,
	account operatoraccount.Account,
	acknowledged bool,
	expectedVersion operatoraccount.Version,
) ForceForgetCommand {
	return ForceForgetCommand{
		Actor:           actor,
		AccountID:       account.ID(),
		ExpectedVersion: expectedVersion,
		Acknowledged:    acknowledged,
		IdempotencyKey:  uuid.NewSHA1(uuid.Nil, []byte(account.ID().UUID().String())),
	}
}

type forceForgetPersistenceKey struct {
	actorID   uuid.UUID
	accountID operatoraccount.ID
}

type forceForgetPersistence struct {
	accounts     map[forceForgetPersistenceKey]operatoraccount.Account
	applied      map[uuid.UUID]bool
	writes       []operatoraccount.Account
	persistCalls int
}

func newForceForgetPersistence(actor application.Actor, account operatoraccount.Account) *forceForgetPersistence {
	persistence := &forceForgetPersistence{
		accounts: make(map[forceForgetPersistenceKey]operatoraccount.Account),
		applied:  make(map[uuid.UUID]bool),
	}
	persistence.accounts[forceForgetPersistenceKey{actorID: actor.OperatorID, accountID: account.ID()}] = account
	return persistence
}

func (persistence *forceForgetPersistence) LoadAccount(_ context.Context, actor application.Actor, accountID operatoraccount.ID) (operatoraccount.Account, error) {
	account, ok := persistence.accounts[forceForgetPersistenceKey{actorID: actor.OperatorID, accountID: accountID}]
	if !ok {
		return operatoraccount.Account{}, ErrAccountNotFound
	}
	return account, nil
}

func (persistence *forceForgetPersistence) ForceForgetAlreadyApplied(_ context.Context, actor application.Actor, accountID operatoraccount.ID, key uuid.UUID) (bool, error) {
	if _, ok := persistence.accounts[forceForgetPersistenceKey{actorID: actor.OperatorID, accountID: accountID}]; !ok {
		return false, ErrAccountNotFound
	}
	return persistence.applied[key], nil
}

func (persistence *forceForgetPersistence) PersistForceForget(_ context.Context, actor application.Actor, account operatoraccount.Account, _ operatoraccount.Version, key uuid.UUID) (bool, error) {
	persistence.persistCalls++
	if persistence.applied[key] {
		return true, nil
	}
	persistence.writes = append(persistence.writes, account)
	persistence.accounts[forceForgetPersistenceKey{actorID: actor.OperatorID, accountID: account.ID()}] = account
	persistence.applied[key] = true
	return false, nil
}

func (persistence *forceForgetPersistence) account(actor application.Actor, accountID operatoraccount.ID) operatoraccount.Account {
	return persistence.accounts[forceForgetPersistenceKey{actorID: actor.OperatorID, accountID: accountID}]
}

type localStopperStub struct {
	calls       []RuntimeTarget
	err         error
	sawDeadline bool
	onStop      func()
}

func (runtime *localStopperStub) StopAccount(ctx context.Context, target RuntimeTarget) error {
	runtime.calls = append(runtime.calls, target)
	_, runtime.sawDeadline = ctx.Deadline()
	if runtime.onStop != nil {
		runtime.onStop()
	}
	return runtime.err
}
