package operatoraccounts

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func TestRevokeOutcomeRejectsMalformedAdapterResults(t *testing.T) {
	tests := []struct {
		name    string
		outcome RevokeOutcome
	}{
		{name: "zero result", outcome: RevokeOutcome{}},
		{name: "failed without failure", outcome: RevokeOutcome{kind: revokeOutcomeFailed}},
		{name: "invalid failure class", outcome: RevokeOutcome{
			kind:    revokeOutcomeFailed,
			failure: &RemoteLogoutFailure{},
		}},
		{name: "success with failure", outcome: RevokeOutcome{
			kind:    revokeOutcomeSucceeded,
			failure: &RemoteLogoutFailure{},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !errors.Is(test.outcome.Validate(), ErrInvalidRuntimeOutcome) {
				t.Fatalf("RevokeOutcome.Validate() = %v, want ErrInvalidRuntimeOutcome", test.outcome.Validate())
			}
			if test.outcome.Converged() {
				t.Fatal("malformed outcome reported convergence")
			}
		})
	}
}

func TestNewRevokeFailureRejectsInvalidFailureAndCopiesValidFailure(t *testing.T) {
	if _, err := NewRevokeFailure(nil); !errors.Is(err, ErrInvalidRemoteLogoutFailure) {
		t.Fatalf("NewRevokeFailure(nil) error = %v, want ErrInvalidRemoteLogoutFailure", err)
	}
	if _, err := NewRevokeFailure(&RemoteLogoutFailure{}); !errors.Is(err, ErrInvalidRemoteLogoutFailure) {
		t.Fatalf("NewRevokeFailure(zero) error = %v, want ErrInvalidRemoteLogoutFailure", err)
	}

	failure, err := NewRemoteLogoutFailure(RemoteLogoutFailurePermanent, 0)
	if err != nil {
		t.Fatalf("NewRemoteLogoutFailure() error = %v", err)
	}
	outcome, err := NewRevokeFailure(failure)
	if err != nil {
		t.Fatalf("NewRevokeFailure() error = %v", err)
	}
	returned, ok := outcome.Failure()
	if !ok || returned == failure {
		t.Fatalf("RevokeOutcome.Failure() = (%#v, %t), want a copied failure", returned, ok)
	}
	if returned.Kind() != RemoteLogoutFailurePermanent {
		t.Fatalf("copied failure kind = %q, want %q", returned.Kind(), RemoteLogoutFailurePermanent)
	}
}

func TestServiceDisconnectTreatsMalformedRuntimeResultAsSafeProtocolFailure(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusActive, 5, operatoraccount.NoFailure, 1, false)
	persistence := newDisconnectPersistence(actor, account)
	runtime := &revokeRuntime{outcomes: map[operatoraccount.ID][]RevokeOutcome{
		accountID: {RevokeOutcome{}},
	}}

	result, err := NewService(persistence, runtime).Disconnect(nil, actor, accountID)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Disconnect(nil) error = %v, want ErrInvalidInput", err)
	}
	_ = result

	result, err = NewService(persistence, runtime).Disconnect(context.Background(), actor, accountID)
	if !errors.Is(err, ErrInvalidRuntimeOutcome) {
		t.Fatalf("Disconnect() error = %v, want ErrInvalidRuntimeOutcome", err)
	}
	if errors.Is(err, ErrRemoteLogoutNotConverged) || strings.Contains(err.Error(), "provider") {
		t.Fatalf("Disconnect() leaked remote/provider classification: %v", err)
	}
	if result.Outcome != DisconnectPending || persistence.account(actor, accountID).Status() != operatoraccount.StatusDisconnecting {
		t.Fatalf("result=%#v stored=%q, want pending durable intent", result, persistence.account(actor, accountID).Status())
	}
}

func TestServiceRecoveryTreatsMalformedRuntimeResultAsStartupFatal(t *testing.T) {
	actor := testActor()
	accountID := testAccountID(t)
	account := restoreAccount(t, accountID, operatoraccount.StatusDisconnecting, 7, operatoraccount.NoFailure, 1, true)
	persistence := newDisconnectPersistence(actor, account)
	persistence.intents = []RuntimeTarget{remoteTarget(actor, account)}
	runtime := &revokeRuntime{outcomes: map[operatoraccount.ID][]RevokeOutcome{
		accountID: {RevokeOutcome{}},
	}}

	result, err := NewService(persistence, runtime).Recover(context.Background())
	if !errors.Is(err, ErrStartupRecovery) || !errors.Is(err, ErrInvalidRuntimeOutcome) {
		t.Fatalf("Recover() error = %v, want startup fatal invalid outcome", err)
	}
	if result.Attempted != 1 || result.Pending != 0 || result.Completed != 0 {
		t.Fatalf("RecoveryResult = %#v, want one attempted fatal result", result)
	}
	if strings.Contains(err.Error(), "provider") {
		t.Fatalf("Recover() leaked provider data: %v", err)
	}
}

func TestSafeFailureDoesNotWrapProviderLikeErrors(t *testing.T) {
	failure, err := NewRemoteLogoutFailure(RemoteLogoutFailureTransient, 0)
	if err != nil {
		t.Fatalf("NewRemoteLogoutFailure() error = %v", err)
	}
	wrapped := nonConvergedRemoteFailure(failure)
	providerFailure := &providerLikeError{}
	if strings.Contains(wrapped.Error(), "provider secret") {
		t.Fatalf("safe error contains provider data: %v", wrapped)
	}
	if errors.Is(wrapped, providerFailure) {
		t.Fatal("safe error matched provider-like error")
	}
	var leaked *providerLikeError
	if errors.As(wrapped, &leaked) {
		t.Fatal("safe error exposed provider-like error through errors.As")
	}
	var safe *RemoteLogoutFailure
	if !errors.As(wrapped, &safe) || safe.Kind() != RemoteLogoutFailureTransient {
		t.Fatalf("safe error did not preserve bounded failure: %v", wrapped)
	}
}

type providerLikeError struct{}

func (*providerLikeError) Error() string { return "provider secret response" }
