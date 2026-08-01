package operatoraccountauth

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

type recoveryPersistence struct {
	servicePersistence
	targets       []AuthTarget
	disconnecting []AuthTarget
}

func (p *recoveryPersistence) ListOrphanAuthenticationLifecycles(context.Context) ([]AuthTarget, error) {
	targets := append([]AuthTarget(nil), p.targets...)
	return append(targets, p.disconnecting...), nil
}

func TestStartupRecoveryUsesStrictAbortProtocolForEveryOrphan(t *testing.T) {
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	targets := []AuthTarget{
		{Actor: actor, AccountID: operatoraccount.Identity(uuid.New()), Status: operatoraccount.StatusAuthenticating, Version: 4},
		{Actor: actor, AccountID: operatoraccount.Identity(uuid.New()), Status: operatoraccount.StatusAuthenticating, Version: 7},
	}
	persistence := &recoveryPersistence{targets: targets}
	stopper := &serviceStopper{}
	service := NewService(persistence, nil, stopper)
	if err := service.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if persistence.beginAborts != len(targets) || persistence.completeAborts != len(targets) {
		t.Fatalf("recovery abort calls: begin=%d complete=%d", persistence.beginAborts, persistence.completeAborts)
	}
	if len(stopper.accounts) != len(targets) {
		t.Fatalf("runtime stop calls = %d, want %d", len(stopper.accounts), len(targets))
	}
}

func TestStartupRecoveryDoesNotCompleteWhenOneRuntimeStopFails(t *testing.T) {
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	persistence := &recoveryPersistence{targets: []AuthTarget{{
		Actor: actor, AccountID: operatoraccount.Identity(uuid.New()), Status: operatoraccount.StatusAuthenticating, Version: 2,
	}}}
	stopper := &serviceStopper{err: errors.New("stop failed")}
	service := NewService(persistence, nil, stopper)
	if err := service.Recover(context.Background()); !errors.Is(err, ErrStartupRecovery) {
		t.Fatalf("recovery error = %v, want startup recovery error", err)
	}
	if persistence.completeAborts != 0 {
		t.Fatalf("complete abort calls = %d, want zero", persistence.completeAborts)
	}
}

func TestStartupRecoveryCompletesAlreadyDisconnectingCandidates(t *testing.T) {
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	target := AuthTarget{Actor: actor, AccountID: operatoraccount.Identity(uuid.New()), Status: operatoraccount.StatusDisconnecting, Version: 9}
	persistence := &recoveryPersistence{disconnecting: []AuthTarget{target}}
	stopper := &serviceStopper{}
	service := NewService(persistence, nil, stopper)
	if err := service.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if persistence.beginAborts != 0 || persistence.completeAborts != 1 || len(stopper.accounts) != 1 {
		t.Fatalf("already-disconnecting recovery calls: begin=%d stop=%d complete=%d", persistence.beginAborts, len(stopper.accounts), persistence.completeAborts)
	}
	if stopper.accounts[0].Version != target.Version {
		t.Fatalf("disconnecting StopAccount version = %d, want %d", stopper.accounts[0].Version, target.Version)
	}
}
