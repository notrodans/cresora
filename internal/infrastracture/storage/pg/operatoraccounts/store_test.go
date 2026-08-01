package operatoraccounts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func TestLifecyclePredecessorsMatchDomainTransitionGraph(t *testing.T) {
	tests := []struct {
		next         operatoraccount.Status
		predecessors string
	}{
		{operatoraccount.StatusAuthenticating, `'disconnected', 'reauth_required'`},
		{operatoraccount.StatusActive, `'authenticating'`},
		{operatoraccount.StatusReauthRequired, `'active'`},
		{operatoraccount.StatusDisconnected, `'disconnecting'`},
		{operatoraccount.StatusDisconnecting, `'authenticating', 'active', 'reauth_required'`},
	}
	for _, test := range tests {
		t.Run(string(test.next), func(t *testing.T) {
			predecessors, ok := lifecyclePredecessors(test.next)
			if !ok {
				t.Fatalf("lifecycle predecessors for %q not found", test.next)
			}
			if predecessors != test.predecessors {
				t.Fatalf("lifecycle predecessors for %q = %q, want %q", test.next, predecessors, test.predecessors)
			}
		})
	}
	if _, ok := lifecyclePredecessors(operatoraccount.Status("unknown")); ok {
		t.Fatal("unknown lifecycle status was accepted")
	}
}

func TestPersistLifecycleRejectsNonAdjacentVersionWithoutDatabaseWrite(t *testing.T) {
	store := &Store{}
	actor := application.Actor{OperatorID: uuid.New()}
	account := operatoraccount.New(operatoraccount.Identity(uuid.New()))
	if err := account.BeginAuthentication(testAuthenticationExpiry()); err != nil {
		t.Fatalf("begin authentication: %v", err)
	}

	err := store.PersistLifecycle(context.Background(), actor, account, account.Version())
	if !errors.Is(err, applicationoperatoraccounts.ErrAccountVersionConflict) {
		t.Fatalf("persist lifecycle error = %v, want %v", err, applicationoperatoraccounts.ErrAccountVersionConflict)
	}
}

func testAuthenticationExpiry() time.Time {
	return time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
}
