package account_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/account"
)

type targetLookup struct {
	requests []telegram.PeerLookupRequest
}

func (lookup *targetLookup) Lookup(
	_ context.Context,
	request telegram.PeerLookupRequest,
) (telegram.PeerProjection, error) {
	lookup.requests = append(lookup.requests, request)
	return telegram.PeerProjection{
		Type: telegram.PeerTypeChat,
		ID:   1,
	}, nil
}

func TestNewTargetsBindsResolversToRoutes(t *testing.T) {
	ctx := context.Background()

	lookup := &targetLookup{}
	provider := account.NewTargets(lookup)
	firstAccount := uuid.New()
	secondAccount := uuid.New()
	recipientID := uuid.New()

	firstTarget, failure := provider.Targets(application.Routing(firstAccount)).Target(
		ctx,
		recipient.Identity(recipientID),
	)
	if failure != nil {
		t.Fatalf("resolve first route target: %v", failure)
	}
	secondTarget, failure := provider.Targets(application.Routing(secondAccount)).Target(
		ctx,
		recipient.Identity(recipientID),
	)
	if failure != nil {
		t.Fatalf("resolve second route target: %v", failure)
	}
	if firstTarget == nil || secondTarget == nil {
		t.Fatal("expected targets for both routes")
	}

	if len(lookup.requests) != 2 {
		t.Fatalf("expected two lookup requests, got %d", len(lookup.requests))
	}
	if lookup.requests[0].AccountID != firstAccount {
		t.Fatalf("expected first account %s, got %s", firstAccount, lookup.requests[0].AccountID)
	}
	if lookup.requests[1].AccountID != secondAccount {
		t.Fatalf("expected second account %s, got %s", secondAccount, lookup.requests[1].AccountID)
	}
	if lookup.requests[0].RecipientID != recipientID || lookup.requests[1].RecipientID != recipientID {
		t.Fatalf("expected recipient %s in both requests, got %s and %s", recipientID, lookup.requests[0].RecipientID, lookup.requests[1].RecipientID)
	}
}

func TestTargetsGuardsZeroRoute(t *testing.T) {
	provider := account.NewTargets(&targetLookup{})

	assertPanics(t, func() {
		_ = provider.Targets(application.Routing(uuid.Nil))
	})
}

func assertPanics(t *testing.T, call func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	call()
}
