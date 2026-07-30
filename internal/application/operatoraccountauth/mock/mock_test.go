package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
)

func TestMockAuthenticationStateIsActorScoped(t *testing.T) {
	store := NewStore()
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	startQR := NewStartQR(store)
	refreshQR := NewRefreshQR(store)
	status := NewStatus(store)

	challenge, failure := startQR.Execute(context.Background(), actorA)
	if failure != nil {
		t.Fatalf("start actor A QR: %v", failure)
	}
	otherStatus, failure := status.Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("load actor B status: %v", failure)
	}
	if otherStatus.QRChallenge != nil {
		t.Fatal("actor B received actor A QR challenge")
	}
	if _, failure = refreshQR.Execute(context.Background(), actorB, challenge.RequestID); !errors.Is(failure, ErrChallengeNotFound) {
		t.Fatalf("cross-actor QR refresh: expected not found, got %v", failure)
	}
}

func TestMockFixtureAccountIDsAreActorScoped(t *testing.T) {
	store := NewStore()
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	status := NewStatus(store)

	statusA, failure := status.Execute(context.Background(), actorA)
	if failure != nil {
		t.Fatalf("load actor A status: %v", failure)
	}
	statusB, failure := status.Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("load actor B status: %v", failure)
	}
	if len(statusA.Accounts) != len(statusB.Accounts) {
		t.Fatalf("fixture account counts differ: A=%d B=%d", len(statusA.Accounts), len(statusB.Accounts))
	}
	for index := range statusA.Accounts {
		if statusA.Accounts[index].ID == statusB.Accounts[index].ID {
			t.Fatalf("fixture account %d has the same ID for distinct actors: %s", index, statusA.Accounts[index].ID)
		}
	}
}

func TestMockChallengeIdentitiesIncludeActor(t *testing.T) {
	store := NewStore()
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	startPhone := NewStartPhone(store)
	startQR := NewStartQR(store)

	phoneA, failure := startPhone.Execute(context.Background(), actorA, "+15551230001")
	if failure != nil {
		t.Fatalf("start actor A phone: %v", failure)
	}
	phoneB, failure := startPhone.Execute(context.Background(), actorB, "+15551230001")
	if failure != nil {
		t.Fatalf("start actor B phone: %v", failure)
	}
	if phoneA.RequestID == phoneB.RequestID {
		t.Fatal("phone challenge IDs collided across actors")
	}

	qrA, failure := startQR.Execute(context.Background(), actorA)
	if failure != nil {
		t.Fatalf("start actor A QR: %v", failure)
	}
	qrB, failure := startQR.Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("start actor B QR: %v", failure)
	}
	if qrA.RequestID == qrB.RequestID || qrA.URL == qrB.URL {
		t.Fatal("QR challenge identity or URL collided across actors")
	}
	if _, failure = NewVerifyPhone(store).Execute(context.Background(), actorB, phoneA.RequestID, MockPhoneCode); !errors.Is(failure, ErrChallengeNotFound) {
		t.Fatalf("cross-actor phone verify: expected not found, got %v", failure)
	}
	if _, failure = NewRefreshQR(store).Execute(context.Background(), actorB, qrA.RequestID); !errors.Is(failure, ErrChallengeNotFound) {
		t.Fatalf("cross-actor QR refresh: expected not found, got %v", failure)
	}
}

func TestMockForeignAndRandomChallengesHaveSameFailure(t *testing.T) {
	store := NewStore()
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	phone, failure := NewStartPhone(store).Execute(context.Background(), actorB, "+15551230001")
	if failure != nil {
		t.Fatalf("start foreign phone: %v", failure)
	}
	verify := NewVerifyPhone(store)
	_, foreignPhoneFailure := verify.Execute(context.Background(), actorA, phone.RequestID, MockPhoneCode)
	_, randomPhoneFailure := verify.Execute(context.Background(), actorA, uuid.New(), MockPhoneCode)
	if !errors.Is(foreignPhoneFailure, ErrChallengeNotFound) || !errors.Is(randomPhoneFailure, ErrChallengeNotFound) {
		t.Fatalf("foreign/random phone failures: foreign=%v random=%v", foreignPhoneFailure, randomPhoneFailure)
	}

	qr, failure := NewStartQR(store).Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("start foreign QR: %v", failure)
	}
	refresh := NewRefreshQR(store)
	_, foreignFailure := refresh.Execute(context.Background(), actorA, qr.RequestID)
	_, randomFailure := refresh.Execute(context.Background(), actorA, uuid.New())
	if !errors.Is(foreignFailure, ErrChallengeNotFound) || !errors.Is(randomFailure, ErrChallengeNotFound) {
		t.Fatalf("foreign/random QR failures: foreign=%v random=%v", foreignFailure, randomFailure)
	}
}
