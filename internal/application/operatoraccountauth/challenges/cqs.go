package challenges

import (
	"context"
	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	requests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
)

// CQS exposes the coordinator through the repository's existing command/query
// ports. Each adapter still receives the trusted actor explicitly; no adapter
// reads actor identity from a browser request.
type CQS struct {
	StartPhone  commands.StartPhone
	VerifyPhone commands.VerifyPhone
	StartQR     commands.StartQR
	RefreshQR   commands.RefreshQR
	Status      requests.Status
}

func (coordinator *Coordinator) CQS() CQS {
	return CQS{
		StartPhone:  startPhoneAdapter{coordinator: coordinator},
		VerifyPhone: verifyPhoneAdapter{coordinator: coordinator},
		StartQR:     startQRAdapter{coordinator: coordinator},
		RefreshQR:   refreshQRAdapter{coordinator: coordinator},
		Status:      statusAdapter{coordinator: coordinator},
	}
}

type startPhoneAdapter struct{ coordinator *Coordinator }

func (adapter startPhoneAdapter) Execute(ctx context.Context, actor applicationroot.Actor, phone string) (common.PhoneChallenge, error) {
	return adapter.coordinator.StartPhone(ctx, actor, phone)
}

type verifyPhoneAdapter struct{ coordinator *Coordinator }

func (adapter verifyPhoneAdapter) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (common.Account, error) {
	return adapter.coordinator.VerifyPhone(ctx, actor, requestID, code)
}

type startQRAdapter struct{ coordinator *Coordinator }

func (adapter startQRAdapter) Execute(ctx context.Context, actor applicationroot.Actor) (common.QRChallenge, error) {
	return adapter.coordinator.StartQR(ctx, actor)
}

type refreshQRAdapter struct{ coordinator *Coordinator }

func (adapter refreshQRAdapter) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) (common.QRChallenge, error) {
	return adapter.coordinator.RefreshQR(ctx, actor, requestID)
}

type statusAdapter struct{ coordinator *Coordinator }

func (adapter statusAdapter) Execute(ctx context.Context, actor applicationroot.Actor) (common.Status, error) {
	return adapter.coordinator.Status(ctx, actor)
}
