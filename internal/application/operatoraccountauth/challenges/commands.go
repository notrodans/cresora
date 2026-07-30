package challenges

import (
	"context"
	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
)

// StartPhoneCommand is an explicit actor-scoped phone-start command. Keeping
// the actor as an Execute argument makes it impossible for a form, cookie, or
// query parameter to select the operator scope.
type StartPhoneCommand struct{ Coordinator *Coordinator }

func (command StartPhoneCommand) Execute(ctx context.Context, actor applicationroot.Actor, phone string) (PhoneProjection, error) {
	return command.Coordinator.StartPhoneChallenge(ctx, actor, phone)
}

// SubmitPhoneCodeCommand is the actor-scoped phone completion command.
type SubmitPhoneCodeCommand struct{ Coordinator *Coordinator }

func (command SubmitPhoneCodeCommand) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (Submission, error) {
	return command.Coordinator.SubmitPhoneCode(ctx, actor, requestID, code)
}

// StartQRCommand is the actor-scoped QR-start command.
type StartQRCommand struct{ Coordinator *Coordinator }

func (command StartQRCommand) Execute(ctx context.Context, actor applicationroot.Actor) (QRProjection, error) {
	return command.Coordinator.StartQRChallenge(ctx, actor)
}

// RefreshQRCommand is the actor-scoped QR-refresh command.
type RefreshQRCommand struct{ Coordinator *Coordinator }

func (command RefreshQRCommand) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) (QRProjection, error) {
	return command.Coordinator.RefreshQRChallenge(ctx, actor, requestID)
}

// CancelChallengeCommand is the actor-scoped, idempotent cancellation command.
type CancelChallengeCommand struct{ Coordinator *Coordinator }

func (command CancelChallengeCommand) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	return command.Coordinator.Cancel(ctx, actor, requestID)
}

// StatusQuery is the actor-scoped safe challenge projection query.
type StatusQuery struct{ Coordinator *Coordinator }

func (query StatusQuery) Execute(ctx context.Context, actor applicationroot.Actor) (StatusProjection, error) {
	return query.Coordinator.Query(ctx, actor)
}
