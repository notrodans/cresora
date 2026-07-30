package operatoraccountauth

import (
	"context"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/nebula-go/internal/application"
	application "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
)

// StartPhone starts phone-code authentication for a normalized phone number.
type StartPhone interface {
	Execute(context.Context, applicationroot.Actor, string) (application.PhoneChallenge, error)
}

// VerifyPhone completes phone-code authentication for a pending request.
type VerifyPhone interface {
	Execute(context.Context, applicationroot.Actor, uuid.UUID, string) (application.Account, error)
}

// StartQR starts QR authentication.
type StartQR interface {
	Execute(context.Context, applicationroot.Actor) (application.QRChallenge, error)
}

// RefreshQR refreshes a pending QR authentication token.
type RefreshQR interface {
	Execute(context.Context, applicationroot.Actor, uuid.UUID) (application.QRChallenge, error)
}
