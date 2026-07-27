package operatoraccountauth

import (
	"context"

	"github.com/google/uuid"
	application "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
)

// StartPhone starts phone-code authentication for a normalized phone number.
type StartPhone interface {
	Execute(context.Context, string) (application.PhoneChallenge, error)
}

// VerifyPhone completes phone-code authentication for a pending request.
type VerifyPhone interface {
	Execute(context.Context, uuid.UUID, string) (application.Account, error)
}

// StartQR starts QR authentication.
type StartQR interface {
	Execute(context.Context) (application.QRChallenge, error)
}

// RefreshQR refreshes a pending QR authentication token.
type RefreshQR interface {
	Execute(context.Context, uuid.UUID) (application.QRChallenge, error)
}
