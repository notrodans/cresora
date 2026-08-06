package operatoraccountauth

import (
	"context"

	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// Status returns the actor-scoped account list and any in-progress
// authentication challenge. The returned projection never contains the
// Telegram phone-code hash or a provider/runtime value.
type Status interface {
	Status(context.Context, applicationroot.Actor) (application.Status, error)
}
