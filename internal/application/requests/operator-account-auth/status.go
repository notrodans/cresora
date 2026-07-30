package operatoraccountauth

import (
	"context"

	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// Status returns the operator account list and any in-progress authentication
// challenges.
type Status interface {
	Execute(context.Context, applicationroot.Actor) (application.Status, error)
}
