package operatoraccountauth

import (
	"context"

	application "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
)

// Status returns the operator account list and any in-progress authentication
// challenges.
type Status interface {
	Execute(context.Context) (application.Status, error)
}
