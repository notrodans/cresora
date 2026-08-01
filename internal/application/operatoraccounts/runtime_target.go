package operatoraccounts

import (
	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// RuntimeTarget is the application-owned admission value for one operator
// account runtime. Status and Version fence runtime work to the lifecycle
// snapshot observed by the caller.
//
// It contains only application and domain values; transport-specific runtime
// session types belong to an adapter.
type RuntimeTarget struct {
	Actor     application.Actor
	AccountID operatoraccount.ID
	Status    operatoraccount.Status
	Version   operatoraccount.Version
}
