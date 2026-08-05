package accountowner

import (
	"context"
	"errors"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

func isContextFailure(failure error) bool {
	return errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded)
}

func accountKeyFromTarget(target operatoraccounts.RuntimeTarget) accountKey {
	return accountKey{
		operatorID: target.Actor.OperatorID,
		accountID:  target.AccountID.UUID(),
	}
}
