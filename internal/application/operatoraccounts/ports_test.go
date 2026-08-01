package operatoraccounts

import (
	"context"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

type lifecyclePortProbe struct{}

func (lifecyclePortProbe) LoadAccount(context.Context, application.Actor, operatoraccount.ID) (operatoraccount.Account, error) {
	return operatoraccount.Account{}, nil
}

func (lifecyclePortProbe) PersistLifecycle(
	context.Context,
	application.Actor,
	operatoraccount.Account,
	operatoraccount.Version,
) error {
	return nil
}

func (lifecyclePortProbe) DeleteSession(context.Context, application.Actor, operatoraccount.ID) error {
	return nil
}

var (
	_ AccountLifecycleReader     = lifecyclePortProbe{}
	_ AccountLifecycleWriter     = lifecyclePortProbe{}
	_ AccountLifecycleRepository = lifecyclePortProbe{}
	_ SessionDeleter             = lifecyclePortProbe{}
)
