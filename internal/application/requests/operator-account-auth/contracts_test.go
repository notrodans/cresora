package operatoraccountauth

import (
	"context"

	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

type statusRequestProbe struct{}

func (statusRequestProbe) Execute(context.Context, applicationroot.Actor) (application.Status, error) {
	return application.Status{}, nil
}

var _ Status = statusRequestProbe{}
