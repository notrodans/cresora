package operatoraccountauth

import (
	"context"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

type startCommandProbe struct{}

func (startCommandProbe) Execute(context.Context, applicationroot.Actor, string) (application.Result, error) {
	return application.Result{}, nil
}

type codeCommandProbe struct{}

func (codeCommandProbe) Execute(context.Context, applicationroot.Actor, uuid.UUID, string) (application.Result, error) {
	return application.Result{}, nil
}

type passwordCommandProbe struct{}

func (passwordCommandProbe) Execute(context.Context, applicationroot.Actor, uuid.UUID, string) (application.Result, error) {
	return application.Result{}, nil
}

type cancelCommandProbe struct{}

func (cancelCommandProbe) Execute(context.Context, applicationroot.Actor, uuid.UUID) error {
	return nil
}

var (
	_ Start    = startCommandProbe{}
	_ Code     = codeCommandProbe{}
	_ Password = passwordCommandProbe{}
	_ Cancel   = cancelCommandProbe{}
)
