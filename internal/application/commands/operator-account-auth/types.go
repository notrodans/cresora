package operatoraccountauth

import (
	"context"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// Start begins or resumes one actor-scoped phone-auth attempt. The durable
// account admission happens before the provider's SendCode operation. An
// already-active account is represented in the returned Result and must not
// trigger SendCode.
type Start interface {
	Start(context.Context, applicationroot.Actor, string) (application.Result, error)
}

// Code submits one code to the challenge identified by requestID. The
// registry supplies the in-memory phone-code hash retained from SendCode;
// callers never provide or receive that hash.
type Code interface {
	Code(context.Context, applicationroot.Actor, uuid.UUID, string) (application.Result, error)
}

// Password submits the optional Telegram 2FA password for one pending
// challenge. It is admitted only after Code has moved the challenge to the
// password stage.
type Password interface {
	Password(context.Context, applicationroot.Actor, uuid.UUID, string) (application.Result, error)
}

// Cancel conditionally aborts one actor-owned pending challenge. Provider
// cancellation is best effort; durable lifecycle cleanup is not.
type Cancel interface {
	Cancel(context.Context, applicationroot.Actor, uuid.UUID) error
}
