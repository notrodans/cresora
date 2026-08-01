package operatoraccountauth

import (
	"context"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	auth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// Application groups the runtime-backed phone-auth command adapters.
type Application struct {
	Start    Start
	Code     Code
	Password Password
	Cancel   Cancel
}

// NewApplication adapts one authentication service to the command ports.
func NewApplication(service *auth.Service) Application {
	return Application{
		Start:    StartAdapter{service: service},
		Code:     CodeAdapter{service: service},
		Password: PasswordAdapter{service: service},
		Cancel:   CancelAdapter{service: service},
	}
}

// StartAdapter implements Start without allowing transport input to select the
// actor scope.
type StartAdapter struct{ service *auth.Service }

// NewStart constructs the start command adapter.
func NewStart(service *auth.Service) Start { return StartAdapter{service: service} }

func (adapter StartAdapter) Execute(ctx context.Context, actor applicationroot.Actor, phone string) (auth.Result, error) {
	return adapter.service.Start(ctx, actor, phone)
}

// CodeAdapter implements Code.
type CodeAdapter struct{ service *auth.Service }

// NewCode constructs the code command adapter.
func NewCode(service *auth.Service) Code { return CodeAdapter{service: service} }

func (adapter CodeAdapter) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (auth.Result, error) {
	return adapter.service.Code(ctx, actor, requestID, code)
}

// PasswordAdapter implements Password.
type PasswordAdapter struct{ service *auth.Service }

// NewPassword constructs the password command adapter.
func NewPassword(service *auth.Service) Password { return PasswordAdapter{service: service} }

func (adapter PasswordAdapter) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, password string) (auth.Result, error) {
	return adapter.service.Password(ctx, actor, requestID, password)
}

// CancelAdapter implements Cancel.
type CancelAdapter struct{ service *auth.Service }

// NewCancel constructs the cancel command adapter.
func NewCancel(service *auth.Service) Cancel { return CancelAdapter{service: service} }

func (adapter CancelAdapter) Execute(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	return adapter.service.Cancel(ctx, actor, requestID)
}

var (
	_ Start    = StartAdapter{}
	_ Code     = CodeAdapter{}
	_ Password = PasswordAdapter{}
	_ Cancel   = CancelAdapter{}
)
