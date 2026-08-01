package account

import (
	"context"
	"errors"
	"fmt"

	gotdtelegram "github.com/gotd/td/telegram"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

// Runtime is the account runtime boundary consumed by delivery commands. The
// callback receives the client only while the runtime has admitted the
// operation.
type Runtime interface {
	Execute(context.Context, operatoraccounts.RuntimeTarget, accountowner.ClientCallback) error
}

type admissionCommands struct {
	revalidation application.AccountRevalidationReader
	deliveries   application.Deliveries
	targets      Targets
	runtime      Runtime
	clientToAPI  clientToAPI
}

// NewAdmissionCommands creates delivery commands that revalidate account
// lifecycle admission before each command is resolved.
func NewAdmissionCommands(
	revalidation application.AccountRevalidationReader,
	deliveries application.Deliveries,
	targets Targets,
	runtime Runtime,
) admissionCommands {
	return newAdmissionCommands(revalidation, deliveries, targets, runtime, gotdClientToAPI)
}

func newAdmissionCommands(
	revalidation application.AccountRevalidationReader,
	deliveries application.Deliveries,
	targets Targets,
	runtime Runtime,
	clientToAPI clientToAPI,
) admissionCommands {
	return admissionCommands{
		revalidation: revalidation,
		deliveries:   deliveries,
		targets:      targets,
		runtime:      runtime,
		clientToAPI:  clientToAPI,
	}
}

func (commands admissionCommands) Command(
	context context.Context,
	admission application.AccountAdmission,
) (application.Command, error) {
	target, failure := commands.revalidation.Revalidate(context, admission)
	if failure != nil {
		if errors.Is(failure, operatoraccounts.ErrAccountNotFound) {
			return nil, fmt.Errorf(
				"%w: revalidate delivery account admission: %w",
				application.ErrAccountAdmissionRejected,
				failure,
			)
		}
		return nil, fmt.Errorf("revalidate delivery account admission: %w", failure)
	}

	port := runtimeGatedPort{
		runtime:     commands.runtime,
		target:      target,
		targets:     commands.targets.Targets(admission.Route),
		clientToAPI: commands.clientToAPI,
	}
	return application.New(commands.deliveries, port), nil
}

type clientToAPI func(*gotdtelegram.Client) telegram.API

var errRuntimeCallbackMissing = errors.New(
	"telegram account runtime completed without invoking delivery callback",
)

func gotdClientToAPI(client *gotdtelegram.Client) telegram.API {
	return client.API()
}

type runtimeGatedPort struct {
	runtime     Runtime
	target      operatoraccounts.RuntimeTarget
	targets     telegram.Targets
	clientToAPI clientToAPI
}

var _ application.Port = runtimeGatedPort{}

func (port runtimeGatedPort) Send(
	ctx context.Context,
	recipient recipient.Recipient,
	message message.Message,
	randomID int64,
) error {
	entered := false
	var callbackFailure error
	runtimeFailure := port.runtime.Execute(
		ctx,
		port.target,
		func(callbackContext context.Context, client *gotdtelegram.Client) error {
			entered = true
			api := port.clientToAPI(client)
			sender := telegram.New(api, port.targets)
			callbackFailure = sender.Send(callbackContext, recipient, message, randomID)
			return callbackFailure
		},
	)
	if entered {
		if callbackFailure != nil {
			return callbackFailure
		}
		return runtimeFailure
	}
	if runtimeFailure == nil {
		return application.WrapUnknown(errRuntimeCallbackMissing)
	}
	if isRuntimeAdmissionRejection(runtimeFailure) {
		return application.WrapTransient(runtimeFailure)
	}
	return runtimeFailure
}

func isRuntimeAdmissionRejection(failure error) bool {
	return errors.Is(failure, accountowner.ErrRegistryStopped) ||
		errors.Is(failure, accountowner.ErrAccountStopped) ||
		errors.Is(failure, accountowner.ErrStaleAdmission) ||
		errors.Is(failure, accountowner.ErrStopped)
}
