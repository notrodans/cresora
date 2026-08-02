// Package operatoraccounts is the Telegram transport adapter for operator
// account runtime revocation.
package operatoraccounts

import (
	"context"
	"errors"

	"github.com/gotd/td/session"
	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	application "github.com/notrodans/cresora/internal/application/operatoraccounts"
	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

// Runtime is the transport-side callback boundary owned by accountowner. Raw
// gotd clients are available only during the callback and never cross this
// adapter's application-facing method.
type Runtime interface {
	RevokeAndStop(context.Context, application.RuntimeTarget, accountowner.ClientCallback) error
}

// Adapter implements application.RuntimeRevoker. It performs one raw
// auth.logOut request and translates its result to the closed application
// outcome type.
type Adapter struct {
	runtime       Runtime
	clientFactory func(*gotdtelegram.Client) logoutClient
}

var _ application.RuntimeRevoker = Adapter{}
var _ Runtime = (*accountowner.Registry)(nil)

// New constructs a revoker without starting a client or performing network
// I/O.
func New(runtime Runtime) Adapter {
	return Adapter{
		runtime:       runtime,
		clientFactory: newGotdLogoutClient,
	}
}

// RevokeAndStop fences and tears down the account runtime while issuing one
// privileged logout callback. No gotd error or type is returned to the
// application layer.
func (adapter Adapter) RevokeAndStop(
	ctx context.Context,
	target application.RuntimeTarget,
) application.RevokeOutcome {
	failure := adapter.runtime.RevokeAndStop(ctx, target, func(callbackContext context.Context, raw *gotdtelegram.Client) error {
		client := adapter.clientFactory(raw)
		if client == nil {
			return logoutFailure{err: errLogoutClientUnavailable}
		}
		response, failure := client.logOut(callbackContext)
		if failure != nil {
			return logoutFailure{err: failure}
		}
		if response == nil {
			return logoutFailure{err: errInvalidLogoutResponse}
		}
		return nil
	})
	if failure == nil {
		return application.RevokeSucceeded()
	}
	if isUnauthorizedFailure(failure) {
		return application.RevokeAlreadyComplete()
	}
	var transportFailure logoutFailure
	if !errors.As(failure, &transportFailure) {
		if isPermanentSessionFailure(failure) {
			return failedOutcome(application.NewRemoteLogoutFailure(application.RemoteLogoutFailurePermanent, 0))
		}
		return failedOutcome(application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0))
	}
	if rpcFailure, ok := tgerr.As(transportFailure.err); ok && rpcFailure != nil && rpcFailure.Code == 401 {
		return application.RevokeAlreadyComplete()
	}
	return failedOutcome(classifyRevokeFailure(transportFailure.err))
}

// logoutClient is deliberately transport-local. Tests can substitute it
// without constructing a gotd client or issuing a request.
type logoutClient interface {
	logOut(context.Context) (*tg.AuthLoggedOut, error)
}

type logoutFailure struct {
	err error
}

func (failure logoutFailure) Error() string {
	return "telegram account logout request failed"
}

func (failure logoutFailure) Unwrap() error {
	return failure.err
}

type gotdLogoutClient struct {
	client *gotdtelegram.Client
}

func newGotdLogoutClient(client *gotdtelegram.Client) logoutClient {
	if client == nil {
		return nil
	}
	return gotdLogoutClient{client: client}
}

func (client gotdLogoutClient) logOut(ctx context.Context) (*tg.AuthLoggedOut, error) {
	return client.client.API().AuthLogOut(ctx)
}

func classifyRevokeFailure(failure error) (*application.RemoteLogoutFailure, error) {
	if failure == nil {
		return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0)
	}
	if errors.Is(failure, errInvalidLogoutResponse) || isPermanentSessionFailure(failure) {
		return application.NewRemoteLogoutFailure(application.RemoteLogoutFailurePermanent, 0)
	}
	if errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded) {
		return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureAmbiguous, 0)
	}
	if duration, ok := tgerr.AsFloodWait(failure); ok {
		if duration > 0 {
			return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureFloodWait, duration)
		}
		return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0)
	}
	if rpcFailure, ok := tgerr.As(failure); ok && rpcFailure != nil {
		switch {
		case rpcFailure.Code >= 500 && rpcFailure.Code < 600:
			return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureTransient, 0)
		case rpcFailure.Code >= 400 && rpcFailure.Code < 500:
			return application.NewRemoteLogoutFailure(application.RemoteLogoutFailurePermanent, 0)
		}
	}
	if isRuntimeFailure(failure) {
		return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0)
	}
	return application.NewRemoteLogoutFailure(application.RemoteLogoutFailureTransient, 0)
}

func isUnauthorizedFailure(failure error) bool {
	rpcFailure, ok := tgerr.As(failure)
	return ok && rpcFailure != nil && rpcFailure.Code == 401
}

func isPermanentSessionFailure(failure error) bool {
	for _, sessionFailure := range []error{
		session.ErrNotFound,
		application.ErrSessionNotFound,
		transporttelegram.ErrSessionInvalid,
		transporttelegram.ErrSessionTooLarge,
		transporttelegram.ErrSessionCorrupt,
	} {
		if errors.Is(failure, sessionFailure) {
			return true
		}
	}
	return false
}

func isRuntimeFailure(failure error) bool {
	for _, runtimeFailure := range []error{
		accountowner.ErrRegistryStopped,
		accountowner.ErrAccountStopped,
		accountowner.ErrRuntimeCapacity,
		accountowner.ErrFenceCapacity,
		accountowner.ErrStaleAdmission,
		accountowner.ErrInvalidAdmission,
		accountowner.ErrNilCallback,
	} {
		if errors.Is(failure, runtimeFailure) {
			return true
		}
	}
	return errors.Is(failure, errLogoutClientUnavailable)
}

func failedOutcome(failure *application.RemoteLogoutFailure, err error) application.RevokeOutcome {
	if err != nil || failure == nil {
		failure, _ = application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0)
	}
	outcome, err := application.NewRevokeFailure(failure)
	if err != nil {
		fallback, _ := application.NewRemoteLogoutFailure(application.RemoteLogoutFailureUnavailable, 0)
		outcome, _ = application.NewRevokeFailure(fallback)
	}
	return outcome
}

var errLogoutClientUnavailable = errors.New("telegram account logout client unavailable")

var errInvalidLogoutResponse = errors.New("telegram account logout response is invalid")
