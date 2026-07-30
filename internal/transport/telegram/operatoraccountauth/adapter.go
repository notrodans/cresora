// Package operatoraccountauth is the Telegram transport boundary for
// operator-account authentication. The methods are deliberately unimplemented
// until live authentication and session persistence are wired in.
package operatoraccountauth

import (
	"context"
	"errors"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/auth/qrlogin"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// ErrLiveAuthenticationDisabled is the safe result of the intentionally inert
// Telegram auth adapter. Live gotd auth, session writes, and account
// persistence are not part of the challenge-coordinator composition yet.
var ErrLiveAuthenticationDisabled = errors.New("live Telegram operator authentication is disabled")

// Adapter owns the gotd client configuration needed by the eventual Telegram
// authentication implementation. It performs no network calls and stores no
// session data in this scaffold.
type Adapter struct {
	client  *telegram.Client
	appID   int
	appHash string

	// These fields make the intended gotd integration explicit without
	// constructing live auth flows in the scaffold.
	phoneAuth *auth.Client
	qrLogin   *qrlogin.QR
}

// New constructs an adapter around a gotd Telegram client.
func New(client *telegram.Client, appID int, appHash string) Adapter {
	return Adapter{
		client:  client,
		appID:   appID,
		appHash: appHash,
	}
}

// NewAdapter is an explicit constructor alias.
func NewAdapter(client *telegram.Client, appID int, appHash string) Adapter {
	return New(client, appID, appHash)
}

// StartPhone is the future phone-code authentication entry point. Wire an
// auth.Client.SendCode call here and map its response to PhoneChallenge.
func (adapter Adapter) StartPhone(
	context.Context,
	applicationroot.Actor,
	string,
) (application.PhoneChallenge, error) {
	return application.PhoneChallenge{}, ErrLiveAuthenticationDisabled
}

// VerifyPhone is the future phone-code authentication completion point. Wire
// auth.Client.SignIn here, then map the authorized Telegram user to Account.
func (adapter Adapter) VerifyPhone(
	context.Context,
	applicationroot.Actor,
	uuid.UUID,
	string,
) (application.Account, error) {
	return application.Account{}, ErrLiveAuthenticationDisabled
}

// StartQR is the future QR authentication entry point. Wire qrlogin.NewQR,
// followed by token export, here.
func (adapter Adapter) StartQR(context.Context, applicationroot.Actor) (application.QRChallenge, error) {
	return application.QRChallenge{}, ErrLiveAuthenticationDisabled
}

// RefreshQR is the future QR token refresh point. Wire QR token export/import
// and expiry mapping here.
func (adapter Adapter) RefreshQR(
	context.Context,
	applicationroot.Actor,
	uuid.UUID,
) (application.QRChallenge, error) {
	return application.QRChallenge{}, ErrLiveAuthenticationDisabled
}

// Status is the future account projection endpoint. It intentionally does not
// read persistence in this scaffold.
func (adapter Adapter) Status(context.Context, applicationroot.Actor) (application.Status, error) {
	return application.Status{}, ErrLiveAuthenticationDisabled
}
