// Package operatoraccountauth is the Telegram transport boundary for
// operator-account authentication. The methods are deliberately unimplemented
// until live authentication and session persistence are wired in.
package operatoraccountauth

import (
	"context"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/auth/qrlogin"

	"github.com/google/uuid"
	application "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
)

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
	string,
) (application.PhoneChallenge, error) {
	panic("TODO: use auth.Client.SendCode and map its delivery to PhoneChallenge")
}

// VerifyPhone is the future phone-code authentication completion point. Wire
// auth.Client.SignIn here, then map the authorized Telegram user to Account.
func (adapter Adapter) VerifyPhone(
	context.Context,
	uuid.UUID,
	string,
) (application.Account, error) {
	panic("TODO: use auth.Client.SignIn and persist no session until explicitly wired")
}

// StartQR is the future QR authentication entry point. Wire qrlogin.NewQR,
// followed by token export, here.
func (adapter Adapter) StartQR(context.Context) (application.QRChallenge, error) {
	panic("TODO: use qrlogin.NewQR and QR token export to build QRChallenge")
}

// RefreshQR is the future QR token refresh point. Wire QR token export/import
// and expiry mapping here.
func (adapter Adapter) RefreshQR(
	context.Context,
	uuid.UUID,
) (application.QRChallenge, error) {
	panic("TODO: use qrlogin QR token export/import to refresh QRChallenge")
}

// Status is the future account projection endpoint. It intentionally does not
// read persistence in this scaffold.
func (adapter Adapter) Status(context.Context) (application.Status, error) {
	panic("TODO: load operator_accounts display rows and in-progress auth challenges")
}
