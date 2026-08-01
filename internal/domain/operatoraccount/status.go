package operatoraccount

// Status is the current lifecycle state of an operator account. Lifecycle
// history is intentionally not part of this model. The only transitions are:
// authenticating -> active or disconnecting, active -> reauth_required or
// disconnecting, reauth_required -> authenticating or disconnecting,
// disconnected -> authenticating, and disconnecting -> disconnected.
type Status string

const (
	// StatusAuthenticating means that an authentication attempt is in progress.
	StatusAuthenticating Status = "authenticating"
	// StatusActive means that the account has an authenticated Telegram
	// identity and an active session.
	StatusActive Status = "active"
	// StatusReauthRequired means that the account remains identified but needs a
	// new authentication attempt before it can become active again.
	StatusReauthRequired Status = "reauth_required"
	// StatusDisconnected means that the account has no active lifecycle run.
	StatusDisconnected Status = "disconnected"
	// StatusDisconnecting means that account shutdown has been requested but has
	// not completed yet.
	StatusDisconnecting Status = "disconnecting"
)

func (status Status) valid() bool {
	switch status {
	case StatusAuthenticating, StatusActive, StatusReauthRequired, StatusDisconnected, StatusDisconnecting:
		return true
	default:
		return false
	}
}
