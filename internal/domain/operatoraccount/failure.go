package operatoraccount

// FailureCode describes why an account entered StatusReauthRequired. The domain does
// not couple itself to a transport's error type. Codes are deliberately
// bounded so transport errors cannot become unbounded persisted state.
type FailureCode string

const (
	// NoFailure is the only valid failure code outside StatusReauthRequired.
	NoFailure FailureCode = ""
	// FailureCodeAuthExpired indicates that an authentication attempt expired.
	FailureCodeAuthExpired FailureCode = "auth_expired"
	// FailureCodeSessionInvalid indicates that a stored session was rejected by
	// Telegram and reauthentication is required.
	FailureCodeSessionInvalid FailureCode = "session_invalid"
	// FailureCodeAuthorizationRevoked indicates that Telegram revoked the
	// account authorization.
	FailureCodeAuthorizationRevoked FailureCode = "authorization_revoked"
)

func validFailureCode(code FailureCode) bool {
	switch code {
	case FailureCodeAuthExpired, FailureCodeSessionInvalid, FailureCodeAuthorizationRevoked:
		return true
	default:
		return false
	}
}
