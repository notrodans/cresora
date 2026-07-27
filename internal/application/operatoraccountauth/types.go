// Package operatoraccountauth contains transport-neutral data for operator
// account authentication.
package operatoraccountauth

import (
	"time"

	"github.com/google/uuid"
)

// Account is the display projection of an operator-owned Telegram account.
// Its fields mirror the account data exposed by the operator_accounts table.
type Account struct {
	ID                uuid.UUID
	Phone             string
	TelegramUsername  string
	TelegramFirstName string
	TelegramLastName  string
}

// PhoneChallenge describes a pending Telegram phone-code authentication
// request.
type PhoneChallenge struct {
	RequestID uuid.UUID
	Phone     string
	Delivery  string
	ExpiresAt time.Time
}

// QRChallenge describes a pending Telegram QR authentication request.
type QRChallenge struct {
	RequestID uuid.UUID
	URL       string
	ExpiresAt time.Time
}

// Status is the operator account authentication dashboard projection. A nil
// challenge means that no authentication flow of that kind is in progress.
type Status struct {
	Accounts       []Account
	PhoneChallenge *PhoneChallenge
	QRChallenge    *QRChallenge
}
