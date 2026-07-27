package mailingconsole

import (
	"time"

	"github.com/google/uuid"
)

// Account is an operator-owned Telegram account available to the console.
type Account struct {
	ID                uuid.UUID
	Phone             string
	TelegramUsername  string
	TelegramFirstName string
	TelegramLastName  string
}

// SharedDialog is a shared Telegram dialog available through an account.
type SharedDialog struct {
	ID                uuid.UUID
	AccountID         uuid.UUID
	PeerID            int64
	Kind              string
	Title             string
	CanonicalUsername string
	AccessHash        int64
}

// PeerType identifies a private Telegram peer kind.
type PeerType string

const (
	PeerTypeUser    PeerType = "user"
	PeerTypeChat    PeerType = "chat"
	PeerTypeChannel PeerType = "channel"
)

// PrivateTarget identifies a private peer without accepting an account ID
// from the caller. The selected route account is bound by the repository.
type PrivateTarget struct {
	PeerType PeerType
	PeerID   int64
}

// PrivateDialog is a sendable, resolvable private dialog for one account.
type PrivateDialog struct {
	AccountID  uuid.UUID
	PeerType   PeerType
	PeerID     int64
	Title      string
	Username   string
	AccessHash *int64
}

// MailingSummary is the list/dashboard representation of a mailing.
type MailingSummary struct {
	ID             uuid.UUID
	Name           string
	Status         string
	AccountID      uuid.UUID
	RecipientCount int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateDraftInput contains the editable data for a new draft.
// Operator identity is deliberately not part of this request.
type CreateDraftInput struct {
	Name            string
	MessageText     string
	AccountID       uuid.UUID
	SharedDialogIDs []uuid.UUID
	PrivateTargets  []PrivateTarget
}

// Dashboard contains the operator-scoped console landing data.
type Dashboard struct {
	Accounts       []Account
	SharedDialogs  []SharedDialog
	PrivateDialogs []PrivateDialog
	Mailings       []MailingSummary
}
