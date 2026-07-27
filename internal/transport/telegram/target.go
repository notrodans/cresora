package telegram

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/gotd/td/tg"

	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

var (
	ErrTargetNotFound = errors.New("Telegram target not found")
	ErrInvalidPeer    = errors.New("invalid Telegram peer")
)

// PeerType identifies the Telegram peer kind returned by a lookup
type PeerType string

const (
	PeerTypeUser    PeerType = "user"
	PeerTypeChat    PeerType = "chat"
	PeerTypeChannel PeerType = "channel"
)

// PeerProjection is the account-specific Telegram identity needed to build an input peer
type PeerProjection struct {
	Type       PeerType
	ID         int64
	AccessHash *int64
}

// PeerLookupRequest identifies the account and mailing recipient to resolve
type PeerLookupRequest struct {
	AccountID   uuid.UUID
	RecipientID uuid.UUID
}

// PeerLookup resolves a mailing recipient to an account-specific Telegram peer
type PeerLookup interface {
	Lookup(context.Context, PeerLookupRequest) (PeerProjection, error)
}

// Represents an addressable Telegram destination
type Target interface {
	Peer() (tg.InputPeerClass, error)
}

// Resolves domain recipients for one account
type Targets interface {
	Target(context.Context, recipient.Recipient) (Target, error)
}
