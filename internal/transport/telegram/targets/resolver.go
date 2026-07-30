package telegram

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/notrodans/cresora/internal/domain/recipient"
	transport "github.com/notrodans/cresora/internal/transport/telegram"
)

var _ transport.Targets = resolver{}

// resolver resolves recipients for one Telegram account.
type resolver struct {
	accountID uuid.UUID
	lookup    transport.PeerLookup
}

// NewResolver creates a target resolver bound to one Telegram account.
func NewResolver(accountID uuid.UUID, lookup transport.PeerLookup) transport.Targets {
	if accountID == uuid.Nil {
		panic("resolve Telegram targets without account identity")
	}
	if lookup == nil {
		panic("resolve Telegram targets without peer lookup")
	}
	return resolver{
		accountID: accountID,
		lookup:    lookup,
	}
}

func (resolver resolver) Target(
	context context.Context,
	recipientValue recipient.Recipient,
) (transport.Target, error) {
	if context == nil {
		panic("resolve Telegram target without context")
	}
	if recipientValue == nil {
		panic("resolve Telegram target without recipient")
	}
	recipientID := recipientValue.UUID()
	if recipientID == uuid.Nil {
		panic("resolve Telegram target with zero recipient identity")
	}
	projection, failure := resolver.lookup.Lookup(context, transport.PeerLookupRequest{
		AccountID:   resolver.accountID,
		RecipientID: recipientID,
	})
	if failure != nil {
		return nil, fmt.Errorf(
			"lookup Telegram target for account %s and recipient %s: %w",
			resolver.accountID,
			recipientID,
			failure,
		)
	}
	return project(projection)
}

func project(projection transport.PeerProjection) (transport.Target, error) {
	if projection.ID == 0 {
		return nil, fmt.Errorf("%w: zero peer ID", transport.ErrInvalidPeer)
	}

	switch projection.Type {
	case transport.PeerTypeUser:
		hash, failure := requiredHash(projection)
		if failure != nil {
			return nil, failure
		}
		return User(transport.UserID(projection.ID), hash), nil
	case transport.PeerTypeChat:
		return BasicGroup(transport.ChatID(projection.ID)), nil
	case transport.PeerTypeChannel:
		hash, failure := requiredHash(projection)
		if failure != nil {
			return nil, failure
		}
		return Channel(transport.ChannelID(projection.ID), hash), nil
	default:
		return nil, fmt.Errorf("%w: unknown peer type %q", transport.ErrInvalidPeer, projection.Type)
	}
}

func requiredHash(projection transport.PeerProjection) (transport.AccessHash, error) {
	if projection.AccessHash == nil || *projection.AccessHash == 0 {
		return 0, fmt.Errorf("%w: peer %q has no access hash", transport.ErrInvalidPeer, projection.Type)
	}
	return transport.AccessHash(*projection.AccessHash), nil
}
