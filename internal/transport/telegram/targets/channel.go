package telegram

import (
	"errors"

	"github.com/gotd/td/tg"

	transport "github.com/notrodans/cresora/internal/transport/telegram"
)

// Represents a Telegram channel as a delivery target.
type channel struct {
	identity transport.ChannelID
	hash     transport.AccessHash
}

// Channel creates a generic Telegram channel target.
func Channel(identity transport.ChannelID, hash transport.AccessHash) transport.Target {
	return channel{
		identity: identity,
		hash:     hash,
	}
}

func (target channel) Peer() (tg.InputPeerClass, error) {
	if target.identity == 0 {
		return nil, errors.New("resolve Telegram channel peer with zero channel identity")
	}
	if target.hash == 0 {
		return nil, errors.New("resolve Telegram channel peer with zero access hash")
	}
	return &tg.InputPeerChannel{
		ChannelID:  int64(target.identity),
		AccessHash: int64(target.hash),
	}, nil
}
