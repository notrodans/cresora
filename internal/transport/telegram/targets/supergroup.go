package telegram

import (
	"errors"

	"github.com/gotd/td/tg"
	"github.com/notrodans/cresora/internal/transport/telegram"
)

// Представляет супергруппу Telegram как цель доставки
type supergroup struct {
	identity telegram.ChannelID
	hash     telegram.AccessHash
}

func Supergroup(identity telegram.ChannelID, hash telegram.AccessHash) telegram.Target {
	return supergroup{
		identity: identity,
		hash:     hash,
	}
}

func (target supergroup) Peer() (tg.InputPeerClass, error) {
	if target.identity == 0 {
		return nil, errors.New("resolve Telegram supergroup peer with zero channel identity")
	}
	if target.hash == 0 {
		return nil, errors.New("resolve Telegram supergroup peer with zero access hash")
	}
	return &tg.InputPeerChannel{
		ChannelID:  int64(target.identity),
		AccessHash: int64(target.hash),
	}, nil
}
