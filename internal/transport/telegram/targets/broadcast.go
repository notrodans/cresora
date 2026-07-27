package telegram

import (
	"errors"

	"github.com/gotd/td/tg"
	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

// Представляет широковещательный канал Telegram как цель доставки
type broadcast struct {
	identity telegram.ChannelID
	hash     telegram.AccessHash
}

func Broadcast(identity telegram.ChannelID, hash telegram.AccessHash) telegram.Target {
	return broadcast{
		identity: identity,
		hash:     hash,
	}
}

func (target broadcast) Peer() (tg.InputPeerClass, error) {
	if target.identity == 0 {
		return nil, errors.New("resolve Telegram broadcast peer with zero channel identity")
	}
	if target.hash == 0 {
		return nil, errors.New("resolve Telegram broadcast peer with zero access hash")
	}
	return &tg.InputPeerChannel{
		ChannelID:  int64(target.identity),
		AccessHash: int64(target.hash),
	}, nil
}
