package telegram

import (
	"errors"
	"github.com/gotd/td/tg"
	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

// Представляет пользователя Telegram как цель доставки
type user struct {
	identity telegram.UserID
	hash     telegram.AccessHash
}

func User(identity telegram.UserID, hash telegram.AccessHash) telegram.Target {
	return user{
		identity: identity,
		hash:     hash,
	}
}

func (target user) Peer() (tg.InputPeerClass, error) {
	if target.identity == 0 {
		return nil, errors.New("resolve Telegram user peer with zero user identity")
	}
	if target.hash == 0 {
		return nil, errors.New("resolve Telegram user peer with zero access hash")
	}
	return &tg.InputPeerUser{
		UserID:     int64(target.identity),
		AccessHash: int64(target.hash),
	}, nil
}
