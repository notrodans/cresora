package telegram

import (
	"errors"

	"github.com/gotd/td/tg"
	"github.com/notrodans/cresora/internal/transport/telegram"
)

// Представляет базовую группу Telegram как цель доставки
type basicGroup struct {
	identity telegram.ChatID
}

func BasicGroup(identity telegram.ChatID) telegram.Target {
	return basicGroup{
		identity: identity,
	}
}

func (target basicGroup) Peer() (tg.InputPeerClass, error) {
	if target.identity == 0 {
		return nil, errors.New("resolve Telegram basic group peer with zero chat identity")
	}
	return &tg.InputPeerChat{
		ChatID: int64(target.identity),
	}, nil
}
