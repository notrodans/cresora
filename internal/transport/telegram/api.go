package telegram

import (
	"context"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// Предоставляет операции API сообщений Telegram, сгенерированные из TL-схемы
type API interface {
	MessagesSendMessage(
		context.Context,
		*tg.MessagesSendMessageRequest,
	) (tg.UpdatesClass, error)
}

type telegramApi struct {
	client *telegram.Client
}

func NewTelegramApi(appID int, appHash string, options telegram.Options) telegramApi {
	client := telegram.NewClient(appID, appHash, options)
	return telegramApi{
		client: client,
	}
}

func (ta *telegramApi) Run() {
	if err := ta.client.Run(context.Background(), func(ctx context.Context) error {
		api := ta.client.API()
		_ = api

		return nil
	}); err != nil {
		panic(err)
	}
}
