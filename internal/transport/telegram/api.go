package telegram

import (
	"context"

	"github.com/gotd/td/tg"
)

// Предоставляет операции API сообщений Telegram, сгенерированные из TL-схемы
type API interface {
	MessagesSendMessage(
		context.Context,
		*tg.MessagesSendMessageRequest,
	) (tg.UpdatesClass, error)
}
