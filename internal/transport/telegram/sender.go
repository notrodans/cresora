package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"

	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

// Delivers domain messages through Telegram
type telegramSender struct {
	api     API
	targets Targets
}

func New(api API, targets Targets) telegramSender {
	return telegramSender{
		api:     api,
		targets: targets,
	}
}

func (ts telegramSender) Send(
	context context.Context,
	recipient recipient.Recipient,
	message message.Message,
	random int64,
) error {
	if context == nil {
		panic("send Telegram message without context")
	}
	if ts.api == nil {
		panic("send Telegram message without API")
	}
	if ts.targets == nil {
		panic("send Telegram message without targets")
	}
	if random == 0 {
		panic("send Telegram message with zero random identity")
	}
	target, failure := ts.targets.Target(context, recipient)
	if failure != nil {
		return fmt.Errorf("resolve Telegram target: %w", failure)
	}
	if target == nil {
		return errors.New("resolve Telegram target: target is nil")
	}
	peer, failure := target.Peer()
	if failure != nil {
		return fmt.Errorf("resolve Telegram input peer: %w", failure)
	}
	var body strings.Builder
	if failure = message.Print(&body); failure != nil {
		return fmt.Errorf("render Telegram message body: %w", failure)
	}
	_, failure = ts.api.MessagesSendMessage(
		context,
		&tg.MessagesSendMessageRequest{
			Peer:     peer,
			Message:  body.String(),
			RandomID: random,
		},
	)
	if failure != nil {
		return fmt.Errorf("send Telegram message through generated API: %w", failure)
	}
	return nil
}
