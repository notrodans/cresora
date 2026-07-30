package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
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
		return classifyTargetFailure("resolve Telegram target", failure)
	}
	if target == nil {
		return application.WrapPermanent(errors.New("resolve Telegram target: target is nil"))
	}
	peer, failure := target.Peer()
	if failure != nil {
		return classifyTargetFailure("resolve Telegram input peer", failure)
	}
	if peer == nil {
		return application.WrapPermanent(errors.New("resolve Telegram input peer: peer is nil"))
	}
	var body strings.Builder
	if failure = message.Print(&body); failure != nil {
		return application.WrapUnknown(fmt.Errorf("render Telegram message body: %w", failure))
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
		return classifyTelegramFailure(failure)
	}
	return nil
}

func classifyTargetFailure(operation string, failure error) error {
	wrapped := fmt.Errorf("%s: %w", operation, failure)
	if errors.Is(failure, ErrTargetNotFound) || errors.Is(failure, ErrInvalidPeer) {
		return application.WrapPermanent(wrapped)
	}
	return application.WrapUnknown(wrapped)
}

// classifyTelegramFailure translates only the error returned by the generated
// API. In particular, it never invokes gotd's FloodWait helper, because that
// helper sleeps and retries inside the transport instead of exposing the
// server duration to delivery persistence.
func classifyTelegramFailure(failure error) error {
	if failure == nil {
		return nil
	}
	if duration, ok := tgerr.AsFloodWait(failure); ok {
		if duration <= 0 {
			return application.WrapUnknown(fmt.Errorf("invalid Telegram FloodWait duration: %w", failure))
		}
		return fmt.Errorf(
			"%w: send Telegram message: %w",
			&application.FloodWaitError{Duration: duration},
			failure,
		)
	}

	rpcFailure, ok := tgerr.As(failure)
	if !ok || rpcFailure == nil {
		return application.WrapUnknown(fmt.Errorf("send Telegram message: %w", failure))
	}
	if rpcFailure.IsType("RANDOM_ID_DUPLICATE") {
		// Telegram's duplicate response is ambiguous: the original request may
		// still be in flight, and this response is not a proof that this delivery
		// attempt reached the success finalizer. Preserve the gotd error so the
		// outcome remains visibly unknown to persistence.
		return application.WrapUnknown(fmt.Errorf("send Telegram message: %w", failure))
	}
	switch {
	case rpcFailure.Code >= 500 && rpcFailure.Code < 600:
		return application.WrapTransient(fmt.Errorf("send Telegram message: %w", failure))
	case rpcFailure.Code == 420:
		// A 420 that was not recognized as FloodWait has no safe retry
		// duration and is therefore unknown.
		return application.WrapUnknown(fmt.Errorf("send Telegram message: %w", failure))
	case rpcFailure.Code >= 400 && rpcFailure.Code < 500:
		return application.WrapPermanent(fmt.Errorf("send Telegram message: %w", failure))
	default:
		return application.WrapUnknown(fmt.Errorf("send Telegram message: %w", failure))
	}
}
