package telegram_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	delivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
	telegram "github.com/notrodans/cresora/internal/transport/telegram"
)

type senderAPI struct {
	failure error
	calls   int
}

func (api *senderAPI) MessagesSendMessage(
	context.Context,
	*tg.MessagesSendMessageRequest,
) (tg.UpdatesClass, error) {
	api.calls++
	return nil, api.failure
}

type senderTargets struct {
	target  telegram.Target
	failure error
}

func (targets senderTargets) Target(context.Context, recipient.Recipient) (telegram.Target, error) {
	return targets.target, targets.failure
}

type senderTarget struct {
	peer    tg.InputPeerClass
	failure error
}

func (target senderTarget) Peer() (tg.InputPeerClass, error) {
	return target.peer, target.failure
}

func TestSenderMapsReturnedTelegramErrorsWithoutSleeping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind delivery.FailureKind
		wait time.Duration
	}{
		{
			name: "flood wait",
			err:  tgerr.New(420, "FLOOD_WAIT_3"),
			kind: delivery.FailureFloodWait,
			wait: 3 * time.Second,
		},
		{
			name: "server error",
			err:  tgerr.New(500, "INTERNAL"),
			kind: delivery.FailureTransient,
		},
		{
			name: "client error",
			err:  tgerr.New(400, "CHAT_WRITE_FORBIDDEN"),
			kind: delivery.FailurePermanent,
		},
		{
			name: "unmatched rate limit",
			err:  tgerr.New(420, "SOMETHING_ELSE"),
			kind: delivery.FailureUnknown,
		},
		{
			name: "network error",
			err:  errors.New("connection reset"),
			kind: delivery.FailureUnknown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := &senderAPI{failure: test.err}
			sender := telegram.New(api, senderTargets{
				target: senderTarget{peer: &tg.InputPeerUser{UserID: 1, AccessHash: 2}},
			})
			started := time.Now()
			failure := sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
			if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
				t.Fatalf("sender slept for %s while inspecting returned error", elapsed)
			}
			if classification := delivery.Classify(failure); classification.Kind != test.kind || classification.RetryAfter != test.wait {
				t.Fatalf("classification = %+v, want kind %d and delay %s", classification, test.kind, test.wait)
			}
			if test.name == "flood wait" {
				var rpcFailure *tgerr.Error
				if !errors.As(failure, &rpcFailure) || rpcFailure == nil {
					t.Fatalf("FloodWait error did not preserve gotd error: %v", failure)
				}
			}
			if api.calls != 1 {
				t.Fatalf("API calls = %d, want 1", api.calls)
			}
		})
	}
}

func TestSenderTreatsRandomIDDuplicateAsUnknown(t *testing.T) {
	underlying := tgerr.New(400, "RANDOM_ID_DUPLICATE")
	api := &senderAPI{failure: underlying}
	sender := telegram.New(api, senderTargets{
		target: senderTarget{peer: &tg.InputPeerUser{UserID: 1, AccessHash: 2}},
	})
	failure := sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if failure == nil {
		t.Fatal("duplicate random ID returned nil, want unknown outcome")
	}
	if classification := delivery.Classify(failure); classification.Kind != delivery.FailureUnknown {
		t.Fatalf("duplicate random ID classification = %+v, want unknown", classification)
	}
	if !errors.Is(failure, underlying) {
		t.Fatalf("duplicate random ID error = %v, want original gotd error preserved", failure)
	}
}

func TestSenderClassifiesTargetAndPeerFailuresByEvidence(t *testing.T) {
	underlying := errors.New("target lookup failed")
	sender := telegram.New(&senderAPI{}, senderTargets{failure: underlying})
	failure := sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrUnknownOutcome) || !errors.Is(failure, underlying) {
		t.Fatalf("operational target failure = %v, want unknown taxonomy and original error", failure)
	}

	peerFailure := errors.New("peer unavailable")
	sender = telegram.New(&senderAPI{}, senderTargets{
		target: senderTarget{failure: peerFailure},
	})
	failure = sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrUnknownOutcome) || !errors.Is(failure, peerFailure) {
		t.Fatalf("operational peer failure = %v, want unknown taxonomy and original error", failure)
	}

	semanticTargetFailure := fmt.Errorf("lookup: %w", telegram.ErrTargetNotFound)
	sender = telegram.New(&senderAPI{}, senderTargets{failure: semanticTargetFailure})
	failure = sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrPermanent) || !errors.Is(failure, semanticTargetFailure) {
		t.Fatalf("semantic target failure = %v, want permanent taxonomy and original error", failure)
	}

	semanticPeerFailure := fmt.Errorf("projection: %w", telegram.ErrInvalidPeer)
	sender = telegram.New(&senderAPI{}, senderTargets{
		target: senderTarget{failure: semanticPeerFailure},
	})
	failure = sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrPermanent) || !errors.Is(failure, semanticPeerFailure) {
		t.Fatalf("semantic peer failure = %v, want permanent taxonomy and original error", failure)
	}

	sender = telegram.New(&senderAPI{}, senderTargets{})
	failure = sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrPermanent) {
		t.Fatalf("nil target = %v, want permanent structural failure", failure)
	}

	sender = telegram.New(&senderAPI{}, senderTargets{target: senderTarget{}})
	failure = sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrPermanent) {
		t.Fatalf("nil peer = %v, want permanent structural failure", failure)
	}
}

func TestSenderRejectsNotSendableTargetWithoutRPC(t *testing.T) {
	api := &senderAPI{}
	sender := telegram.New(api, senderTargets{
		failure: fmt.Errorf("project shared channel: %w", telegram.ErrTargetNotSendable),
	})

	failure := sender.Send(context.Background(), recipient.Identity(uuid.New()), message.Text("hello"), 1)
	if !errors.Is(failure, delivery.ErrPermanent) {
		t.Fatalf("not-sendable target = %v, want permanent failure", failure)
	}
	if api.calls != 0 {
		t.Fatalf("API calls = %d, want 0 for a denied target", api.calls)
	}
}
