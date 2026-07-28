package faketelegram

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

func TestFake_ScriptsFakeOnlyFailures(t *testing.T) {
	fake := New(
		WithScriptFor(1, Step{Outcome: OutcomeTransient}),
		WithScriptFor(2, Step{Outcome: OutcomePermanent}),
		WithScriptFor(3, Step{Outcome: OutcomeFloodWait, FloodWait: 3 * time.Second}),
		WithScriptFor(4, Step{Outcome: OutcomeUnknown}),
	)

	if failure := fake.Send(context.Background(), nil, nil, 1); !errors.Is(failure, ErrTransient) {
		t.Fatalf("send transient scenario: got %v, want %v", failure, ErrTransient)
	}
	if failure := fake.Send(context.Background(), nil, nil, 2); !errors.Is(failure, ErrPermanent) {
		t.Fatalf("send permanent scenario: got %v, want %v", failure, ErrPermanent)
	}

	failure := fake.Send(context.Background(), nil, nil, 3)
	if !errors.Is(failure, ErrFloodWait) {
		t.Fatalf("send flood-wait scenario: got %v, want an error matching %v", failure, ErrFloodWait)
	}
	var floodWait *FloodWaitError
	if !errors.As(failure, &floodWait) {
		t.Fatalf("send flood-wait scenario: got %T, want *FloodWaitError", failure)
	}
	if floodWait.RetryAfter() != 3*time.Second {
		t.Fatalf("flood-wait retry duration: got %s, want %s", floodWait.RetryAfter(), 3*time.Second)
	}

	if failure := fake.Send(context.Background(), nil, nil, 4); !errors.Is(failure, ErrUnknownOutcome) {
		t.Fatalf("send unknown scenario: got %v, want %v", failure, ErrUnknownOutcome)
	}
}

func TestFake_UnknownOutcomeDeduplicatesByRandomID(t *testing.T) {
	fake := New(
		WithScriptFor(11, Step{Outcome: OutcomeUnknown}),
		WithScriptFor(22, Step{Outcome: OutcomeUnknown}),
	)

	if failure := fake.Send(context.Background(), nil, nil, 11); !errors.Is(failure, ErrUnknownOutcome) {
		t.Fatalf("first send: got %v, want %v", failure, ErrUnknownOutcome)
	}
	if failure := fake.Send(context.Background(), nil, nil, 11); failure != nil {
		t.Fatalf("retry with the same random ID: got %v, want nil", failure)
	}
	if fake.EffectCount(11) != 1 {
		t.Fatalf("effect count for random ID 11: got %d, want 1", fake.EffectCount(11))
	}

	if failure := fake.Send(context.Background(), nil, nil, 22); !errors.Is(failure, ErrUnknownOutcome) {
		t.Fatalf("send with a different random ID: got %v, want %v", failure, ErrUnknownOutcome)
	}
	if got := len(fake.Effects()); got != 2 {
		t.Fatalf("number of effects: got %d, want 2", got)
	}
}

func TestFake_CancellationStopsScriptedLatency(t *testing.T) {
	fake := New(WithScript(Step{Latency: time.Hour}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan struct{})
	ctx = &observingContext{Context: ctx, observed: observed}

	result := make(chan error, 1)
	go func() {
		result <- fake.Send(ctx, nil, nil, 1)
	}()
	<-observed
	cancel()

	if failure := <-result; !errors.Is(failure, context.Canceled) {
		t.Fatalf("canceled send: got %v, want %v", failure, context.Canceled)
	}
	if got := len(fake.Effects()); got != 0 {
		t.Fatalf("effects after cancellation: got %d, want 0", got)
	}
}

func TestFake_DoesNotRetainCallDataByDefault(t *testing.T) {
	recipientID := uuid.New()
	fake := New(WithCallRecording(2))

	for randomID := int64(1); randomID <= 3; randomID++ {
		if failure := fake.Send(
			context.Background(),
			recipient.Identity(recipientID),
			message.Text("message body"),
			randomID,
		); failure != nil {
			t.Fatalf("send %d: %v", randomID, failure)
		}
	}

	calls := fake.Calls()
	if len(calls) != 2 {
		t.Fatalf("recorded call count: got %d, want 2", len(calls))
	}
	if calls[0].RecipientID != uuid.Nil || calls[0].Body != "" {
		t.Fatalf("default call data retention: got recipient %v and body %q, want zero values", calls[0].RecipientID, calls[0].Body)
	}
	effects := fake.Effects()
	if len(effects) != 3 {
		t.Fatalf("effect count: got %d, want 3", len(effects))
	}
	if effects[0].RecipientID != uuid.Nil || effects[0].Body != "" {
		t.Fatalf("default effect data retention: got recipient %v and body %q, want zero values", effects[0].RecipientID, effects[0].Body)
	}
}

func TestFake_RetainsCallDataWhenRequested(t *testing.T) {
	recipientID := uuid.New()
	fake := New(WithCallRecording(0), WithCallData())

	if failure := fake.Send(
		context.Background(),
		recipient.Identity(recipientID),
		message.Text("message body"),
		1,
	); failure != nil {
		t.Fatalf("send: %v", failure)
	}

	calls := fake.Calls()
	if len(calls) != 1 {
		t.Fatalf("recorded call count: got %d, want 1", len(calls))
	}
	if calls[0].RecipientID != recipientID || calls[0].Body != "message body" {
		t.Fatalf("retained call data: got recipient %v and body %q, want %v and %q", calls[0].RecipientID, calls[0].Body, recipientID, "message body")
	}

	effects := fake.Effects()
	if effects[0].RecipientID != recipientID || effects[0].Body != "message body" {
		t.Fatalf("retained effect data: got recipient %v and body %q, want %v and %q", effects[0].RecipientID, effects[0].Body, recipientID, "message body")
	}
}

func TestFake_IsSafeForConcurrentSends(t *testing.T) {
	fake := New(WithCallRecording(64), WithDefault(Step{Outcome: OutcomeUnknown}))

	const sends = 64
	var waitGroup sync.WaitGroup
	waitGroup.Add(sends)
	for index := 0; index < sends; index++ {
		randomID := int64(index + 1)
		go func() {
			defer waitGroup.Done()
			_ = fake.Send(context.Background(), nil, nil, randomID)
			_ = fake.Send(context.Background(), nil, nil, randomID)
		}()
	}
	waitGroup.Wait()

	if got := len(fake.Effects()); got != sends {
		t.Fatalf("concurrent effect count: got %d, want %d", got, sends)
	}
	if got := len(fake.Calls()); got != 64 {
		t.Fatalf("bounded concurrent call count: got %d, want 64", got)
	}
}

type observingContext struct {
	context.Context
	observed chan struct{}
	one      sync.Once
}

func (context *observingContext) Done() <-chan struct{} {
	context.one.Do(func() { close(context.observed) })
	return context.Context.Done()
}
