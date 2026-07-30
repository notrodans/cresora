// Package faketelegram provides a deterministic Telegram sender for tests.
// Configure a Fake with New and its functional options, such as WithScript
// and WithCallRecording.
package faketelegram

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/recipient"
)

var _ delivery.Port = (*Fake)(nil)

// DefaultMaxRecordedCalls bounds call memory when recording is enabled without
// an explicit limit.
const DefaultMaxRecordedCalls = 256

// Outcome describes the result a scripted Send call should have.
//
// These outcomes are deliberately fake-only. They do not model Telegram
// errors and are intended to exercise application retry and failure paths.
type Outcome uint8

const (
	OutcomeSuccess Outcome = iota
	OutcomeTransient
	OutcomePermanent
	OutcomeFloodWait
	OutcomeUnknown
)

// Step describes one scripted attempt. Latency is cancellable through the
// context passed to Send. FloodWait is used as the retry-after duration for a
// FloodWait outcome.
type Step struct {
	Outcome   Outcome
	Latency   time.Duration
	FloodWait time.Duration
}

type config struct {
	Script  []Step
	Scripts map[int64][]Step
	Default Step

	// Latency is the fallback latency for steps whose Latency is zero.
	Latency time.Duration

	// RecordCalls opts into recording attempts. A non-zero call limit also opts
	// in. Recording is bounded.
	RecordCalls      bool
	MaxRecordedCalls int
	RetainCallData   bool
}

// Option configures a Fake during construction.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (function optionFunc) apply(config *config) {
	function(config)
}

// WithScript supplies a fallback script.
func WithScript(steps ...Step) Option {
	return optionFunc(func(config *config) {
		config.Script = append([]Step(nil), steps...)
	})
}

// WithScriptFor supplies a script for one stable Telegram random ID.
func WithScriptFor(randomID int64, steps ...Step) Option {
	return optionFunc(func(config *config) {
		if config.Scripts == nil {
			config.Scripts = make(map[int64][]Step)
		}
		config.Scripts[randomID] = append([]Step(nil), steps...)
	})
}

// WithDefault supplies the result used after a script is exhausted.
func WithDefault(step Step) Option {
	return optionFunc(func(config *config) {
		config.Default = step
	})
}

// WithLatency supplies fallback latency for scripted steps.
func WithLatency(latency time.Duration) Option {
	return optionFunc(func(config *config) {
		config.Latency = latency
	})
}

// WithCallRecording opts into bounded call recording. A non-positive limit
// uses DefaultMaxRecordedCalls.
func WithCallRecording(limit int) Option {
	return optionFunc(func(config *config) {
		config.RecordCalls = true
		config.MaxRecordedCalls = limit
	})
}

// WithCallData opts into retaining a body and recipient ID in recorded calls
// and simulated effects. It is off by default so a test fake does not retain
// message contents or recipient identities accidentally.
func WithCallData() Option {
	return optionFunc(func(config *config) {
		config.RetainCallData = true
	})
}

// Call is a snapshot of one Send attempt. RecipientID and Body remain zero
// unless call-data retention was explicitly enabled. Error is the result of
// the attempt and is safe to inspect after Send returns.
type Call struct {
	RandomID    int64
	RecipientID uuid.UUID
	Body        string
	Outcome     Outcome
	Error       error
}

// Effect is one simulated external Telegram effect. Telegram's random ID is
// the idempotency key, so at most one Effect exists for any RandomID.
type Effect struct {
	RandomID    int64
	RecipientID uuid.UUID
	Body        string
}

// Fake is a deterministic, concurrency-safe implementation of
// delivery.Port. Its zero value is ready to use.
type Fake struct {
	mu sync.Mutex

	globalScript  []Step
	perIDScript   map[int64][]Step
	defaultStep   Step
	defaultLatent time.Duration

	recordCalls bool
	callLimit   int
	retainData  bool
	calls       []Call
	effects     []Effect
	effectIDs   map[int64]struct{}
}

// New constructs a fake from functional options.
func New(options ...Option) *Fake {
	config := config{}
	for _, option := range options {
		if option == nil {
			continue
		}
		option.apply(&config)
	}
	return newFake(config)
}

func newFake(config config) *Fake {
	if config.Latency < 0 {
		panic("configure fake Telegram with negative latency")
	}

	limit := config.MaxRecordedCalls
	if limit < 0 {
		panic("configure fake Telegram with negative call limit")
	}
	if limit == 0 {
		limit = DefaultMaxRecordedCalls
	}

	fake := &Fake{
		globalScript:  append([]Step(nil), config.Script...),
		perIDScript:   make(map[int64][]Step, len(config.Scripts)),
		defaultStep:   config.Default,
		defaultLatent: config.Latency,
		recordCalls:   config.RecordCalls || config.MaxRecordedCalls != 0,
		callLimit:     limit,
		retainData:    config.RetainCallData,
		effectIDs:     make(map[int64]struct{}),
	}
	for randomID, script := range config.Scripts {
		fake.perIDScript[randomID] = append([]Step(nil), script...)
	}

	validateStep(fake.defaultStep)
	for _, step := range fake.globalScript {
		validateStep(step)
	}
	for _, script := range fake.perIDScript {
		for _, step := range script {
			validateStep(step)
		}
	}
	return fake
}

func validateStep(step Step) {
	if step.Latency < 0 {
		panic("configure fake Telegram with negative step latency")
	}
	if step.FloodWait < 0 {
		panic("configure fake Telegram with negative flood wait")
	}
	if step.Outcome > OutcomeUnknown {
		panic("configure fake Telegram with unknown outcome")
	}
}

// Send implements delivery.Port.
func (fake *Fake) Send(
	context context.Context,
	target recipient.Recipient,
	body message.Message,
	randomID int64,
) error {
	if context == nil {
		panic("send fake Telegram message without context")
	}
	if randomID == 0 {
		panic("send fake Telegram message with zero random identity")
	}

	recipientID, messageBody := fake.callData(target, body)
	step, callIndex := fake.begin(randomID, recipientID, messageBody)
	if failure := wait(context, step.Latency); failure != nil {
		fake.finish(callIndex, step.Outcome, failure)
		return failure
	}

	var outcome Outcome
	var failure error
	fake.mu.Lock()
	if _, alreadyApplied := fake.effectIDs[randomID]; alreadyApplied {
		// A retry after an unknown outcome is a successful idempotent retry.
		// This also mirrors Telegram's random-ID deduplication after success.
		outcome = OutcomeSuccess
	} else {
		switch step.Outcome {
		case OutcomeSuccess:
			fake.recordEffectLocked(randomID, recipientID, messageBody)
			outcome = OutcomeSuccess
		case OutcomeTransient:
			outcome = OutcomeTransient
			failure = ErrTransient
		case OutcomePermanent:
			outcome = OutcomePermanent
			failure = ErrPermanent
		case OutcomeFloodWait:
			outcome = OutcomeFloodWait
			failure = &FloodWaitError{Duration: step.FloodWait}
		case OutcomeUnknown:
			fake.recordEffectLocked(randomID, recipientID, messageBody)
			outcome = OutcomeUnknown
			failure = ErrUnknownOutcome
		}
	}
	fake.mu.Unlock()

	fake.finish(callIndex, outcome, failure)
	return failure
}

func (fake *Fake) callData(target recipient.Recipient, body message.Message) (uuid.UUID, string) {
	if !fake.retainData {
		return uuid.Nil, ""
	}

	var recipientID uuid.UUID
	if target != nil {
		recipientID = target.UUID()
	}

	var messageBody strings.Builder
	if body != nil {
		if failure := body.Print(&messageBody); failure != nil {
			return recipientID, ""
		}
	}
	return recipientID, messageBody.String()
}

func (fake *Fake) begin(randomID int64, recipientID uuid.UUID, messageBody string) (Step, int) {
	fake.mu.Lock()
	defer fake.mu.Unlock()

	step := fake.nextStepLocked(randomID)
	if step.Latency == 0 {
		step.Latency = fake.defaultLatent
	}
	validateStep(step)

	callIndex := -1
	if fake.recordCalls && len(fake.calls) < fake.callLimit {
		fake.calls = append(fake.calls, Call{
			RandomID:    randomID,
			RecipientID: recipientID,
			Body:        messageBody,
			Outcome:     step.Outcome,
		})
		callIndex = len(fake.calls) - 1
	}
	return step, callIndex
}

func (fake *Fake) nextStepLocked(randomID int64) Step {
	if script := fake.perIDScript[randomID]; len(script) > 0 {
		step := script[0]
		fake.perIDScript[randomID] = script[1:]
		return step
	}
	if len(fake.globalScript) > 0 {
		step := fake.globalScript[0]
		fake.globalScript = fake.globalScript[1:]
		return step
	}
	return fake.defaultStep
}

func (fake *Fake) finish(callIndex int, outcome Outcome, failure error) {
	if callIndex < 0 {
		return
	}
	fake.mu.Lock()
	fake.calls[callIndex].Outcome = outcome
	fake.calls[callIndex].Error = failure
	fake.mu.Unlock()
}

func (fake *Fake) recordEffectLocked(randomID int64, recipientID uuid.UUID, messageBody string) {
	if fake.effectIDs == nil {
		fake.effectIDs = make(map[int64]struct{})
	}
	if _, exists := fake.effectIDs[randomID]; exists {
		return
	}
	fake.effectIDs[randomID] = struct{}{}
	fake.effects = append(fake.effects, Effect{
		RandomID:    randomID,
		RecipientID: recipientID,
		Body:        messageBody,
	})
}

// Calls returns a copy of the retained call records. It returns no records by
// default and never exposes the fake's mutable backing slice.
func (fake *Fake) Calls() []Call {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]Call(nil), fake.calls...)
}

// Effects returns a copy of simulated external effects. Effects are always
// tracked by random ID for idempotency, but body and recipient data are zero
// unless retention was enabled.
func (fake *Fake) Effects() []Effect {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]Effect(nil), fake.effects...)
}

// EffectCount reports how many simulated external effects were made for one
// random ID. The result is zero or one.
func (fake *Fake) EffectCount(randomID int64) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, exists := fake.effectIDs[randomID]; !exists {
		return 0
	}
	return 1
}

// These aliases keep the fake convenient for tests while making its outcomes
// use the same consumer-owned taxonomy as real transports.
var (
	ErrTransient      = delivery.ErrTransient
	ErrPermanent      = delivery.ErrPermanent
	ErrFloodWait      = delivery.ErrFloodWait
	ErrUnknownOutcome = delivery.ErrUnknownOutcome
)

type FloodWaitError = delivery.FloodWaitError

func wait(context context.Context, latency time.Duration) error {
	if failure := context.Err(); failure != nil {
		return failure
	}
	if latency == 0 {
		select {
		case <-context.Done():
			return context.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(latency)
	defer timer.Stop()
	select {
	case <-context.Done():
		return context.Err()
	case <-timer.C:
		return context.Err()
	}
}
