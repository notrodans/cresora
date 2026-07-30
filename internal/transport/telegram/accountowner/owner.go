// Package accountowner owns the lifetime of one gotd Telegram client.
package accountowner

import (
	"context"
	"errors"
	"sync"

	gotdtelegram "github.com/gotd/td/telegram"

	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

var (
	// ErrAlreadyRun indicates that the one-shot owner has already been started
	// or consumed by Stop.
	ErrAlreadyRun = errors.New("telegram account owner already run")
	// ErrStopped indicates that the owner completed without becoming ready.
	ErrStopped = errors.New("telegram account owner stopped")
)

// lifecycle is the only gotd behavior needed by Owner. Keeping it private
// leaves the production boundary concrete while allowing lifecycle tests to
// use a deterministic fake without widening a transport interface.
type lifecycle interface {
	Run(context.Context, func(context.Context) error) error
	Ready() <-chan struct{}
}

// Owner owns one factory-created gotd client. It deliberately exposes only
// lifecycle operations; callers cannot reach gotd's API, auth, or client.
type Owner struct {
	client lifecycle

	mu         sync.Mutex
	started    bool
	completed  bool
	cancel     context.CancelFunc
	result     error
	done       chan struct{}
	startedCh  chan struct{}
	stopping   bool
	stoppingCh chan struct{}
}

// New constructs an owner around exactly one client made by factory. The
// factory error is returned unchanged so invalid construction never produces
// a partially initialized owner.
func New(
	factory gotdclient.Factory,
	scope transporttelegram.SessionScope,
	appID int,
	appHash string,
) (*Owner, error) {
	// The tracker must exist before gotd construction: gotd can publish an
	// initial connection state as soon as Run starts, and readiness is a
	// current-state property rather than a one-shot Run callback.
	readiness := newReadinessTracker()
	client, err := factory.NewClientWithConnectionState(
		scope,
		appID,
		appHash,
		readiness.observe,
	)
	if err != nil {
		return nil, err
	}
	return newOwner(newGotdLifecycleWithReadiness(client, readiness)), nil
}

// newOwner is the private lifecycle seam used by unit tests.
func newOwner(client lifecycle) *Owner {
	return &Owner{
		client:     client,
		done:       make(chan struct{}),
		startedCh:  make(chan struct{}),
		stoppingCh: make(chan struct{}),
	}
}

// Run starts the owned gotd client once and joins its complete teardown. The
// callback intentionally does no work: gotd owns readiness and invokes it
// after session initialization, while the owner only keeps the client alive
// until gotd asks it to stop.
func (owner *Owner) Run(ctx context.Context) error {
	if ctx == nil {
		panic("run Telegram account owner without context")
	}
	if owner == nil {
		return errors.New("run Telegram account owner without owner")
	}

	runContext, cancel := context.WithCancel(ctx)
	owner.mu.Lock()
	if owner.started {
		owner.mu.Unlock()
		cancel()
		return ErrAlreadyRun
	}
	owner.started = true
	owner.cancel = cancel
	close(owner.startedCh)
	owner.mu.Unlock()

	failure := owner.client.Run(runContext, waitForCallbackContext)
	// Capture this before local cancellation. Once cancel is called, an
	// internal gotd failure can be indistinguishable from owner teardown.
	runContextCause := runContext.Err()
	cancel()
	failure = normalizeRunFailure(failure, runContextCause)
	owner.complete(failure)
	return failure
}

// Stop cancels the active gotd run. It is safe to call more than once. A stop
// before Run consumes the one-shot owner and makes subsequent Run calls fail
// with ErrAlreadyRun.
func (owner *Owner) Stop() {
	if owner == nil {
		return
	}

	owner.mu.Lock()
	if owner.started {
		if owner.completed {
			owner.mu.Unlock()
			return
		}
		cancel := owner.cancel
		if !owner.stopping {
			owner.stopping = true
			close(owner.stoppingCh)
		}
		owner.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}

	owner.started = true
	owner.stopping = true
	owner.completed = true
	owner.result = ErrStopped
	close(owner.startedCh)
	close(owner.stoppingCh)
	close(owner.done)
	owner.mu.Unlock()
}

// WaitReady waits for the current gotd readiness signal, caller cancellation,
// or owner shutdown. Ready is obtained for every call rather than cached:
// gotd replaces that channel when it reconnects.
func (owner *Owner) WaitReady(ctx context.Context) error {
	if ctx == nil {
		panic("wait for Telegram account owner readiness without context")
	}
	if owner == nil {
		return errors.New("wait for Telegram account owner readiness without owner")
	}

	for {
		owner.mu.Lock()
		stopping := owner.stopping
		started := owner.started
		completed := owner.completed
		owner.mu.Unlock()
		if stopping {
			return ErrStopped
		}
		if completed {
			return owner.completionResult()
		}
		if !started {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-owner.startedCh:
				continue
			case <-owner.stoppingCh:
				continue
			case <-owner.done:
				continue
			}
		}

		if waiter, ok := owner.client.(readinessWaiter); ok {
			failure := waiter.waitReady(ctx, owner.stoppingCh, owner.done)
			if failure == errReadinessCompleted {
				continue
			}
			if failure != nil {
				return failure
			}
			return owner.readyResult()
		}

		// Keep this fallback for deterministic lifecycle fakes. The production
		// adapter uses readinessWaiter so a channel replacement wakes the wait
		// and the next iteration observes the current channel.
		ready := owner.client.Ready()
		select {
		case <-ready:
			return owner.readyResult()
		case <-ctx.Done():
			return ctx.Err()
		case <-owner.stoppingCh:
			return ErrStopped
		case <-owner.done:
			continue
		}
	}
}

func (owner *Owner) complete(failure error) {
	owner.mu.Lock()
	owner.result = failure
	owner.completed = true
	close(owner.done)
	owner.mu.Unlock()
}

func (owner *Owner) completionResult() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.result != nil {
		return owner.result
	}
	return ErrStopped
}

func (owner *Owner) readyResult() error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.stopping {
		return ErrStopped
	}
	if !owner.completed {
		return nil
	}
	if owner.result != nil {
		return owner.result
	}
	return ErrStopped
}

func normalizeRunFailure(failure, runContextCause error) error {
	if failure == nil || runContextCause == nil {
		return failure
	}
	if !errors.Is(runContextCause, context.Canceled) &&
		!errors.Is(runContextCause, context.DeadlineExceeded) {
		return failure
	}
	if !errors.Is(failure, runContextCause) || !isPureContextCancellation(failure, runContextCause) {
		return failure
	}
	return nil
}

func isPureContextCancellation(failure, expectedCause error) bool {
	if failure == nil {
		return false
	}
	if multiple, ok := failure.(interface{ Unwrap() []error }); ok {
		causes := multiple.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isPureContextCancellation(cause, expectedCause) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(failure); cause != nil {
		return isPureContextCancellation(cause, expectedCause)
	}
	return errors.Is(failure, expectedCause)
}

func waitForCallbackContext(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

var (
	errReadinessCompleted = errors.New("gotd readiness wait completed")
)

// readinessWaiter is an optional production-only extension to lifecycle. It
// lets WaitReady observe tracker generations without changing the narrow
// lifecycle fake used by unit tests.
type readinessWaiter interface {
	waitReady(context.Context, <-chan struct{}, <-chan struct{}) error
}

// readinessTracker models the current gotd connection state. A reset replaces
// the readiness channel and closes changes, ensuring a waiter never remains
// blocked on a discarded generation.
type readinessTracker struct {
	mu sync.Mutex

	state   gotdtelegram.ConnectionState
	ready   chan struct{}
	changes chan struct{}
	closed  bool
}

func newReadinessTracker() *readinessTracker {
	return &readinessTracker{
		state:   gotdtelegram.ConnectionStateConnecting,
		ready:   make(chan struct{}),
		changes: make(chan struct{}),
	}
}

func (tracker *readinessTracker) observe(state gotdtelegram.ConnectionState) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.state = state
	if state == gotdtelegram.ConnectionStateReady {
		if !tracker.closed {
			close(tracker.ready)
			tracker.closed = true
		}
	} else {
		tracker.ready = make(chan struct{})
		tracker.closed = false
	}

	previousChanges := tracker.changes
	tracker.changes = make(chan struct{})
	close(previousChanges)
}

func (tracker *readinessTracker) Ready() <-chan struct{} {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.ready
}

func (tracker *readinessTracker) snapshot() (<-chan struct{}, <-chan struct{}, bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.ready, tracker.changes, tracker.state == gotdtelegram.ConnectionStateReady
}

func (tracker *readinessTracker) waitReady(
	ctx context.Context,
	stopping <-chan struct{},
	done <-chan struct{},
) error {
	for {
		ready, changes, isReady := tracker.snapshot()
		if isReady {
			return nil
		}
		select {
		case <-ready:
			// Verify the state on the next iteration. This matters when a
			// reconnect resets the tracker after an earlier ready channel was
			// closed.
		case <-changes:
		case <-ctx.Done():
			return ctx.Err()
		case <-stopping:
			return ErrStopped
		case <-done:
			return errReadinessCompleted
		}
	}
}

// gotdLifecycle adapts the concrete gotd client to lifecycle. Readiness is
// driven by Options.OnConnectionState, not gotd's one-shot Run callback.
type gotdLifecycle struct {
	client    *gotdtelegram.Client
	readiness *readinessTracker
}

func newGotdLifecycle(client *gotdtelegram.Client) *gotdLifecycle {
	return newGotdLifecycleWithReadiness(client, newReadinessTracker())
}

func newGotdLifecycleWithReadiness(
	client *gotdtelegram.Client,
	readiness *readinessTracker,
) *gotdLifecycle {
	return &gotdLifecycle{
		client:    client,
		readiness: readiness,
	}
}

func (client *gotdLifecycle) Run(
	ctx context.Context,
	callback func(context.Context) error,
) error {
	return client.client.Run(ctx, callback)
}

func (client *gotdLifecycle) Ready() <-chan struct{} {
	return client.readiness.Ready()
}

func (client *gotdLifecycle) waitReady(
	ctx context.Context,
	stopping <-chan struct{},
	done <-chan struct{},
) error {
	return client.readiness.waitReady(ctx, stopping, done)
}
