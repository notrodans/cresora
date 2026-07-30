package accountowner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	gotdtelegram "github.com/gotd/td/telegram"

	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

var errFakeRun = errors.New("fake gotd run failed")
var errMixedRun = errors.New("gotd teardown failed independently")

type fakeLifecycle struct {
	mu sync.Mutex

	ready       chan struct{}
	readyClosed bool
	runCalls    int
	runFailure  error

	runEntered       chan struct{}
	callbackEntered  chan struct{}
	callbackReturned chan struct{}
	callbackFailure  error
}

func newFakeLifecycle() *fakeLifecycle {
	return &fakeLifecycle{
		ready:            make(chan struct{}),
		runEntered:       make(chan struct{}),
		callbackEntered:  make(chan struct{}),
		callbackReturned: make(chan struct{}),
	}
}

func (fake *fakeLifecycle) Run(
	ctx context.Context,
	callback func(context.Context) error,
) error {
	fake.mu.Lock()
	fake.runCalls++
	if fake.runCalls == 1 {
		close(fake.runEntered)
	}
	ready := fake.ready
	runFailure := fake.runFailure
	fake.mu.Unlock()

	if runFailure != nil {
		return runFailure
	}

	select {
	case <-ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	callbackContext, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		close(fake.callbackEntered)
		failure := callback(callbackContext)
		fake.mu.Lock()
		fake.callbackFailure = failure
		fake.mu.Unlock()
		close(fake.callbackReturned)
	}()

	select {
	case <-fake.callbackReturned:
		return nil
	case <-ctx.Done():
		cancel()
		<-fake.callbackReturned
		return ctx.Err()
	}
}

func (fake *fakeLifecycle) Ready() <-chan struct{} {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.ready
}

func (fake *fakeLifecycle) signalReady() {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.readyClosed {
		close(fake.ready)
		fake.readyClosed = true
	}
}

func (fake *fakeLifecycle) resetReady() {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.ready = make(chan struct{})
	fake.readyClosed = false
}

func (fake *fakeLifecycle) calls() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.runCalls
}

func (fake *fakeLifecycle) callbackErr() error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.callbackFailure
}

type blockedLifecycle struct {
	runEntered chan struct{}
	release    chan struct{}
	ready      chan struct{}
}

func newBlockedLifecycle() *blockedLifecycle {
	return &blockedLifecycle{
		runEntered: make(chan struct{}),
		release:    make(chan struct{}),
		ready:      make(chan struct{}),
	}
}

func (blocked *blockedLifecycle) Run(ctx context.Context, _ func(context.Context) error) error {
	close(blocked.runEntered)
	<-ctx.Done()
	<-blocked.release
	return ctx.Err()
}

func (blocked *blockedLifecycle) Ready() <-chan struct{} {
	return blocked.ready
}

type joinedCancellationLifecycle struct {
	runEntered chan struct{}
	failure    error
}

func (joined *joinedCancellationLifecycle) Run(ctx context.Context, _ func(context.Context) error) error {
	close(joined.runEntered)
	<-ctx.Done()
	return joined.failure
}

func (joined *joinedCancellationLifecycle) Ready() <-chan struct{} {
	return make(chan struct{})
}

func awaitChannel(t *testing.T, channel <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func awaitError(t *testing.T, errors <-chan error, name string) error {
	t.Helper()
	select {
	case failure := <-errors:
		return failure
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func TestNewPropagatesFactoryConstructionFailure(t *testing.T) {
	owner, failure := New(
		gotdclient.New(nil),
		transporttelegram.SessionScope{
			OperatorID: uuid.New(),
			AccountID:  uuid.New(),
		},
		123,
		"app-hash",
	)
	if owner != nil {
		t.Fatal("New() returned an owner after construction failed")
	}
	if failure == nil {
		t.Fatal("New() error = nil, want factory construction failure")
	}
}

func TestOwnerRunIsOneShot(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	firstRun := make(chan error, 1)
	go func() { firstRun <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "first gotd Run")

	if failure := owner.Run(context.Background()); !errors.Is(failure, ErrAlreadyRun) {
		t.Fatalf("concurrent Run() error = %v, want ErrAlreadyRun", failure)
	}

	fake.signalReady()
	owner.Stop()
	if failure := awaitError(t, firstRun, "first Run teardown"); failure != nil {
		t.Fatalf("first Run() error = %v, want nil after Stop", failure)
	}
	if failure := owner.Run(context.Background()); !errors.Is(failure, ErrAlreadyRun) {
		t.Fatalf("repeated Run() error = %v, want ErrAlreadyRun", failure)
	}
	if calls := fake.calls(); calls != 1 {
		t.Fatalf("gotd Run call count = %d, want 1", calls)
	}
}

func TestOwnerParentCancellationPropagatesAndJoins(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	parent, cancelParent := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(parent) }()
	awaitChannel(t, fake.runEntered, "gotd Run")
	fake.signalReady()
	awaitChannel(t, fake.callbackEntered, "gotd callback")

	cancelParent()
	if failure := awaitError(t, runResult, "parent-canceled Run teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil after parent cancellation", failure)
	}
	awaitChannel(t, fake.callbackReturned, "callback teardown")
}

func TestOwnerStopIsIdempotentAndJoinsTeardown(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "gotd Run")
	fake.signalReady()
	awaitChannel(t, fake.callbackEntered, "gotd callback")

	owner.Stop()
	owner.Stop()
	if failure := awaitError(t, runResult, "Stop teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil after Stop", failure)
	}
	awaitChannel(t, fake.callbackReturned, "callback teardown")
}

func TestOwnerWaitReadySuccess(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "gotd Run")

	readyResult := make(chan error, 1)
	go func() { readyResult <- owner.WaitReady(context.Background()) }()
	fake.signalReady()
	if failure := awaitError(t, readyResult, "readiness"); failure != nil {
		t.Fatalf("WaitReady() error = %v, want nil", failure)
	}

	owner.Stop()
	if failure := awaitError(t, runResult, "Run teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil", failure)
	}
}

func TestOwnerWaitReadyCallerCancellation(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "gotd Run")

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if failure := owner.WaitReady(ctx); !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("WaitReady() error = %v, want deadline exceeded", failure)
	}

	owner.Stop()
	if failure := awaitError(t, runResult, "Run teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil", failure)
	}
}

func TestOwnerWaitReadyAfterShutdown(t *testing.T) {
	owner := newOwner(newFakeLifecycle())
	owner.Stop()

	if failure := owner.WaitReady(context.Background()); !errors.Is(failure, ErrStopped) {
		t.Fatalf("WaitReady() error = %v, want ErrStopped", failure)
	}
	if failure := owner.Run(context.Background()); !errors.Is(failure, ErrAlreadyRun) {
		t.Fatalf("Run() error = %v, want ErrAlreadyRun", failure)
	}
}

func TestOwnerWaitReadyUsesCurrentReadyChannel(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "gotd Run")

	fake.signalReady()
	if failure := owner.WaitReady(context.Background()); failure != nil {
		t.Fatalf("first WaitReady() error = %v, want nil", failure)
	}

	fake.resetReady()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	if failure := owner.WaitReady(ctx); !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("WaitReady() on reset channel error = %v, want deadline exceeded", failure)
	}
	cancel()

	fake.signalReady()
	if failure := owner.WaitReady(context.Background()); failure != nil {
		t.Fatalf("second WaitReady() error = %v, want nil", failure)
	}
	owner.Stop()
	if failure := awaitError(t, runResult, "Run teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil", failure)
	}
}

func TestReadinessTrackerStartupAndReconnectUseCurrentGeneration(t *testing.T) {
	tracker := newReadinessTracker()
	oldReady := tracker.Ready()
	stopping := make(chan struct{})
	done := make(chan struct{})
	readyResult := make(chan error, 1)
	go func() { readyResult <- tracker.waitReady(context.Background(), stopping, done) }()

	tracker.observe(gotdtelegram.ConnectionStateDisconnected)
	select {
	case <-oldReady:
		t.Fatal("discarded readiness generation was signaled")
	default:
	}
	tracker.observe(gotdtelegram.ConnectionStateReady)
	if failure := awaitError(t, readyResult, "initial readiness"); failure != nil {
		t.Fatalf("waitReady() initial error = %v, want nil", failure)
	}

	tracker.observe(gotdtelegram.ConnectionStateConnecting)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	if failure := tracker.waitReady(ctx, stopping, done); !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("waitReady() during reconnect error = %v, want deadline exceeded", failure)
	}
	cancel()

	tracker.observe(gotdtelegram.ConnectionStateReady)
	if failure := tracker.waitReady(context.Background(), stopping, done); failure != nil {
		t.Fatalf("waitReady() after reconnect error = %v, want nil", failure)
	}
}

func TestOwnerRunFailureIsPreserved(t *testing.T) {
	fake := newFakeLifecycle()
	fake.runFailure = errFakeRun
	owner := newOwner(fake)

	if failure := owner.Run(context.Background()); !errors.Is(failure, errFakeRun) {
		t.Fatalf("Run() error = %v, want fake failure", failure)
	}
	if failure := owner.WaitReady(context.Background()); !errors.Is(failure, errFakeRun) {
		t.Fatalf("WaitReady() error = %v, want fake failure", failure)
	}
}

func TestOwnerInternalDeadlineWhileRunContextIsLiveIsPreserved(t *testing.T) {
	fake := newFakeLifecycle()
	fake.runFailure = context.DeadlineExceeded
	owner := newOwner(fake)

	if failure := owner.Run(context.Background()); !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want internal deadline exceeded", failure)
	}
}

func TestOwnerMixedCancellationFailureIsPreserved(t *testing.T) {
	joined := &joinedCancellationLifecycle{
		runEntered: make(chan struct{}),
		failure:    errors.Join(errMixedRun, context.Canceled),
	}
	owner := newOwner(joined)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, joined.runEntered, "joined-failure Run")

	owner.Stop()
	failure := awaitError(t, runResult, "joined-failure teardown")
	if !errors.Is(failure, errMixedRun) || !errors.Is(failure, context.Canceled) {
		t.Fatalf("Run() error = %v, want both independent failures", failure)
	}
}

func TestNormalizeRunFailurePreservesOppositeJoinedContextSentinel(t *testing.T) {
	tests := []struct {
		name            string
		runContextCause error
		failure         error
		oppositeCause   error
	}{
		{
			name:            "canceled owner with deadline failure",
			runContextCause: context.Canceled,
			failure:         errors.Join(context.Canceled, context.DeadlineExceeded),
			oppositeCause:   context.DeadlineExceeded,
		},
		{
			name:            "deadline owner with canceled failure",
			runContextCause: context.DeadlineExceeded,
			failure:         errors.Join(context.DeadlineExceeded, context.Canceled),
			oppositeCause:   context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := normalizeRunFailure(test.failure, test.runContextCause)
			if failure == nil {
				t.Fatal("normalizeRunFailure() returned nil for an opposite joined context sentinel")
			} else if failure != test.failure {
				t.Fatalf("normalizeRunFailure() error = %v, want original joined failure", failure)
			} else if !errors.Is(failure, test.oppositeCause) {
				t.Fatalf("normalizeRunFailure() error = %v, want opposite context sentinel", failure)
			}
		})
	}
}

func TestOwnerCallbackIsInertUntilCancellation(t *testing.T) {
	fake := newFakeLifecycle()
	owner := newOwner(fake)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, fake.runEntered, "gotd Run")
	fake.signalReady()
	awaitChannel(t, fake.callbackEntered, "gotd callback")

	select {
	case <-fake.callbackReturned:
		t.Fatal("owner callback returned before cancellation")
	case <-time.After(time.Millisecond):
	}

	owner.Stop()
	if failure := awaitError(t, runResult, "Run teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil", failure)
	}
	if failure := fake.callbackErr(); failure != nil {
		t.Fatalf("owner callback error = %v, want nil", failure)
	}
}

func TestOwnerRunStopWaitReadyRace(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		fake := newFakeLifecycle()
		owner := newOwner(fake)
		runResult := make(chan error, 1)
		readyResult := make(chan error, 1)

		go func() { runResult <- owner.Run(context.Background()) }()
		go func() { readyResult <- owner.WaitReady(context.Background()) }()
		go owner.Stop()

		runFailure := awaitError(t, runResult, "raced Run")
		if runFailure != nil && !errors.Is(runFailure, ErrAlreadyRun) {
			t.Fatalf("raced Run() error = %v, want nil or ErrAlreadyRun", runFailure)
		}
		if failure := awaitError(t, readyResult, "raced WaitReady"); !errors.Is(failure, ErrStopped) {
			t.Fatalf("raced WaitReady() error = %v, want ErrStopped", failure)
		}
	}
}

func TestOwnerStopPublishesStoppingBeforeTeardownCompletes(t *testing.T) {
	blocked := newBlockedLifecycle()
	owner := newOwner(blocked)
	runResult := make(chan error, 1)
	go func() { runResult <- owner.Run(context.Background()) }()
	awaitChannel(t, blocked.runEntered, "blocked gotd Run")

	owner.Stop()
	readyContext, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()
	if failure := owner.WaitReady(readyContext); !errors.Is(failure, ErrStopped) {
		t.Fatalf("WaitReady() after Stop = %v, want ErrStopped before teardown", failure)
	}

	close(blocked.release)
	if failure := awaitError(t, runResult, "blocked teardown"); failure != nil {
		t.Fatalf("Run() error = %v, want nil after teardown", failure)
	}
}
