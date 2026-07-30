package challenges

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type testProvider struct {
	mu sync.Mutex

	sequence int
	verify   atomic.Int32
	cancel   atomic.Int32
	code     string

	startEntered chan struct{}
	startRelease chan struct{}
}

func (provider *testProvider) StartPhone(ctx context.Context, request PhoneStart) (PhoneStarted, error) {
	if provider.startEntered != nil {
		closeOnce(provider.startEntered)
		select {
		case <-provider.startRelease:
		case <-ctx.Done():
			// This fake deliberately returns a late successful result so the
			// coordinator must discard it after cancellation.
		}
	}
	provider.mu.Lock()
	provider.sequence++
	value := fmt.Sprintf("phone-secret-%d", provider.sequence)
	provider.mu.Unlock()
	return PhoneStarted{Handle: NewProviderHandle(value), Delivery: "test SMS"}, nil
}

func (provider *testProvider) VerifyPhone(ctx context.Context, handle ProviderHandle, code string) (PhoneVerified, error) {
	if err := ctx.Err(); err != nil {
		return PhoneVerified{}, err
	}
	provider.verify.Add(1)
	if code != provider.code {
		return PhoneVerified{}, errors.New("provider details must not escape")
	}
	return PhoneVerified{}, nil
}

func (provider *testProvider) CancelPhone(ctx context.Context, handle ProviderHandle) error {
	provider.cancel.Add(1)
	return nil
}

func (provider *testProvider) StartQR(ctx context.Context, _ QRStart) (QRStarted, error) {
	provider.mu.Lock()
	provider.sequence++
	value := fmt.Sprintf("qr-secret-%d", provider.sequence)
	provider.mu.Unlock()
	return QRStarted{Handle: NewProviderHandle(value), URL: "tg://login?token=public-test-token"}, nil
}

func (provider *testProvider) RefreshQR(ctx context.Context, handle ProviderHandle) (QRStarted, error) {
	return provider.StartQR(ctx, QRStart{})
}

func (provider *testProvider) CancelQR(ctx context.Context, handle ProviderHandle) error {
	provider.cancel.Add(1)
	return nil
}

func closeOnce(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func shutdownCoordinatorForTest(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if failure := coordinator.Shutdown(shutdownContext); failure != nil {
			t.Errorf("cleanup executor did not stop: %v", failure)
		}
	})
}

func newCoordinatorForTest(t *testing.T, clock *testClock, provider interface {
	PhoneProvider
	QRProvider
}, options ...Option) *Coordinator {
	t.Helper()
	all := []Option{WithClock(clock.Now), WithRequestIDGenerator(uuid.New)}
	all = append(all, options...)
	config := Config{PhoneProvider: provider, QRProvider: provider}
	for _, option := range all {
		option(&config)
	}
	coordinator := New(config)
	shutdownCoordinatorForTest(t, coordinator)
	return coordinator
}

func actor() applicationroot.Actor { return applicationroot.Actor{OperatorID: uuid.New()} }

func TestCoordinatorScopesSlotsAndPreservesDuplicate(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider)
	firstActor := actor()
	secondActor := actor()

	first, err := coordinator.StartPhoneChallenge(context.Background(), firstActor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.StartPhoneChallenge(context.Background(), firstActor, "+15557654321"); !errors.Is(err, ErrChallengeAlreadyActive) {
		t.Fatalf("duplicate phone start error = %v", err)
	}
	current, err := coordinator.Query(context.Background(), firstActor)
	if err != nil || current.Phone == nil || current.Phone.RequestID != first.RequestID || current.Phone.Phone != first.Phone {
		t.Fatalf("duplicate replaced current phone challenge: %+v, %v", current.Phone, err)
	}
	if _, err := coordinator.StartQRChallenge(context.Background(), firstActor); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.StartPhoneChallenge(context.Background(), secondActor, "+15557654321"); err != nil {
		t.Fatal(err)
	}
	other, err := coordinator.Query(context.Background(), secondActor)
	if err != nil || other.Phone == nil || other.Phone.RequestID == first.RequestID || other.QR != nil {
		t.Fatalf("actor scope leaked: %+v", other)
	}
}

func TestCoordinatorTTLAndRestartAreEmpty(t *testing.T) {
	clock := &testClock{now: time.Unix(200, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider, WithPhoneTTL(time.Minute), WithQRTTL(time.Minute))
	operator := actor()
	if _, err := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567"); err != nil {
		t.Fatal(err)
	}
	// Advance beyond the configured TTL rather than landing exactly on its
	// boundary; expiry policy is intentionally expressed as "not before".
	clock.Advance(2 * time.Minute)
	status, err := coordinator.Query(context.Background(), operator)
	if err != nil || status.Phone != nil {
		t.Fatalf("expired phone remains visible: %+v, %v", status, err)
	}
	if _, err := coordinator.StartPhoneChallenge(context.Background(), operator, "+15557654321"); err != nil {
		t.Fatalf("expired slot was not lazily reclaimed: %v", err)
	}
	restartedCoordinator := New(Config{PhoneProvider: provider, QRProvider: provider})
	shutdownCoordinatorForTest(t, restartedCoordinator)
	if restarted, err := restartedCoordinator.Query(context.Background(), operator); err != nil || restarted.Phone != nil || restarted.QR != nil {
		t.Fatalf("new coordinator retained process state: %+v, %v", restarted, err)
	}
}

func TestCoordinatorAttemptsAreAtomicAndCapped(t *testing.T) {
	clock := &testClock{now: time.Unix(300, 0)}
	provider := &testProvider{code: "never"}
	coordinator := newCoordinatorForTest(t, clock, provider)
	operator := actor()
	challenge, err := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	var rejected atomic.Int32
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, failure := coordinator.SubmitPhoneCode(context.Background(), operator, challenge.RequestID, "wrong")
			if errors.Is(failure, ErrAttemptsExceeded) || errors.Is(failure, ErrChallengeUnavailable) {
				rejected.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := provider.verify.Load(); got != MaxCodeAttempts {
		t.Fatalf("provider received %d attempts, want %d", got, MaxCodeAttempts)
	}
	if rejected.Load() == 0 {
		t.Fatal("concurrent submissions did not observe atomic attempt cap")
	}
	if _, failure := coordinator.SubmitPhoneCode(context.Background(), operator, challenge.RequestID, "wrong"); !errors.Is(failure, ErrAttemptsExceeded) {
		t.Fatalf("post-cap submission error = %v, want attempts exceeded", failure)
	}
	status, err := coordinator.Query(context.Background(), operator)
	if err != nil || status.Phone != nil {
		t.Fatalf("exhausted challenge was not invalidated: %+v, %v", status.Phone, err)
	}
}

func TestCoordinatorCancelDiscardsLateProviderResult(t *testing.T) {
	clock := &testClock{now: time.Unix(400, 0)}
	entered := make(chan struct{})
	provider := &testProvider{code: "ok", startEntered: entered, startRelease: make(chan struct{})}
	coordinator := newCoordinatorForTest(t, clock, provider)
	operator := actor()
	result := make(chan error, 1)
	go func() {
		_, err := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567")
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider did not block")
	}
	status, err := coordinator.Query(context.Background(), operator)
	if err != nil || status.Phone == nil {
		t.Fatalf("starting challenge not visible to coordinator: %+v, %v", status.Phone, err)
	}
	if err := coordinator.Cancel(context.Background(), operator, status.Phone.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Cancel(context.Background(), operator, status.Phone.RequestID); err != nil {
		t.Fatalf("owning cancellation was not idempotent: %v", err)
	}
	close(provider.startRelease)
	if err := <-result; !errors.Is(err, ErrChallengeUnavailable) {
		t.Fatalf("late provider result error = %v", err)
	}
	if current, _ := coordinator.Query(context.Background(), operator); current.Phone != nil {
		t.Fatalf("late provider result resurrected challenge: %+v", current.Phone)
	}
	waitForCondition(t, func() bool { return provider.cancel.Load() > 0 })
}

func TestCoordinatorStartCancellationRemovesStartingSlotBeforeLateFinish(t *testing.T) {
	clock := &testClock{now: time.Unix(450, 0)}
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &testProvider{code: "ok", startEntered: entered, startRelease: release}
	coordinator := newCoordinatorForTest(t, clock, provider)
	operator := actor()
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstResult := make(chan error, 1)
	go func() {
		_, failure := coordinator.StartPhoneChallenge(requestContext, operator, "+15551234567")
		firstResult <- failure
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider did not block")
	}

	cancel()
	deadline := time.After(time.Second)
	for {
		status, failure := coordinator.Query(context.Background(), operator)
		if failure != nil {
			t.Fatal(failure)
		}
		if status.Phone == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("request cancellation did not remove the starting record")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// The old provider call is still blocked, but its actor/kind slot has
	// already been released. A replacement can therefore be started before the
	// late result is delivered.
	close(release)
	replacement, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15557654321")
	if failure != nil {
		t.Fatalf("replacement start failed: %v", failure)
	}
	if failure := <-firstResult; !errors.Is(failure, ErrChallengeUnavailable) && !errors.Is(failure, context.Canceled) {
		t.Fatalf("late start result error = %v", failure)
	}
	status, failure := coordinator.Query(context.Background(), operator)
	if failure != nil || status.Phone == nil || status.Phone.RequestID != replacement.RequestID {
		t.Fatalf("late result disturbed replacement: %+v, %v", status.Phone, failure)
	}
}

func TestCoordinatorStartingWatcherDoesNotRemoveCommittedActiveChallenge(t *testing.T) {
	clock := &testClock{now: time.Unix(475, 0)}
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &testProvider{code: "ok", startEntered: entered, startRelease: release}
	coordinator := newCoordinatorForTest(t, clock, provider)
	operator := actor()
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan PhoneProjection, 1)
	failureResult := make(chan error, 1)
	go func() {
		projection, failure := coordinator.StartPhoneChallenge(requestContext, operator, "+15551234567")
		result <- projection
		failureResult <- failure
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider did not block")
	}
	close(release)
	projection := <-result
	if failure := <-failureResult; failure != nil {
		t.Fatalf("start failed: %v", failure)
	}

	// Exercise the cancellation watcher after the provider finish has
	// committed StateActive. A watcher from that start generation must not
	// remove the committed record, even when its cancellation branch wins its
	// select.
	coordinator.mu.Lock()
	record := coordinator.challenges[projection.RequestID]
	if record == nil || record.state != StateActive {
		coordinator.mu.Unlock()
		t.Fatalf("start did not commit active state: %+v", record)
	}
	generation := record.generation
	coordinator.mu.Unlock()

	cancel()
	canceled := make(chan struct{})
	go func() {
		coordinator.watchStarting(requestContext, requestContext, projection.RequestID, operator.OperatorID, generation, make(chan struct{}))
		close(canceled)
	}()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("cancellation watcher did not finish")
	}
	status, failure := coordinator.Query(context.Background(), operator)
	if failure != nil || status.Phone == nil || status.Phone.RequestID != projection.RequestID || status.Phone.State != StateActive {
		t.Fatalf("committed active challenge was removed by watcher: %+v, %v", status.Phone, failure)
	}
}

func TestCoordinatorTombstoneTTLUsesChallengeKind(t *testing.T) {
	clock := &testClock{now: time.Unix(490, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider, WithPhoneTTL(time.Minute), WithQRTTL(time.Hour))
	operator := actor()
	phone, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567")
	if failure != nil {
		t.Fatal(failure)
	}
	qr, failure := coordinator.StartQRChallenge(context.Background(), operator)
	if failure != nil {
		t.Fatal(failure)
	}
	if failure := coordinator.CancelPhone(context.Background(), operator, phone.RequestID); failure != nil {
		t.Fatal(failure)
	}
	if failure := coordinator.CancelQR(context.Background(), operator, qr.RequestID); failure != nil {
		t.Fatal(failure)
	}

	clock.Advance(2 * time.Minute)
	if failure := coordinator.CancelPhone(context.Background(), operator, phone.RequestID); !errors.Is(failure, ErrChallengeUnavailable) {
		t.Fatalf("phone tombstone error after phone TTL = %v", failure)
	}
	if failure := coordinator.CancelQR(context.Background(), operator, qr.RequestID); failure != nil {
		t.Fatalf("QR tombstone did not honor QR TTL: %v", failure)
	}
}

type blockingCleanupProvider struct {
	cleanupEntered chan struct{}
	cancel         atomic.Int32
}

func (provider *blockingCleanupProvider) StartPhone(context.Context, PhoneStart) (PhoneStarted, error) {
	return PhoneStarted{Handle: NewProviderHandle("cleanup-phone")}, nil
}

func (provider *blockingCleanupProvider) VerifyPhone(context.Context, ProviderHandle, string) (PhoneVerified, error) {
	return PhoneVerified{}, nil
}

func (provider *blockingCleanupProvider) CancelPhone(ctx context.Context, _ ProviderHandle) error {
	provider.cancel.Add(1)
	closeOnce(provider.cleanupEntered)
	<-ctx.Done()
	return ctx.Err()
}

func (provider *blockingCleanupProvider) StartQR(context.Context, QRStart) (QRStarted, error) {
	return QRStarted{Handle: NewProviderHandle("cleanup-qr"), URL: "tg://cleanup"}, nil
}

func (provider *blockingCleanupProvider) RefreshQR(context.Context, ProviderHandle) (QRStarted, error) {
	return QRStarted{Handle: NewProviderHandle("cleanup-qr"), URL: "tg://cleanup-refresh"}, nil
}

func (provider *blockingCleanupProvider) CancelQR(ctx context.Context, _ ProviderHandle) error {
	provider.cancel.Add(1)
	closeOnce(provider.cleanupEntered)
	<-ctx.Done()
	return ctx.Err()
}

func TestCoordinatorProviderCleanupIsBoundedAndDoesNotHoldMutex(t *testing.T) {
	clock := &testClock{now: time.Unix(550, 0)}
	provider := &blockingCleanupProvider{cleanupEntered: make(chan struct{})}
	coordinator := newCoordinatorForTest(t, clock, provider, WithCleanupTimeout(25*time.Millisecond))
	operator := actor()
	challenge, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567")
	if failure != nil {
		t.Fatal(failure)
	}

	cancelResult := make(chan error, 1)
	go func() { cancelResult <- coordinator.Cancel(context.Background(), operator, challenge.RequestID) }()
	select {
	case <-provider.cleanupEntered:
	case <-time.After(time.Second):
		t.Fatal("provider cleanup did not start")
	}
	queryResult := make(chan error, 1)
	go func() {
		_, queryFailure := coordinator.Query(context.Background(), operator)
		queryResult <- queryFailure
	}()
	select {
	case failure := <-queryResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cleanup held coordinator mutex")
	}
	select {
	case failure := <-cancelResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("provider cleanup exceeded configured bound")
	}
	if provider.cancel.Load() != 1 {
		t.Fatalf("provider cleanup calls = %d, want 1", provider.cancel.Load())
	}
	if _, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15557654321"); failure != nil {
		t.Fatalf("cleanup stranded actor slot: %v", failure)
	}
}

type uncooperativeCleanupProvider struct {
	cleanupEntered chan struct{}
	release        chan struct{}
	active         atomic.Int32
	maxActive      atomic.Int32
}

func (provider *uncooperativeCleanupProvider) StartPhone(context.Context, PhoneStart) (PhoneStarted, error) {
	return PhoneStarted{Handle: NewProviderHandle("uncooperative-phone")}, nil
}

func (provider *uncooperativeCleanupProvider) VerifyPhone(context.Context, ProviderHandle, string) (PhoneVerified, error) {
	return PhoneVerified{}, nil
}

func (provider *uncooperativeCleanupProvider) CancelPhone(context.Context, ProviderHandle) error {
	active := provider.active.Add(1)
	for {
		previous := provider.maxActive.Load()
		if active <= previous || provider.maxActive.CompareAndSwap(previous, active) {
			break
		}
	}
	closeOnce(provider.cleanupEntered)
	<-provider.release
	provider.active.Add(-1)
	return nil
}

func (provider *uncooperativeCleanupProvider) StartQR(context.Context, QRStart) (QRStarted, error) {
	return QRStarted{Handle: NewProviderHandle("uncooperative-qr"), URL: "tg://uncooperative"}, nil
}

func (provider *uncooperativeCleanupProvider) RefreshQR(context.Context, ProviderHandle) (QRStarted, error) {
	return provider.StartQR(context.Background(), QRStart{})
}

func (provider *uncooperativeCleanupProvider) CancelQR(ctx context.Context, handle ProviderHandle) error {
	return provider.CancelPhone(ctx, handle)
}

func TestCoordinatorUncooperativeCleanupDoesNotBlockCancelExpiryOrSuccess(t *testing.T) {
	clock := &testClock{now: time.Unix(700, 0)}
	provider := &uncooperativeCleanupProvider{cleanupEntered: make(chan struct{}), release: make(chan struct{})}
	coordinator := newCoordinatorForTest(t, clock, provider, WithCleanupWorkers(1), WithCleanupQueueSize(1), WithCleanupTimeout(time.Millisecond))

	firstActor := actor()
	first, failure := coordinator.StartPhoneChallenge(context.Background(), firstActor, "+15551234567")
	if failure != nil {
		t.Fatal(failure)
	}
	cancelResult := make(chan error, 1)
	go func() { cancelResult <- coordinator.Cancel(context.Background(), firstActor, first.RequestID) }()
	select {
	case failure := <-cancelResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancel waited for an uncooperative cleanup provider")
	}
	select {
	case <-provider.cleanupEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not start")
	}

	secondActor := actor()
	_, failure = coordinator.StartPhoneChallenge(context.Background(), secondActor, "+15557654321")
	if failure != nil {
		t.Fatal(failure)
	}
	clock.Advance(time.Minute)
	queryResult := make(chan error, 1)
	go func() {
		_, queryFailure := coordinator.Query(context.Background(), secondActor)
		queryResult <- queryFailure
	}()
	select {
	case failure := <-queryResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expiry cleanup waited for an uncooperative provider")
	}

	thirdActor := actor()
	third, failure := coordinator.StartPhoneChallenge(context.Background(), thirdActor, "+15559876543")
	if failure != nil {
		t.Fatal(failure)
	}
	successResult := make(chan error, 1)
	go func() {
		result, submitFailure := coordinator.SubmitPhoneCode(context.Background(), thirdActor, third.RequestID, "ok")
		if submitFailure == nil && !result.Completed {
			submitFailure = errors.New("successful verification did not complete")
		}
		successResult <- submitFailure
	}()
	select {
	case failure := <-successResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("successful verification waited for an uncooperative provider")
	}

	close(provider.release)
	shutdownCoordinatorForTest(t, coordinator)
}

func TestCleanupExecutorHasFixedWorkersAndBoundedQueue(t *testing.T) {
	provider := &uncooperativeCleanupProvider{cleanupEntered: make(chan struct{}), release: make(chan struct{})}
	executor := newCleanupExecutor(MaxCleanupWorkers+1, MaxCleanupQueueSize+1, time.Millisecond, provider, provider)
	if executor.workers != MaxCleanupWorkers {
		t.Fatalf("worker count = %d, want clamp %d", executor.workers, MaxCleanupWorkers)
	}
	if cap(executor.queue) != MaxCleanupQueueSize {
		t.Fatalf("queue capacity = %d, want clamp %d", cap(executor.queue), MaxCleanupQueueSize)
	}

	executor.stopWorkers()
	if executor.enqueue(cleanupTask{kind: KindPhone, handle: NewProviderHandle("after-stop")}) {
		t.Fatal("cleanup executor accepted work after stop")
	}
	if failure := executor.waitForStop(context.Background()); failure != nil {
		t.Fatal(failure)
	}

	bounded := newCleanupExecutor(2, 3, time.Millisecond, provider, provider)
	accepted := 0
	for index := 0; index < 100; index++ {
		if bounded.enqueue(cleanupTask{kind: KindPhone, handle: NewProviderHandle(fmt.Sprintf("handle-%d", index))}) {
			accepted++
		}
	}
	if accepted > bounded.workers+cap(bounded.queue) {
		t.Fatalf("accepted %d tasks with %d workers and queue %d", accepted, bounded.workers, cap(bounded.queue))
	}
	if len(bounded.queue) > cap(bounded.queue) || bounded.dropped.Load() == 0 {
		t.Fatalf("queue bounds/drop policy violated: len=%d cap=%d dropped=%d", len(bounded.queue), cap(bounded.queue), bounded.dropped.Load())
	}
	close(provider.release)
	bounded.stopWorkers()
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if failure := bounded.waitForStop(shutdownContext); failure != nil {
		t.Fatal(failure)
	}
	if provider.maxActive.Load() > int32(bounded.workers) {
		t.Fatalf("active cleanup calls = %d, want at most %d", provider.maxActive.Load(), bounded.workers)
	}
}

type deadlineCleanupProvider struct {
	entered  chan struct{}
	deadline chan time.Time
}

func (provider *deadlineCleanupProvider) StartPhone(context.Context, PhoneStart) (PhoneStarted, error) {
	return PhoneStarted{Handle: NewProviderHandle("deadline-phone")}, nil
}

func (provider *deadlineCleanupProvider) VerifyPhone(context.Context, ProviderHandle, string) (PhoneVerified, error) {
	return PhoneVerified{}, nil
}

func (provider *deadlineCleanupProvider) StartQR(context.Context, QRStart) (QRStarted, error) {
	return QRStarted{Handle: NewProviderHandle("deadline-qr"), URL: "tg://deadline"}, nil
}

func (provider *deadlineCleanupProvider) RefreshQR(context.Context, ProviderHandle) (QRStarted, error) {
	return provider.StartQR(context.Background(), QRStart{})
}

func (provider *deadlineCleanupProvider) CancelQR(ctx context.Context, handle ProviderHandle) error {
	return provider.CancelPhone(ctx, handle)
}

func (provider *deadlineCleanupProvider) CancelPhone(ctx context.Context, _ ProviderHandle) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("cleanup context had no deadline")
	}
	provider.deadline <- deadline
	closeOnce(provider.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestCleanupWorkerGivesCooperativeProviderDeadline(t *testing.T) {
	provider := &deadlineCleanupProvider{entered: make(chan struct{}), deadline: make(chan time.Time, 1)}
	executor := newCleanupExecutor(1, 1, 25*time.Millisecond, provider, nil)
	if !executor.enqueue(cleanupTask{kind: KindPhone, handle: NewProviderHandle("deadline")}) {
		t.Fatal("failed to enqueue cleanup")
	}
	select {
	case deadline := <-provider.deadline:
		if deadline.Before(time.Now()) || deadline.After(time.Now().Add(time.Second)) {
			t.Fatalf("unexpected cleanup deadline %v", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not call provider")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	executor.stopWorkers()
	if failure := executor.waitForStop(shutdownContext); failure != nil {
		t.Fatal(failure)
	}
}

func TestCleanupExecutorStopRacesWithEnqueue(t *testing.T) {
	provider := &testProvider{}
	executor := newCleanupExecutor(2, 4, time.Second, provider, provider)
	var wait sync.WaitGroup
	for index := 0; index < 100; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			executor.enqueue(cleanupTask{kind: KindPhone, handle: NewProviderHandle(fmt.Sprintf("race-%d", index))})
		}(index)
	}
	executor.stopWorkers()
	wait.Wait()
	if executor.enqueue(cleanupTask{kind: KindPhone, handle: NewProviderHandle("post-race")}) {
		t.Fatal("cleanup executor accepted work after racing stop")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if failure := executor.waitForStop(shutdownContext); failure != nil {
		t.Fatal(failure)
	}
}

func TestExpiredBatchEnqueuesWithoutSerialProviderCalls(t *testing.T) {
	clock := &testClock{now: time.Unix(800, 0)}
	provider := &uncooperativeCleanupProvider{cleanupEntered: make(chan struct{}), release: make(chan struct{})}
	coordinator := newCoordinatorForTest(t, clock, provider, WithCapacity(256), WithCleanupWorkers(1), WithCleanupQueueSize(1))
	actors := make([]applicationroot.Actor, 0, 200)
	for index := 0; index < 200; index++ {
		operator := actor()
		actors = append(actors, operator)
		if _, failure := coordinator.StartPhoneChallenge(context.Background(), operator, fmt.Sprintf("+1555%08d", index)); failure != nil {
			t.Fatalf("start %d failed: %v", index, failure)
		}
	}
	clock.Advance(11 * time.Minute)
	queryResult := make(chan error, 1)
	go func() {
		_, queryFailure := coordinator.Query(context.Background(), actors[0])
		queryResult <- queryFailure
	}()
	select {
	case failure := <-queryResult:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("large expired batch was serialized through provider cleanup")
	}
	close(provider.release)
	shutdownCoordinatorForTest(t, coordinator)
}

type equalHandleQRProvider struct {
	handle ProviderHandle
	cancel atomic.Int32
}

func (provider *equalHandleQRProvider) StartQR(context.Context, QRStart) (QRStarted, error) {
	return QRStarted{Handle: provider.handle, URL: "tg://same-start"}, nil
}

func (provider *equalHandleQRProvider) RefreshQR(context.Context, ProviderHandle) (QRStarted, error) {
	return QRStarted{Handle: provider.handle, URL: "tg://same-refresh"}, nil
}

func (provider *equalHandleQRProvider) CancelQR(context.Context, ProviderHandle) error {
	provider.cancel.Add(1)
	return nil
}

func TestCoordinatorQRRefreshRetainsEqualProviderHandle(t *testing.T) {
	clock := &testClock{now: time.Unix(600, 0)}
	provider := &equalHandleQRProvider{handle: NewProviderHandle("same-opaque-handle")}
	coordinator := New(Config{
		Clock:        clock.Now,
		QRProvider:   provider,
		NewRequestID: uuid.New,
	})
	shutdownCoordinatorForTest(t, coordinator)
	operator := actor()
	challenge, failure := coordinator.StartQRChallenge(context.Background(), operator)
	if failure != nil {
		t.Fatal(failure)
	}
	refreshed, failure := coordinator.RefreshQRChallenge(context.Background(), operator, challenge.RequestID)
	if failure != nil {
		t.Fatal(failure)
	}
	if refreshed.URL != "tg://same-refresh" {
		t.Fatalf("refresh URL = %q", refreshed.URL)
	}
	if provider.cancel.Load() != 0 {
		t.Fatalf("equal retained handle was canceled during refresh: %d", provider.cancel.Load())
	}
	if failure := coordinator.CancelQR(context.Background(), operator, challenge.RequestID); failure != nil {
		t.Fatal(failure)
	}
	waitForCondition(t, func() bool { return provider.cancel.Load() == 1 })
}

func TestCoordinatorForeignAndRandomIDsAreIndistinguishable(t *testing.T) {
	clock := &testClock{now: time.Unix(500, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider)
	owner := actor()
	foreign := actor()
	challenge, err := coordinator.StartPhoneChallenge(context.Background(), owner, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		actor applicationroot.Actor
		id    uuid.UUID
	}{
		{"foreign", foreign, challenge.RequestID},
		{"random", owner, uuid.New()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, failure := coordinator.SubmitPhoneCode(context.Background(), test.actor, test.id, "ok"); failure != ErrChallengeUnavailable {
				t.Fatalf("submit error = %v, want exact unavailable", failure)
			}
		})
	}
	if _, err := coordinator.StartQRChallenge(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	qr, err := coordinator.Query(context.Background(), owner)
	if err != nil || qr.QR == nil {
		t.Fatalf("QR projection = %+v, %v", qr.QR, err)
	}
	if _, err := coordinator.RefreshQRChallenge(context.Background(), owner, qr.QR.RequestID); err != nil {
		t.Fatal(err)
	}
	projectionText := fmt.Sprintf("%+v", qr.QR)
	if strings.Contains(projectionText, "qr-secret") || strings.Contains(projectionText, "phone-secret") {
		t.Fatalf("provider handle leaked in projection: %s", projectionText)
	}
}

func TestCoordinatorCapacityIsBounded(t *testing.T) {
	clock := &testClock{now: time.Unix(600, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider, WithCapacity(2))
	if _, err := coordinator.StartPhoneChallenge(context.Background(), actor(), "+15551234567"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.StartPhoneChallenge(context.Background(), actor(), "+15557654321"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.StartQRChallenge(context.Background(), actor()); !errors.Is(err, ErrChallengeCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
}

type returnedContextProvider struct {
	contexts chan context.Context
}

func (provider *returnedContextProvider) StartPhone(ctx context.Context, _ PhoneStart) (PhoneStarted, error) {
	provider.contexts <- ctx
	return PhoneStarted{Handle: NewProviderHandle("returned-phone"), Delivery: "test"}, nil
}

func (provider *returnedContextProvider) VerifyPhone(context.Context, ProviderHandle, string) (PhoneVerified, error) {
	return PhoneVerified{}, nil
}

func (provider *returnedContextProvider) CancelPhone(context.Context, ProviderHandle) error {
	return nil
}

func (provider *returnedContextProvider) StartQR(ctx context.Context, _ QRStart) (QRStarted, error) {
	provider.contexts <- ctx
	return QRStarted{Handle: NewProviderHandle("returned-qr"), URL: "tg://returned"}, nil
}

func (provider *returnedContextProvider) RefreshQR(context.Context, ProviderHandle) (QRStarted, error) {
	return QRStarted{Handle: NewProviderHandle("returned-refresh"), URL: "tg://returned-refresh"}, nil
}

func (provider *returnedContextProvider) CancelQR(context.Context, ProviderHandle) error {
	return nil
}

func TestCoordinatorSuccessfulStartsReleaseChildContexts(t *testing.T) {
	provider := &returnedContextProvider{contexts: make(chan context.Context, 2)}
	coordinator := NewWithProviders(provider, provider)
	shutdownCoordinatorForTest(t, coordinator)
	operator := actor()
	if _, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567"); failure != nil {
		t.Fatal(failure)
	}
	phoneContext := <-provider.contexts
	select {
	case <-phoneContext.Done():
	case <-time.After(time.Second):
		t.Fatal("successful phone start retained its child context")
	}
	if _, failure := coordinator.StartQRChallenge(context.Background(), operator); failure != nil {
		t.Fatal(failure)
	}
	qrContext := <-provider.contexts
	select {
	case <-qrContext.Done():
	case <-time.After(time.Second):
		t.Fatal("successful QR start retained its child context")
	}
}

func TestCoordinatorCloseInvalidatesBlockedStartAndRejectsOperations(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &testProvider{code: "ok", startEntered: entered, startRelease: release}
	coordinator := NewWithProviders(provider, provider)
	shutdownCoordinatorForTest(t, coordinator)
	operator := actor()
	startResult := make(chan error, 1)
	go func() {
		_, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567")
		startResult <- failure
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider did not block")
	}

	coordinator.Close()
	close(release)
	if failure := <-startResult; !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("blocked start after close = %v", failure)
	}
	if _, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15557654321"); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("start after close = %v", failure)
	}
	if _, failure := coordinator.StartQRChallenge(context.Background(), operator); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("QR start after close = %v", failure)
	}
	if _, failure := coordinator.SubmitPhoneCode(context.Background(), operator, uuid.New(), "ok"); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("submit after close = %v", failure)
	}
	if _, failure := coordinator.RefreshQRChallenge(context.Background(), operator, uuid.New()); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("refresh after close = %v", failure)
	}
	if _, failure := coordinator.Get(context.Background(), operator); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("get after close = %v", failure)
	}
	if failure := coordinator.Cancel(context.Background(), operator, uuid.New()); !errors.Is(failure, ErrCoordinatorClosed) {
		t.Fatalf("cancel after close = %v", failure)
	}
	if len(coordinator.challenges) != 0 || len(coordinator.phoneSlots) != 0 || len(coordinator.qrSlots) != 0 {
		t.Fatalf("closed coordinator retained active state: challenges=%d phone=%d qr=%d", len(coordinator.challenges), len(coordinator.phoneSlots), len(coordinator.qrSlots))
	}
}

func TestCoordinatorCloseCleansKnownHandlesBeforeExecutorStop(t *testing.T) {
	clock := &testClock{now: time.Unix(900, 0)}
	provider := &testProvider{code: "ok"}
	coordinator := newCoordinatorForTest(t, clock, provider, WithCleanupWorkers(1), WithCleanupQueueSize(2))
	operator := actor()
	if _, failure := coordinator.StartPhoneChallenge(context.Background(), operator, "+15551234567"); failure != nil {
		t.Fatal(failure)
	}
	if _, failure := coordinator.StartQRChallenge(context.Background(), operator); failure != nil {
		t.Fatal(failure)
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if failure := coordinator.Shutdown(shutdownContext); failure != nil {
		t.Fatal(failure)
	}
	if provider.cancel.Load() != 2 {
		t.Fatalf("known provider handles canceled %d times, want 2", provider.cancel.Load())
	}
}
