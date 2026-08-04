package accountowner

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/gotd/td/tgerr"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func TestRegistryRevokeAndStopFencesDrainsAndTearsDown(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	prior := registryTarget()
	prior.Status = operatoraccount.StatusActive
	ordinaryContext, cancelOrdinary := context.WithTimeout(context.Background(), time.Second)
	defer cancelOrdinary()
	ordinaryEntered := make(chan struct{})
	ordinaryCanceled := make(chan struct{})
	ordinaryDrained := make(chan struct{})
	releaseOrdinary := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOrdinary) }) }
	t.Cleanup(release)
	var ordinaryCalls atomic.Int32
	ordinaryDone := make(chan error, 1)
	go func() {
		ordinaryDone <- registry.Execute(ordinaryContext, prior, func(ctx context.Context, _ *gotdtelegram.Client) error {
			ordinaryCalls.Add(1)
			close(ordinaryEntered)
			select {
			case <-ctx.Done():
				close(ordinaryCanceled)
				<-releaseOrdinary
			case <-releaseOrdinary:
			}
			close(ordinaryDrained)
			return nil
		})
	}()
	select {
	case <-ordinaryEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ordinary callback")
	}

	queuedContext, cancelQueued := context.WithTimeout(context.Background(), time.Second)
	defer cancelQueued()
	var queuedCalls atomic.Int32
	queuedDone := make(chan error, 1)
	queuedChecked := make(chan error, 1)
	// Open is the public admission barrier. The first callback is still holding
	// the account gate, so this admitted handle's Execute cannot reach its body.
	queuedHandle, openFailure := registry.Open(queuedContext, prior)
	if openFailure != nil {
		t.Fatalf("Open() second ordinary admission error = %v", openFailure)
	}
	t.Cleanup(func() { _ = queuedHandle.Close() })
	queuedGateWaitContext := &executeGateWaitContext{
		Context:  queuedContext,
		observed: make(chan struct{}),
	}
	go func() {
		queuedDone <- queuedHandle.Execute(queuedGateWaitContext, func(context.Context, *gotdtelegram.Client) error {
			queuedCalls.Add(1)
			return nil
		})
	}()
	select {
	case <-queuedGateWaitContext.observed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second ordinary Execute gate admission")
	}

	logoutCalls := atomic.Int32{}
	privilegedEntered := make(chan struct{}, 1)
	disconnecting := prior
	disconnecting.Status = operatoraccount.StatusDisconnecting
	disconnecting.Version++
	owner := factory.owner(0)
	revokeContext, cancelRevoke := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRevoke()
	revokeDone := make(chan error, 1)
	go func() {
		revokeDone <- registry.RevokeAndStop(revokeContext, disconnecting, func(callbackContext context.Context, _ *gotdtelegram.Client) error {
			select {
			case <-owner.stopped:
				t.Error("owner stopped before privileged callback")
			default:
			}
			select {
			case <-ordinaryDrained:
			default:
				t.Error("privileged callback began before ordinary callback drained")
			}
			select {
			case failure := <-queuedDone:
				if !errors.Is(failure, ErrAccountStopped) {
					t.Errorf("queued ordinary Execute() error = %v, want %v", failure, ErrAccountStopped)
				}
				queuedChecked <- failure
			case <-callbackContext.Done():
				return callbackContext.Err()
			}
			logoutCalls.Add(1)
			privilegedEntered <- struct{}{}
			return nil
		})
	}()
	select {
	case <-ordinaryCanceled:
		release()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for revoke to cancel ordinary callback")
	}
	select {
	case <-ordinaryDrained:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ordinary callback to drain")
	}
	select {
	case <-privilegedEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for privileged callback")
	}
	if failure := <-revokeDone; failure != nil {
		t.Fatalf("RevokeAndStop() error = %v", failure)
	}
	if got := logoutCalls.Load(); got != 1 {
		t.Fatalf("privileged callback calls = %d, want 1", got)
	}
	if got := ordinaryCalls.Load(); got != 1 {
		t.Fatalf("ordinary callback calls = %d, want 1", got)
	}
	if got := queuedCalls.Load(); got != 0 {
		t.Fatalf("queued ordinary callback calls = %d, want 0", got)
	}
	select {
	case <-owner.stopped:
	default:
		t.Fatal("owner was not stopped after privileged callback")
	}
	select {
	case failure := <-queuedChecked:
		if !errors.Is(failure, ErrAccountStopped) {
			t.Fatalf("queued ordinary Execute() error = %v, want ErrAccountStopped", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued ordinary Execute")
	}
	select {
	case failure := <-ordinaryDone:
		if !errors.Is(failure, ErrAccountStopped) {
			t.Fatalf("ordinary Execute() result = %v, want ErrAccountStopped", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ordinary Execute")
	}
}

type executeGateWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (context *executeGateWaitContext) Done() <-chan struct{} {
	context.once.Do(func() { close(context.observed) })
	return context.Context.Done()
}

func TestRegistryRevokeAndStopSerializesSameIntentAndBuildsPrivateOwners(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting
	target.Version++

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var calls atomic.Int32
	var firstOnce sync.Once
	callback := func(_ context.Context, _ *gotdtelegram.Client) error {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		calls.Add(1)
		if current == 1 {
			firstOnce.Do(func() { close(firstEntered) })
			<-releaseFirst
		}
		active.Add(-1)
		return nil
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- registry.RevokeAndStop(context.Background(), target, callback) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first privileged callback")
	}

	var wait sync.WaitGroup
	secondDone := make(chan error, 1)
	wait.Go(func() {
		secondDone <- registry.RevokeAndStop(context.Background(), target, callback)
	})
	close(releaseFirst)
	if failure := <-firstDone; failure != nil {
		t.Fatalf("first RevokeAndStop() error = %v", failure)
	}
	wait.Wait()
	if failure := <-secondDone; failure != nil {
		t.Fatalf("second RevokeAndStop() error = %v", failure)
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent privileged callbacks = %d, want 1", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("privileged callback calls = %d, want 2", got)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("private owner constructions = %d, want 2", got)
	}
}

func TestRegistryRevokeAndStopRepanicsAfterTeardown(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting
	target.Version++
	want := "privileged callback panic"

	func() {
		defer func() {
			if got := recover(); got != want {
				t.Fatalf("panic = %v, want %q", got, want)
			}
		}()
		_ = registry.RevokeAndStop(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
			panic(want)
		})
	}()
	select {
	case <-factory.owner(0).stopped:
	default:
		t.Fatal("owner was not stopped after callback panic")
	}
}

func TestRegistryStopAccountAcceptsReauthenticationTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusReauthRequired

	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("StopAccount() error = %v, want nil for reauth_required target", failure)
	}
}

func TestRegistryRevokeAndStopAcceptsReauthenticationPriorOwner(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	prior := registryTarget()
	prior.Status = operatoraccount.StatusReauthRequired
	if failure := registry.Execute(context.Background(), prior, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("initial Execute() error = %v", failure)
	}

	disconnecting := prior
	disconnecting.Status = operatoraccount.StatusDisconnecting
	disconnecting.Version++
	if failure := registry.RevokeAndStop(context.Background(), disconnecting, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("RevokeAndStop() error = %v, want nil", failure)
	}
}

func TestRegistryRevokeAndStopRejectsNoOwnerInitialVersion(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting

	if failure := registry.RevokeAndStop(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		t.Fatal("privileged callback ran for an invalid structural target")
		return nil
	}); !errors.Is(failure, ErrInvalidAdmission) {
		t.Fatalf("RevokeAndStop() error = %v, want ErrInvalidAdmission", failure)
	}
	if got := factory.count(); got != 0 {
		t.Fatalf("private owners for invalid target = %d, want 0", got)
	}
}

func TestRegistryPrivateRevokeBuildFailureRestoresNewerAdmission(t *testing.T) {
	factory := &registryOwnerFactory{fail: errors.New("private owner build failed")}
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting
	target.Version++

	buildFailure := registry.RevokeAndStop(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		t.Fatal("privileged callback ran after private owner build failure")
		return nil
	})
	if !errors.Is(buildFailure, factory.fail) {
		t.Fatalf("RevokeAndStop() error = %v, want private build failure", buildFailure)
	}
	factory.mu.Lock()
	factory.fail = nil
	factory.mu.Unlock()

	newer := target
	newer.Status = operatoraccount.StatusActive
	newer.Version++
	if failure := registry.Execute(context.Background(), newer, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("newer Execute() error = %v after private build failure", failure)
	}
}

func TestRegistryPrivateRevokeBuildFailureIgnoresStaleHandleRefs(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	prior := registryTarget()
	prior.Status = operatoraccount.StatusActive
	staleHandle, failure := registry.Open(context.Background(), prior)
	if failure != nil {
		t.Fatalf("Open() error = %v", failure)
	}
	// Keep staleHandle open deliberately. Stopping its owner leaves the handle
	// reference behind while making the entry non-current.
	factory.owner(0).Stop()
	waitForNoCurrentEntry(t, registry, accountKeyFromTarget(prior))

	buildFailure := errors.New("private owner build failed with stale handle")
	factory.mu.Lock()
	factory.fail = buildFailure
	factory.mu.Unlock()
	disconnecting := prior
	disconnecting.Status = operatoraccount.StatusDisconnecting
	disconnecting.Version++
	if got := registry.RevokeAndStop(context.Background(), disconnecting, func(context.Context, *gotdtelegram.Client) error {
		t.Fatal("privileged callback ran after private owner build failure")
		return nil
	}); !errors.Is(got, buildFailure) {
		t.Fatalf("RevokeAndStop() error = %v, want private build failure", got)
	}

	factory.mu.Lock()
	factory.fail = nil
	factory.mu.Unlock()
	newer := disconnecting
	newer.Status = operatoraccount.StatusActive
	newer.Version++
	if failure := registry.Execute(context.Background(), newer, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("newer Execute() error = %v with stale handle ref", failure)
	}
	if failure := staleHandle.Execute(context.Background(), func(context.Context, *gotdtelegram.Client) error {
		t.Fatal("stale handle callback ran after newer lifecycle admission")
		return nil
	}); !errors.Is(failure, ErrStaleAdmission) {
		t.Fatalf("stale Handle.Execute() error = %v, want ErrStaleAdmission", failure)
	}
	_ = staleHandle
}

func TestRegistryPrivateRevokeTeardownFailureRestoresNewerAdmission(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting
	target.Version++
	releaseOwner := make(chan struct{})

	failure := registry.RevokeAndStop(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		owner := factory.owner(0)
		owner.holdTeardown.Store(true)
		owner.releaseTeardown = releaseOwner
		return nil
	})
	if !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("RevokeAndStop() error = %v, want teardown deadline", failure)
	}
	close(releaseOwner)

	newer := target
	newer.Status = operatoraccount.StatusActive
	newer.Version++
	if failure := registry.Execute(context.Background(), newer, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("newer Execute() error = %v after private teardown failure", failure)
	}
}

func TestRegistryRevokeWaitsForTeardownAndQueuedRetry(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	prior := registryTarget()
	prior.Status = operatoraccount.StatusActive
	if failure := registry.Execute(context.Background(), prior, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("initial Execute() error = %v", failure)
	}
	owner := factory.owner(0)
	owner.holdTeardown.Store(true)
	owner.releaseTeardown = make(chan struct{})

	stopDone := make(chan error, 1)
	go func() { stopDone <- registry.StopAccount(context.Background(), prior) }()
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for teardown to stop prior owner")
	}

	disconnecting := prior
	disconnecting.Status = operatoraccount.StatusDisconnecting
	disconnecting.Version++
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	var firstOnce sync.Once
	callback := func(context.Context, *gotdtelegram.Client) error {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		if calls.Add(1) == 1 {
			firstOnce.Do(func() { close(firstEntered) })
			<-releaseFirst
		}
		active.Add(-1)
		return nil
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- registry.RevokeAndStop(context.Background(), disconnecting, callback) }()
	close(owner.releaseTeardown)
	if failure := <-stopDone; failure != nil {
		t.Fatalf("StopAccount() error = %v", failure)
	}
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first privileged callback")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- registry.RevokeAndStop(context.Background(), disconnecting, callback) }()
	waitForRevokeWaiter(t, registry, accountKeyFromTarget(disconnecting))
	close(releaseFirst)
	if failure := <-firstDone; failure != nil {
		t.Fatalf("first RevokeAndStop() error = %v", failure)
	}
	if failure := <-secondDone; failure != nil {
		t.Fatalf("queued RevokeAndStop() error = %v", failure)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("privileged callback calls = %d, want 2", got)
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent privileged callbacks = %d, want 1", got)
	}
}

func waitForRevokeWaiter(t *testing.T, registry *Registry, key accountKey) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		registry.mu.Lock()
		slot := registry.slots[key]
		waiters := 0
		if slot != nil {
			waiters = slot.revokeWaiters
		}
		registry.mu.Unlock()
		if waiters > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for queued revoke")
		default:
			runtime.Gosched()
		}
	}
}

func waitForNoCurrentEntry(t *testing.T, registry *Registry, key accountKey) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		registry.mu.Lock()
		slot := registry.slots[key]
		current := (*runtimeEntry)(nil)
		if slot != nil {
			slot.mu.Lock()
			current = slot.current
			slot.mu.Unlock()
		}
		registry.mu.Unlock()
		if current == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for stale runtime entry")
		default:
			runtime.Gosched()
		}
	}
}

func TestRegistryRevokeTeardownFailurePrecedesCallbackFailure(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting
	target.Version++
	ownerReady := make(chan struct{})
	releaseOwner := make(chan struct{})

	failure := registry.RevokeAndStop(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		owner := factory.owner(0)
		owner.holdTeardown.Store(true)
		owner.releaseTeardown = releaseOwner
		close(ownerReady)
		return tgerr.New(401, "AUTH_KEY_UNREGISTERED")
	})
	select {
	case <-ownerReady:
	default:
		t.Fatal("privileged callback did not run")
	}
	if !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("RevokeAndStop() error = %v, want teardown deadline", failure)
	}
	close(releaseOwner)
}
