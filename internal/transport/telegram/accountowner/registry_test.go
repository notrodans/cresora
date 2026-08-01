package accountowner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	gotdtelegram "github.com/gotd/td/telegram"

	applicationroot "github.com/notrodans/cresora/internal/application"
	operatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

type registryOwner struct {
	ready           chan struct{}
	stop            chan struct{}
	done            chan struct{}
	stopped         chan struct{}
	releaseTeardown chan struct{}

	stopOnce     sync.Once
	doneOnce     sync.Once
	closed       atomic.Bool
	holdTeardown atomic.Bool
}

func newRegistryOwner() *registryOwner {
	ready := make(chan struct{})
	close(ready)
	return &registryOwner{
		ready:   ready,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (owner *registryOwner) Run(ctx context.Context) error {
	select {
	case <-owner.stop:
		if owner.holdTeardown.Load() {
			<-owner.releaseTeardown
		}
	case <-ctx.Done():
	}
	owner.doneOnce.Do(func() { close(owner.done) })
	return nil
}

func (owner *registryOwner) Stop() {
	owner.stopOnce.Do(func() {
		owner.closed.Store(true)
		close(owner.stopped)
		close(owner.stop)
	})
}

func (owner *registryOwner) WaitReady(ctx context.Context) error {
	select {
	case <-owner.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (owner *registryOwner) Wait(ctx context.Context) error {
	select {
	case <-owner.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (owner *registryOwner) Execute(ctx context.Context, callback ClientCallback) error {
	if owner.closed.Load() {
		return ErrAccountStopped
	}
	return callback(ctx, nil)
}

type registryOwnerFactory struct {
	mu     sync.Mutex
	owners []*registryOwner
	fail   error
}

type registryOwnerLifecycle struct {
	ready chan struct{}
}

func newRegistryOwnerLifecycle() *registryOwnerLifecycle {
	ready := make(chan struct{})
	close(ready)
	return &registryOwnerLifecycle{ready: ready}
}

func (lifecycle *registryOwnerLifecycle) Run(ctx context.Context, callback func(context.Context) error) error {
	return callback(ctx)
}

func (lifecycle *registryOwnerLifecycle) Ready() <-chan struct{} {
	return lifecycle.ready
}

func (lifecycle *registryOwnerLifecycle) rawClient() *gotdtelegram.Client {
	return nil
}

func (factory *registryOwnerFactory) build(
	gotdclient.Factory,
	transporttelegram.SessionScope,
	int,
	string,
) (ownerRuntime, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.fail != nil {
		return nil, factory.fail
	}
	owner := newRegistryOwner()
	factory.owners = append(factory.owners, owner)
	return owner, nil
}

func (factory *registryOwnerFactory) count() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return len(factory.owners)
}

func (factory *registryOwnerFactory) owner(index int) *registryOwner {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.owners[index]
}

func registryTarget() operatoraccountauth.AuthTarget {
	return operatoraccountauth.AuthTarget{
		Actor:     applicationroot.Actor{OperatorID: uuid.MustParse("11111111-1111-4111-8111-111111111111")},
		AccountID: operatoraccount.Identity(uuid.MustParse("22222222-2222-4222-8222-222222222222")),
		Status:    operatoraccount.StatusAuthenticating,
		Version:   operatoraccount.Version(1),
	}
}

func newFakeRegistry(t *testing.T, factory *registryOwnerFactory, config RegistryConfig) *Registry {
	t.Helper()
	if config.Capacity == 0 {
		config.Capacity = 4
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = time.Hour
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = time.Second
	}
	registry, err := newRegistry(config, factory.build)
	if err != nil {
		t.Fatalf("newRegistry() error = %v", err)
	}
	t.Cleanup(func() {
		if err := registry.Stop(context.Background()); err != nil {
			t.Errorf("registry.Stop() error = %v", err)
		}
	})
	return registry
}

func TestRegistryRejectsInvalidTargetStatus(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting

	failure := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		return nil
	})
	if !errors.Is(failure, ErrInvalidAdmission) {
		t.Fatalf("Execute() error = %v, want ErrInvalidAdmission", failure)
	}
	if got := factory.count(); got != 0 {
		t.Fatalf("owner constructions after invalid status = %d, want 0", got)
	}
}

func TestRegistryAllowsActiveOperationTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusActive

	if failure := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("Execute() error = %v, want nil for active target", failure)
	}
}

func TestRegistryStopAccountAcceptsDisconnectingRecoveryTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting

	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("StopAccount() error = %v, want nil for disconnecting recovery target", failure)
	}
}

func TestRegistryRejectsActiveStopTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusActive

	if failure := registry.StopAccount(context.Background(), target); !errors.Is(failure, ErrInvalidAdmission) {
		t.Fatalf("StopAccount() error = %v, want ErrInvalidAdmission", failure)
	}
}

func TestRegistryConcurrentOpenBuildsOneOwner(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	const callers = 20
	var wait sync.WaitGroup
	errors := make(chan error, callers)
	handles := make(chan *Handle, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, err := registry.Open(context.Background(), target)
			if err != nil {
				errors <- err
				return
			}
			handles <- handle
		}()
	}
	wait.Wait()
	close(errors)
	close(handles)
	for err := range errors {
		t.Fatalf("Open() error = %v", err)
	}
	for handle := range handles {
		if err := handle.Close(); err != nil {
			t.Fatalf("Handle.Close() error = %v", err)
		}
	}
	if got := factory.count(); got != 1 {
		t.Fatalf("owner constructions = %d, want 1", got)
	}
}

func TestRegistrySerializesOperationsPerAccount(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	var active atomic.Int32
	var maximum atomic.Int32
	var wait sync.WaitGroup
	var entered sync.Once
	firstEntered := make(chan struct{})
	release := make(chan struct{})
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := registry.Execute(context.Background(), target, func(ctx context.Context, _ *gotdtelegram.Client) error {
				current := active.Add(1)
				for {
					old := maximum.Load()
					if current <= old || maximum.CompareAndSwap(old, current) {
						break
					}
				}
				entered.Do(func() { close(firstEntered) })
				select {
				case <-release:
				case <-ctx.Done():
				}
				active.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("Execute() error = %v", err)
			}
		}()
	}
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first operation")
	}
	close(release)
	wait.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent operations = %d, want 1", got)
	}
}

func TestRegistryStopAccountRejectsLateOpenAndAllowsNewVersion(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	if err := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("initial Execute() error = %v", err)
	}
	if err := registry.StopAccount(context.Background(), target); err != nil {
		t.Fatalf("StopAccount() error = %v", err)
	}
	if err := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); !errors.Is(err, ErrAccountStopped) {
		t.Fatalf("late Execute() error = %v, want ErrAccountStopped", err)
	}

	next := target
	next.Version++
	if err := registry.Execute(context.Background(), next, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("new-version Execute() error = %v", err)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("owner constructions = %d, want 2", got)
	}
}

func TestRegistryStopAccountCancelsInFlightOperationAndRejectsLateResult(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	entered := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- registry.Execute(context.Background(), target, func(ctx context.Context, _ *gotdtelegram.Client) error {
			close(entered)
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for operation")
	}
	if err := registry.StopAccount(context.Background(), target); err != nil {
		t.Fatalf("StopAccount() error = %v", err)
	}
	if err := <-operationDone; !errors.Is(err, ErrAccountStopped) {
		t.Fatalf("late operation result = %v, want ErrAccountStopped", err)
	}
}

func TestRegistryStopAccountDrainsBeforeStoppingOwnerAndRetriesAfterTimeout(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	entered := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback")
	}
	owner := factory.owner(0)
	stopContext, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	stopFailure := registry.StopAccount(stopContext, target)
	cancel()
	if !errors.Is(stopFailure, context.DeadlineExceeded) {
		t.Fatalf("first StopAccount() error = %v, want deadline exceeded", stopFailure)
	}
	select {
	case <-owner.stopped:
		t.Fatal("owner stopped before callback drain")
	default:
	}

	close(release)
	if failure := <-operationDone; !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("drained operation result = %v, want ErrAccountStopped", failure)
	}
	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("retry StopAccount() error = %v", failure)
	}
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owner stop")
	}
}

func TestRegistryStopAccountDrainsRealOwnerCallbackBeforeStoppingOwner(t *testing.T) {
	var owner *Owner
	registry, err := newRegistry(RegistryConfig{
		Capacity:     1,
		IdleTimeout:  time.Hour,
		DrainTimeout: time.Millisecond,
	}, func(gotdclient.Factory, transporttelegram.SessionScope, int, string) (ownerRuntime, error) {
		owner = newOwner(newRegistryOwnerLifecycle())
		return owner, nil
	})
	if err != nil {
		t.Fatalf("newRegistry() error = %v", err)
	}
	t.Cleanup(func() {
		if err := registry.Stop(context.Background()); err != nil {
			t.Errorf("registry.Stop() error = %v", err)
		}
	})

	target := registryTarget()
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- registry.Execute(context.Background(), target, func(ctx context.Context, _ *gotdtelegram.Client) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for real owner callback")
	}
	stopContext, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	stopFailure := registry.StopAccount(stopContext, target)
	cancel()
	if !errors.Is(stopFailure, context.DeadlineExceeded) {
		t.Fatalf("first StopAccount() error = %v, want deadline exceeded", stopFailure)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback cancellation")
	}
	owner.mu.Lock()
	ownerStopped := owner.stopping
	owner.mu.Unlock()
	if ownerStopped {
		t.Fatal("real owner stopped before callback released")
	}

	close(release)
	if failure := <-operationDone; !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("real owner operation result = %v, want ErrAccountStopped", failure)
	}
	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("retry StopAccount() error = %v", failure)
	}
	owner.mu.Lock()
	ownerStopped = owner.stopping
	owner.mu.Unlock()
	if !ownerStopped {
		t.Fatal("real owner was not stopped after callback drain")
	}
}

func TestRegistryStopRetriesStoppingEntriesAfterDrainTimeout(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	entered := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback")
	}
	owner := factory.owner(0)
	stopContext, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	stopFailure := registry.Stop(stopContext)
	cancel()
	if !errors.Is(stopFailure, context.DeadlineExceeded) {
		t.Fatalf("first Stop() error = %v, want deadline exceeded", stopFailure)
	}
	select {
	case <-owner.stopped:
		t.Fatal("owner stopped before registry callback drain")
	default:
	}

	close(release)
	if failure := <-operationDone; !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("drained operation result = %v, want ErrAccountStopped", failure)
	}
	if failure := registry.Stop(context.Background()); failure != nil {
		t.Fatalf("retry Stop() error = %v", failure)
	}
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owner stop")
	}
	registry.mu.Lock()
	rootStopped := registry.rootStopped
	registry.mu.Unlock()
	if !rootStopped {
		t.Fatal("registry root was not canceled after complete stop")
	}
}

func TestRegistryReplacementWaitsForPriorOwnerTeardown(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	first := registryTarget()
	if err := registry.Execute(context.Background(), first, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	firstOwner := factory.owner(0)
	firstOwner.holdTeardown.Store(true)
	firstOwner.releaseTeardown = make(chan struct{})

	next := first
	next.Version++
	replacementDone := make(chan error, 1)
	go func() {
		replacementDone <- registry.Execute(context.Background(), next, func(context.Context, *gotdtelegram.Client) error {
			return nil
		})
	}()
	select {
	case <-firstOwner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for prior owner stop")
	}
	if got := factory.count(); got != 1 {
		t.Fatalf("owners while prior teardown is blocked = %d, want 1", got)
	}
	close(firstOwner.releaseTeardown)
	if failure := <-replacementDone; failure != nil {
		t.Fatalf("replacement Execute() error = %v", failure)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("owners after prior teardown = %d, want 2", got)
	}
}

func TestRegistryCapacityEvictsIdleOwner(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1, IdleTimeout: time.Hour})
	first := registryTarget()
	if err := registry.Execute(context.Background(), first, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	registry.mu.Lock()
	registry.slots[keyFor(first)].mu.Lock()
	registry.slots[keyFor(first)].lastUsed = time.Now().Add(-time.Hour)
	registry.slots[keyFor(first)].mu.Unlock()
	registry.mu.Unlock()
	registry.evictIdle()
	second := first
	second.AccountID = operatoraccount.Identity(uuid.MustParse("33333333-3333-4333-8333-333333333333"))
	if err := registry.Execute(context.Background(), second, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("owner constructions = %d, want idle eviction and replacement", got)
	}
}

func TestRegistryOwnerFailureIsolatedToOneAccount(t *testing.T) {
	failure := errors.New("one account failed")
	factory := &registryOwnerFactory{fail: failure}
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	bad := registryTarget()
	good := bad
	good.AccountID = operatoraccount.Identity(uuid.MustParse("33333333-3333-4333-8333-333333333333"))

	if err := registry.Execute(context.Background(), bad, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); !errors.Is(err, failure) {
		t.Fatalf("failed-account Execute() error = %v, want owner failure", err)
	}
	factory.mu.Lock()
	factory.fail = nil
	factory.mu.Unlock()
	if err := registry.Execute(context.Background(), good, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); err != nil {
		t.Fatalf("independent-account Execute() error = %v", err)
	}
}

func TestRegistryFailedBuildRemovesEmptySlot(t *testing.T) {
	failure := errors.New("owner construction failed")
	factory := &registryOwnerFactory{fail: failure}
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1})
	target := registryTarget()

	for attempt := 0; attempt < 20; attempt++ {
		if err := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error {
			return nil
		}); !errors.Is(err, failure) {
			t.Fatalf("failed Execute() error = %v, want construction failure", err)
		}
		registry.mu.Lock()
		slots := len(registry.slots)
		registry.mu.Unlock()
		if slots != 0 {
			t.Fatalf("registry slots after failed build = %d, want 0", slots)
		}
	}
}
