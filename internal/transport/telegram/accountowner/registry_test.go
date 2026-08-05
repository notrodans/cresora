package accountowner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	gotdtelegram "github.com/gotd/td/telegram"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
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

	stopOnce       sync.Once
	doneOnce       sync.Once
	closed         atomic.Bool
	holdTeardown   atomic.Bool
	executeEntered chan struct{}
	executeOnce    sync.Once
	allowExecute   <-chan struct{}
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
	if owner.executeEntered != nil {
		owner.executeOnce.Do(func() { close(owner.executeEntered) })
		select {
		case <-owner.allowExecute:
		case <-ctx.Done():
		}
	}
	if owner.closed.Load() {
		return ErrAccountStopped
	}
	return callback(ctx, nil)
}

type registryOwnerFactory struct {
	mu             sync.Mutex
	owners         []*registryOwner
	fail           error
	buildEntered   chan struct{}
	buildOnce      sync.Once
	allowBuild     <-chan struct{}
	executeEntered chan struct{}
	allowExecute   <-chan struct{}
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
	if factory.fail != nil {
		failure := factory.fail
		factory.mu.Unlock()
		return nil, failure
	}
	owner := newRegistryOwner()
	owner.executeEntered, owner.allowExecute = factory.executeEntered, factory.allowExecute
	factory.owners = append(factory.owners, owner)
	entered, allow := factory.buildEntered, factory.allowBuild
	factory.mu.Unlock()
	if entered != nil {
		factory.buildOnce.Do(func() { close(entered) })
	}
	if allow != nil {
		<-allow
	}
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

func registryTarget() operatoraccounts.RuntimeTarget {
	return operatoraccounts.RuntimeTarget{
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

func TestRegistry_RejectsInvalidTargetStatus(t *testing.T) {
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

func TestRegistry_AllowsActiveOperationTarget(t *testing.T) {
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

func TestRegistry_StopAccountAcceptsDisconnectingRecoveryTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusDisconnecting

	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("StopAccount() error = %v, want nil for disconnecting recovery target", failure)
	}
}

func TestRegistry_AllowsActiveStopTarget(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Status = operatoraccount.StatusActive

	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("StopAccount() error = %v, want nil", failure)
	}
	if failure := registry.Execute(context.Background(), target, func(context.Context, *gotdtelegram.Client) error { return nil }); !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("late active Execute() error = %v, want ErrAccountStopped", failure)
	}
}

func TestRegistry_ConcurrentOpenBuildsOneOwner(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	const callers = 20
	var wait sync.WaitGroup
	errors := make(chan error, callers)
	handles := make(chan *handle, callers)
	for range callers {
		wait.Go(func() {
			handle, err := registry.open(context.Background(), target)
			if err != nil {
				errors <- err
				return
			}
			handles <- handle
		})
	}
	wait.Wait()
	close(errors)
	close(handles)
	for err := range errors {
		t.Fatalf("Open() error = %v", err)
	}
	for handle := range handles {
		if err := handle.Close(); err != nil {
			t.Fatalf("handle.Close() error = %v", err)
		}
	}
	if got := factory.count(); got != 1 {
		t.Fatalf("owner constructions = %d, want 1", got)
	}
}

func TestRegistry_SerializesOperationsPerAccount(t *testing.T) {
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
		wait.Go(func() {
			err := registry.Execute(
				context.Background(),
				target,
				func(ctx context.Context, _ *gotdtelegram.Client) error {
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
				},
			)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
			}
		})
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

func TestRegistry_StopBeforeFirstOwnerFencesEqualStaleAndNewer(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	stopped := registryTarget()
	stopped.Version = 2
	if failure := registry.StopAccount(context.Background(), stopped); failure != nil {
		t.Fatalf("StopAccount() error = %v", failure)
	}
	for _, version := range []operatoraccount.Version{1, 2} {
		target := stopped
		target.Version = version
		if failure := registry.Execute(
			context.Background(),
			target,
			func(context.Context, *gotdtelegram.Client) error {
				t.Fatal("fenced callback invoked")
				return nil
			},
		); !errors.Is(failure, ErrAccountStopped) {
			t.Fatalf("version %d error = %v", version, failure)
		}
	}
	newer := stopped
	newer.Version = 3
	if failure := registry.Execute(
		context.Background(),
		newer,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); failure != nil {
		t.Fatalf("newer Execute() error = %v", failure)
	}
	if got := factory.count(); got != 1 {
		t.Fatalf("owner constructions = %d, want 1", got)
	}
}

func TestRegistry_NoSlotFenceAdvancesMonotonically(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	target.Version = 2
	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatal(failure)
	}
	target.Version = 4
	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatal(failure)
	}
	for version := operatoraccount.Version(2); version <= 4; version++ {
		stale := target
		stale.Version = version
		if failure := registry.Execute(
			context.Background(),
			stale,
			func(
				context.Context,
				*gotdtelegram.Client,
			) error {
				t.Fatal("fenced callback invoked")
				return nil
			},
		); !errors.Is(failure, ErrAccountStopped) {
			t.Fatalf("version %d error = %v", version, failure)
		}
	}
	newer := target
	newer.Version = 5
	if failure := registry.Execute(
		context.Background(),
		newer,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); failure != nil {
		t.Fatal(failure)
	}
}

func TestRegistry_FenceStateIsBounded(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 2})
	for index := 1; index <= 20; index++ {
		target := registryTarget()
		target.AccountID = operatoraccount.Identity(uuid.MustParse(fmt.Sprintf("33333333-3333-4333-8333-%012d", index)))
		target.Version = operatoraccount.Version(index)
		if failure := registry.StopAccount(context.Background(), target); failure != nil {
			t.Fatal(failure)
		}
	}
	registry.mu.Lock()
	fences, limit, slots := len(registry.fences.records), registry.fences.limit, len(registry.slots)
	registry.mu.Unlock()
	if fences > limit {
		t.Fatalf("fence state size = %d, want <= %d", fences, limit)
	}
	if slots != 0 {
		t.Fatalf("slots = %d, want 0", slots)
	}
}

func TestRegistry_ProtectedFenceSurvivesBoundedEviction(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1})
	target := registryTarget()
	if failure := registry.Execute(
		context.Background(),
		target,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); failure != nil {
		t.Fatalf("initial Execute() error = %v", failure)
	}
	owner := factory.owner(0)
	owner.holdTeardown.Store(true)
	owner.releaseTeardown = make(chan struct{})
	stopDone := make(chan error, 1)
	go func() { stopDone <- registry.StopAccount(context.Background(), target) }()
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for protected stop fence")
	}
	other := target
	other.AccountID = operatoraccount.Identity(uuid.MustParse("44444444-4444-4444-8444-444444444444"))
	if failure := registry.StopAccount(context.Background(), other); !errors.Is(failure, ErrFenceCapacity) {
		t.Fatalf("other StopAccount() error = %v, want ErrFenceCapacity", failure)
	}
	registry.mu.Lock()
	fence, exists := registry.fences.records[accountKeyFromTarget(target)]
	_, otherRecorded := registry.fences.records[accountKeyFromTarget(other)]
	fences := len(registry.fences.records)
	registry.mu.Unlock()
	if !exists || !fence.protected || fence.version != target.Version {
		t.Fatalf("protected fence = %#v, exists = %t, want protected version %d", fence, exists, target.Version)
	}
	if otherRecorded {
		t.Fatal("failed stop retained an uncommitted other-account fence")
	}
	if fences > registry.config.Capacity {
		t.Fatalf("fence state size = %d, want <= %d", fences, registry.config.Capacity)
	}
	close(owner.releaseTeardown)
	if failure := <-stopDone; failure != nil {
		t.Fatalf("StopAccount() error after release = %v", failure)
	}
	if failure := registry.Execute(context.Background(), other, func(
		context.Context,
		*gotdtelegram.Client,
	) error {
		return nil
	}); failure != nil {
		t.Fatalf("other account was falsely fenced after capacity failure: %v", failure)
	}
}

func TestRegistry_FenceRejectsOperationBeforeCallbackEntry(t *testing.T) {
	entered := make(chan struct{})
	factory := &registryOwnerFactory{executeEntered: entered, allowExecute: make(chan struct{})}
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	calls := atomic.Int32{}
	done := make(chan error, 1)
	go func() {
		done <- registry.Execute(context.Background(), target, func(
			context.Context,
			*gotdtelegram.Client,
		) error {
			calls.Add(1)
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for execute barrier")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- registry.StopAccount(context.Background(), target) }()
	select {
	case failure := <-stopDone:
		if failure != nil {
			t.Fatal(failure)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stop")
	}
	if failure := <-done; !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("operation error = %v", failure)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("callbacks after fence = %d", got)
	}
}

func TestHandle_CloseRetainsReferenceUntilAdmittedExecuteFinishes(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	handle, failure := registry.open(context.Background(), registryTarget())
	if failure != nil {
		t.Fatalf("Open() error = %v", failure)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- handle.Execute(context.Background(), func(context.Context, *gotdtelegram.Client) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for handle operation")
	}

	if failure := handle.Close(); failure != nil {
		t.Fatalf("Close() error = %v", failure)
	}
	handle.entry.slot.mu.Lock()
	handles, active := handle.entry.slot.handles, handle.entry.slot.active
	handle.entry.slot.mu.Unlock()
	if handles != 1 || active != 1 {
		t.Fatalf("slot state after concurrent Close = handles:%d active:%d, want handles:1 active:1", handles, active)
	}
	if failure := handle.Execute(context.Background(), func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("Execute() after Close error = %v, want ErrAccountStopped", failure)
	}

	close(release)
	if failure := <-done; failure != nil {
		t.Fatalf("admitted Execute() error = %v", failure)
	}
	handle.entry.slot.mu.Lock()
	handles = handle.entry.slot.handles
	handle.entry.slot.mu.Unlock()
	if handles != 0 {
		t.Fatalf("slot handles after admitted operation = %d, want 0", handles)
	}
}

func TestRegistry_StopAccountRejectsLateOpenAndAllowsNewVersion(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	if err := registry.Execute(
		context.Background(),
		target,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("initial Execute() error = %v", err)
	}
	if err := registry.StopAccount(context.Background(), target); err != nil {
		t.Fatalf("StopAccount() error = %v", err)
	}
	if err := registry.Execute(
		context.Background(),
		target,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); !errors.Is(err, ErrAccountStopped) {
		t.Fatalf("late Execute() error = %v, want ErrAccountStopped", err)
	}

	next := target
	next.Version++
	if err := registry.Execute(context.Background(), next, func(
		context.Context,
		*gotdtelegram.Client,
	) error {
		return nil
	}); err != nil {
		t.Fatalf("new-version Execute() error = %v", err)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("owner constructions = %d, want 2", got)
	}
}

func TestRegistry_StopAccountCancelsInFlightOperationAndRejectsLateResult(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()
	entered := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- func() error {
			ctx := context.Background()
			handle, err := registry.open(ctx, target)
			if err != nil {
				return err
			}
			defer handle.Close()
			return handle.Execute(
				ctx,
				func(ctx context.Context, _ *gotdtelegram.Client) error {
					close(entered)
					<-ctx.Done()
					return nil
				},
			)
		}()
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

func TestRegistry_StopAccountDrainsBeforeStoppingOwnerAndRetriesAfterTimeout(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	entered := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- func() error {
			ctx := context.Background()
			handle, err := registry.open(ctx, target)
			if err != nil {
				return err
			}
			defer handle.Close()
			return handle.Execute(ctx, func(context.Context, *gotdtelegram.Client) error {
				close(entered)
				<-release
				return nil
			})
		}()
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
	registry.config.DrainTimeout = time.Second
	if failure := registry.StopAccount(context.Background(), target); failure != nil {
		t.Fatalf("retry StopAccount() error = %v", failure)
	}
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owner stop")
	}
}

func TestRegistry_StopAccountDrainsRealOwnerCallbackBeforeStoppingOwner(t *testing.T) {
	var owner *owner
	registry, err := newRegistry(RegistryConfig{
		Capacity:     1,
		IdleTimeout:  time.Hour,
		DrainTimeout: time.Millisecond,
	}, func(gotdclient.Factory, transporttelegram.SessionScope, int, string) (ownerRuntime, error) {
		owner = newOwnerWithLifecycle(newRegistryOwnerLifecycle())
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
		operationDone <- func() error {
			ctx := context.Background()
			handle, err := registry.open(ctx, target)
			if err != nil {
				return err
			}
			defer handle.Close()
			return handle.Execute(
				ctx,
				func(ctx context.Context, _ *gotdtelegram.Client) error {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return nil
				},
			)
		}()
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
	registry.config.DrainTimeout = time.Second
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

func TestRegistry_StopRetriesStoppingEntriesAfterDrainTimeout(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{DrainTimeout: time.Millisecond})
	target := registryTarget()
	entered := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- registry.Execute(
			context.Background(),
			target,
			func(context.Context, *gotdtelegram.Client) error {
				close(entered)
				<-release
				return nil
			},
		)
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
	registry.config.DrainTimeout = time.Second
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

func TestRegistry_ReplacementWaitsForPriorOwnerTeardown(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	first := registryTarget()
	if err := registry.Execute(
		context.Background(),
		first,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	firstOwner := factory.owner(0)
	firstOwner.holdTeardown.Store(true)
	firstOwner.releaseTeardown = make(chan struct{})

	next := first
	next.Version++
	replacementDone := make(chan error, 1)
	go func() {
		replacementDone <- registry.Execute(
			context.Background(),
			next,
			func(context.Context, *gotdtelegram.Client) error {
				return nil
			},
		)
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

func TestRegistry_CapacityEvictsIdleOwner(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1, IdleTimeout: time.Hour})
	first := registryTarget()
	if err := registry.Execute(
		context.Background(),
		first,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	registry.mu.Lock()
	registry.slots[accountKeyFromTarget(first)].mu.Lock()
	registry.slots[accountKeyFromTarget(first)].lastUsed = time.Now().Add(-time.Hour)
	registry.slots[accountKeyFromTarget(first)].mu.Unlock()
	registry.mu.Unlock()
	registry.evictIdle()
	second := first
	second.AccountID = operatoraccount.Identity(uuid.MustParse("33333333-3333-4333-8333-333333333333"))
	if err := registry.Execute(
		context.Background(),
		second,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if got := factory.count(); got != 2 {
		t.Fatalf("owner constructions = %d, want idle eviction and replacement", got)
	}
}

func TestRegistry_OwnerFailureIsolatedToOneAccount(t *testing.T) {
	failure := errors.New("one account failed")
	factory := &registryOwnerFactory{fail: failure}
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	bad := registryTarget()
	good := bad
	good.AccountID = operatoraccount.Identity(uuid.MustParse("33333333-3333-4333-8333-333333333333"))

	if err := registry.Execute(
		context.Background(),
		bad,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); !errors.Is(err, failure) {
		t.Fatalf("failed-account Execute() error = %v, want owner failure", err)
	}
	factory.mu.Lock()
	factory.fail = nil
	factory.mu.Unlock()
	if err := registry.Execute(
		context.Background(),
		good,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("independent-account Execute() error = %v", err)
	}
}

func TestRegistry_StopDuringBuildStopsRejectedOwner(t *testing.T) {
	buildEntered := make(chan struct{})
	allowBuild := make(chan struct{})
	factory := &registryOwnerFactory{
		buildEntered: buildEntered,
		allowBuild:   allowBuild,
	}
	registry := newFakeRegistry(t, factory, RegistryConfig{})
	target := registryTarget()

	callbackCalled := make(chan struct{}, 1)
	executeDone := make(chan error, 1)
	go func() {
		executeDone <- registry.Execute(context.Background(), target, func(
			context.Context,
			*gotdtelegram.Client,
		) error {
			callbackCalled <- struct{}{}
			return nil
		})
	}()
	select {
	case <-buildEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owner build")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- registry.StopAccount(context.Background(), target)
	}()
	deadline := time.After(time.Second)
	for {
		registry.mu.Lock()
		slot := registry.slots[accountKeyFromTarget(target)]
		closed := false
		if slot != nil {
			slot.mu.Lock()
			closed = slot.closed
			slot.mu.Unlock()
		}
		registry.mu.Unlock()
		if closed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for admission closure")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(allowBuild)
	if failure := <-executeDone; !errors.Is(failure, ErrAccountStopped) {
		t.Fatalf("Execute() error = %v, want ErrAccountStopped", failure)
	}
	if failure := <-stopDone; failure != nil {
		t.Fatalf("StopAccount() error = %v", failure)
	}
	select {
	case <-callbackCalled:
		t.Fatal("callback invoked for owner rejected during build")
	default:
	}
	owner := factory.owner(0)
	select {
	case <-owner.stopped:
	case <-time.After(time.Second):
		t.Fatal("owner built after admission closure was not stopped")
	}
}

func TestRegistry_FailedBuildRemovesEmptySlot(t *testing.T) {
	failure := errors.New("owner construction failed")
	factory := &registryOwnerFactory{fail: failure}
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1})
	target := registryTarget()

	for range 20 {
		if err := registry.Execute(
			context.Background(),
			target,
			func(context.Context, *gotdtelegram.Client) error {
				return nil
			},
		); !errors.Is(err, failure) {
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

func TestRegistry_QuiescesToEmptySlotTableAfterStop(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 4, IdleTimeout: time.Hour})
	target := registryTarget()

	execute := func(tr operatoraccounts.RuntimeTarget) {
		t.Helper()
		if failure := registry.Execute(context.Background(), tr, func(context.Context, *gotdtelegram.Client) error {
			return nil
		}); failure != nil {
			t.Fatalf("Execute() error = %v", failure)
		}
	}

	first := target
	execute(first)
	if failure := registry.StopAccount(context.Background(), first); failure != nil {
		t.Fatalf("StopAccount() error = %v", failure)
	}
	replacement := first
	replacement.Version++
	execute(replacement)

	revoked := registryTarget()
	revoked.AccountID = operatoraccount.Identity(uuid.MustParse("33333333-3333-4333-8333-333333333333"))
	revoked.Status = operatoraccount.StatusActive
	execute(revoked)
	disconnecting := revoked
	disconnecting.Status = operatoraccount.StatusDisconnecting
	disconnecting.Version++
	if failure := registry.RevokeAndStop(context.Background(), disconnecting, func(context.Context, *gotdtelegram.Client) error {
		return nil
	}); failure != nil {
		t.Fatalf("RevokeAndStop() error = %v", failure)
	}

	idle := registryTarget()
	idle.AccountID = operatoraccount.Identity(uuid.MustParse("44444444-4444-4444-8444-444444444444"))
	execute(idle)
	registry.mu.Lock()
	idleSlot := registry.slots[accountKeyFromTarget(idle)]
	registry.mu.Unlock()
	idleSlot.mu.Lock()
	idleSlot.lastUsed = time.Now().Add(-time.Hour)
	idleSlot.mu.Unlock()
	registry.evictIdle()

	if failure := registry.Stop(context.Background()); failure != nil {
		t.Fatalf("Stop() error = %v", failure)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.slots) != 0 {
		t.Fatalf("slot table after Stop = %d entries, want 0", len(registry.slots))
	}
}
