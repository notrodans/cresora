package accountowner

import (
	"context"
	"errors"
	"sync"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

// Registry owns at most one gotd client for each operator/account key. The
// lifecycle version is admission metadata on that key, not a second client
// key: replacing a version therefore shares the same account gate and cannot
// overlap with an older operation.
type Registry struct {
	mu sync.Mutex

	config RegistryConfig
	build  ownerBuilder
	slots  map[accountKey]*accountSlot

	// fences is bounded by config.Capacity. Protected records belong to slots that
	// are still stopping and cannot be evicted until those slots are teardown.
	fences  fenceSet
	stopped bool

	runtimeContext context.Context
	cancel         context.CancelFunc
	reaperDone     chan struct{}
	stopReaper     chan struct{}
	stopOnce       sync.Once
	rootStopped    bool
}

var _ interface {
	StopAccount(context.Context, operatoraccounts.RuntimeTarget) error
} = (*Registry)(nil)

// NewRegistry constructs a runtime registry without starting any gotd client.
// Client construction and Run both remain lazy until open is called.
func NewRegistry(config RegistryConfig) (*Registry, error) {
	return newRegistry(
		config,
		func(
			factory gotdclient.Factory,
			scope transporttelegram.SessionScope,
			appID int,
			appHash string,
		) (ownerRuntime, error) {
			return newOwner(factory, scope, appID, appHash)
		},
	)
}

func newRegistry(config RegistryConfig, build ownerBuilder) (*Registry, error) {
	normalizedConfig, err := normalizeRegistryConfig(config)
	if err != nil {
		return nil, err
	}
	if build == nil {
		return nil, errors.New("telegram account runtime owner builder is required")
	}

	runtimeContext, cancel := context.WithCancel(context.Background())
	registry := &Registry{
		config:         normalizedConfig,
		build:          build,
		slots:          make(map[accountKey]*accountSlot),
		fences:         newFenceSet(normalizedConfig.Capacity),
		runtimeContext: runtimeContext,
		cancel:         cancel,
		reaperDone:     make(chan struct{}),
		stopReaper:     make(chan struct{}),
	}
	go registry.reapIdle()
	return registry, nil
}

// open admits target and waits for the current owner to become ready. Existing
// owners are reused; readiness is a current-state wait and is therefore safe
// across gotd reconnects.
func (registry *Registry) open(ctx context.Context, target operatoraccounts.RuntimeTarget) (*handle, error) {
	if failure := validateAdmission(target); failure != nil {
		return nil, failure
	}

	runtimeEntry, err := registry.reserve(ctx, target)
	if err != nil {
		return nil, err
	}
	handle := &handle{entry: runtimeEntry, target: target}
	if err := runtimeEntry.waitBuilt(ctx); err != nil {
		handle.Close()
		return nil, err
	}
	owner := runtimeEntry.Owner()
	if owner == nil {
		handle.Close()
		return nil, ErrAccountStopped
	}
	if err := owner.WaitReady(ctx); err != nil {
		handle.Close()
		return nil, err
	}
	if err := registry.checkAdmission(runtimeEntry, target); err != nil {
		handle.Close()
		return nil, err
	}
	return handle, nil
}

// Execute opens an admission for one logical operation, executes the callback
// while holding the account gate, and releases the admission afterwards.
func (registry *Registry) Execute(
	ctx context.Context,
	target operatoraccounts.RuntimeTarget,
	callback ClientCallback,
) error {
	handle, err := registry.open(ctx, target)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Execute(ctx, callback)
}

func (registry *Registry) buildEntry(entry *runtimeEntry) {
	scope := transporttelegram.SessionScope{
		OperatorID: entry.target.Actor.OperatorID,
		AccountID:  entry.target.AccountID.UUID(),
	}
	owner, failure := registry.build(registry.config.Factory, scope, registry.config.AppID, registry.config.AppHash)

	registry.mu.Lock()
	entry.slot.mu.Lock()
	valid := !registry.stopped && entry.slot.current == entry &&
		(!entry.slot.closed || entry.slot.revokeRunning)
	if failure != nil || !valid {
		if failure == nil {
			failure = ErrAccountStopped
		}
		if valid {
			entry.slot.current = nil
		}
	}
	entry.slot.mu.Unlock()
	publishedOwner := owner
	if failure != nil {
		publishedOwner = nil
	}
	entry.finishBuild(publishedOwner, failure)
	registry.mu.Unlock()

	if failure != nil {
		if owner != nil {
			owner.Stop()
		}
		if valid {
			registry.cleanupFailedEntry(entry)
		}
		return
	}

	go registry.runEntry(entry, owner)
}

func isContextFailure(failure error) bool {
	return errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded)
}
