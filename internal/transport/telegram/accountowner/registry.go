package accountowner

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	operatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

var (
	// ErrRegistryStopped means that the process-wide runtime no longer admits
	// new account owners.
	ErrRegistryStopped = errors.New("telegram account runtime stopped")
	// ErrAccountStopped means that admission for this operator/account scope was
	// closed before the operation could begin.
	ErrAccountStopped = errors.New("telegram account runtime stopped for account")
	// ErrStaleAdmission means that an operation used an older lifecycle version
	// than the currently admitted version.
	ErrStaleAdmission = errors.New("telegram account runtime admission is stale")
	// ErrInvalidAdmission means that the requested lifecycle state cannot own a
	// runtime through this boundary.
	ErrInvalidAdmission = errors.New("telegram account runtime admission is invalid")
	// ErrRuntimeCapacity means that all runtime slots are occupied by accounts
	// that are not idle and therefore cannot be evicted safely.
	ErrRuntimeCapacity = errors.New("telegram account runtime capacity is exhausted")
	// ErrNilCallback means that no operation was supplied to Execute.
	ErrNilCallback = errors.New("telegram account runtime callback is required")
)

const (
	defaultRuntimeCapacity     = 32
	defaultRuntimeIdleTimeout  = 5 * time.Minute
	defaultRuntimeDrainTimeout = 5 * time.Second
)

// RegistryConfig controls the bounded, process-local account runtime.
//
// A registry is intentionally a single-deployment component. Session bytes
// and lifecycle state remain durable elsewhere; this registry only owns the
// currently running gotd clients and their admission fences.
type RegistryConfig struct {
	Factory gotdclient.Factory
	AppID   int
	AppHash string

	// Capacity is the maximum number of admitted account owners. A zero value
	// uses the conservative default.
	Capacity int
	// IdleTimeout controls when an owner with no open handles or operations may
	// be evicted. A zero value uses the default.
	IdleTimeout time.Duration
	// DrainTimeout bounds per-account draining during Stop and eviction. A zero
	// value uses the default.
	DrainTimeout time.Duration
}

// ownerRuntime is the narrow lifecycle and callback surface the registry
// consumes. Keeping the seam here lets registry tests use deterministic owner
// fakes without exposing a gotd dependency to the registry contract.
type ownerRuntime interface {
	Run(context.Context) error
	Stop()
	WaitReady(context.Context) error
	Wait(context.Context) error
	Execute(context.Context, ClientCallback) error
}

type ownerBuilder func(
	gotdclient.Factory,
	transporttelegram.SessionScope,
	int,
	string,
) (ownerRuntime, error)

// Registry owns at most one gotd client for each operator/account key. The
// lifecycle version is admission metadata on that key, not a second client
// key: replacing a version therefore shares the same account gate and cannot
// overlap with an older operation.
type Registry struct {
	mu sync.Mutex

	config  RegistryConfig
	build   ownerBuilder
	slots   map[accountKey]*accountSlot
	stopped bool

	context     context.Context
	cancel      context.CancelFunc
	reaperDone  chan struct{}
	stopReaper  chan struct{}
	stopOnce    sync.Once
	rootStopped bool
}

// accountKey deliberately excludes lifecycle version. All versions of one
// operator/account must pass through one serialized gate.
type accountKey struct {
	operatorID uuid.UUID
	accountID  uuid.UUID
}

type accountSlot struct {
	mu sync.Mutex

	gate         chan struct{}
	current      *runtimeEntry
	closed       bool
	stopping     bool
	fenced       bool
	fenceVersion operatoraccount.Version
	generation   uint64
	teardownMu   sync.Mutex

	refs         int
	active       int
	activeDone   chan struct{}
	activeCancel context.CancelFunc
	lastUsed     time.Time
}

type runtimeEntry struct {
	registry   *Registry
	slot       *accountSlot
	target     operatoraccountauth.AuthTarget
	generation uint64

	mu       sync.Mutex
	owner    ownerRuntime
	buildErr error
	building bool
	failed   bool
	built    chan struct{}
	runDone  chan struct{}
	runOnce  sync.Once
}

// Handle is a scoped admission to one account runtime. It does not expose a
// gotd client. Call Execute for each logical operation and close the handle
// when the caller no longer needs to retain the admission.
type Handle struct {
	entry  *runtimeEntry
	target operatoraccountauth.AuthTarget

	mu     sync.Mutex
	closed bool
}

var _ operatoraccountauth.RuntimeStopper = (*Registry)(nil)

// NewRegistry constructs a runtime registry without starting any gotd client.
// Client construction and Run both remain lazy until Open is called.
func NewRegistry(config RegistryConfig) (*Registry, error) {
	return newRegistry(config, func(
		factory gotdclient.Factory,
		scope transporttelegram.SessionScope,
		appID int,
		appHash string,
	) (ownerRuntime, error) {
		return New(factory, scope, appID, appHash)
	})
}

func newRegistry(config RegistryConfig, build ownerBuilder) (*Registry, error) {
	if config.Capacity == 0 {
		config.Capacity = defaultRuntimeCapacity
	}
	if config.Capacity < 0 {
		return nil, errors.New("telegram account runtime capacity must not be negative")
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = defaultRuntimeIdleTimeout
	}
	if config.IdleTimeout < 0 {
		return nil, errors.New("telegram account runtime idle timeout must not be negative")
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultRuntimeDrainTimeout
	}
	if config.DrainTimeout < 0 {
		return nil, errors.New("telegram account runtime drain timeout must not be negative")
	}
	if build == nil {
		return nil, errors.New("telegram account runtime owner builder is required")
	}

	runtimeContext, cancel := context.WithCancel(context.Background())
	registry := &Registry{
		config:     config,
		build:      build,
		slots:      make(map[accountKey]*accountSlot),
		context:    runtimeContext,
		cancel:     cancel,
		reaperDone: make(chan struct{}),
		stopReaper: make(chan struct{}),
	}
	go registry.reapIdle()
	return registry, nil
}

// Open admits target and waits for the current owner to become ready. Existing
// owners are reused; readiness is a current-state wait and is therefore safe
// across gotd reconnects.
func (registry *Registry) Open(ctx context.Context, target operatoraccountauth.AuthTarget) (*Handle, error) {
	if failure := validateAdmission(target); failure != nil {
		return nil, failure
	}

	entry, err := registry.reserve(ctx, target)
	if err != nil {
		return nil, err
	}
	handle := &Handle{entry: entry, target: target}
	if err := entry.waitBuilt(ctx); err != nil {
		handle.Close()
		return nil, err
	}
	owner := entry.getOwner()
	if owner == nil {
		handle.Close()
		return nil, ErrAccountStopped
	}
	if err := owner.WaitReady(ctx); err != nil {
		handle.Close()
		return nil, err
	}
	if err := registry.checkAdmission(entry, target); err != nil {
		handle.Close()
		return nil, err
	}
	return handle, nil
}

// Execute opens an admission for one logical operation, executes the callback
// while holding the account gate, and releases the admission afterwards.
func (registry *Registry) Execute(
	ctx context.Context,
	target operatoraccountauth.AuthTarget,
	callback ClientCallback,
) error {
	if callback == nil {
		return ErrNilCallback
	}
	handle, err := registry.Open(ctx, target)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Execute(ctx, callback)
}

// Execute runs callback under the handle's account gate. A callback is never
// invoked after the admission fence has been closed or replaced.
func (handle *Handle) Execute(ctx context.Context, callback ClientCallback) error {
	if callback == nil {
		return ErrNilCallback
	}
	if handle == nil {
		return ErrAccountStopped
	}
	handle.mu.Lock()
	closed := handle.closed
	handle.mu.Unlock()
	if closed {
		return ErrAccountStopped
	}
	return handle.entry.execute(ctx, handle.target, callback)
}

// Close releases the handle's reservation. It is idempotent and does not
// stop the shared owner; idle eviction or StopAccount performs teardown.
func (handle *Handle) Close() error {
	if handle == nil {
		return nil
	}
	handle.mu.Lock()
	if handle.closed {
		handle.mu.Unlock()
		return nil
	}
	handle.closed = true
	handle.mu.Unlock()
	handle.entry.releaseRef()
	return nil
}

// StopAccount closes admission for the exact lifecycle target, cancels the
// current operation, then waits for bounded drain and owner teardown. The
// registry lock is never held while waiting on a callback or gotd.
func (registry *Registry) StopAccount(
	ctx context.Context,
	target operatoraccountauth.AuthTarget,
) error {
	if failure := validateStopTarget(target); failure != nil {
		return failure
	}
	key := keyFor(target)

	registry.mu.Lock()
	if registry.stopped {
		registry.mu.Unlock()
		return nil
	}
	slot := registry.slots[key]
	if slot == nil || slot.current == nil {
		registry.mu.Unlock()
		return nil
	}
	entry := slot.current
	if entry.target.Actor != target.Actor || entry.target.AccountID != target.AccountID || entry.target.Version != target.Version {
		registry.mu.Unlock()
		return ErrStaleAdmission
	}
	if entry.target.Status != target.Status {
		registry.mu.Unlock()
		return ErrInvalidAdmission
	}
	slot.mu.Lock()
	if !slot.stopping {
		slot.closed = true
		slot.stopping = true
	}
	slot.fenced = true
	slot.fenceVersion = target.Version
	cancel := slot.closeAdmissionLocked()
	slot.mu.Unlock()
	registry.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return registry.teardown(slot, entry, ctx)
}

// Stop closes every account admission and joins each owner within the supplied
// context and configured drain bound. It is safe to call more than once.
func (registry *Registry) Stop(ctx context.Context) error {
	registry.stopOnce.Do(func() {
		close(registry.stopReaper)
		var cancels []context.CancelFunc
		registry.mu.Lock()
		registry.stopped = true
		for _, slot := range registry.slots {
			if slot.current == nil {
				continue
			}
			slot.mu.Lock()
			slot.closed = true
			slot.stopping = true
			if cancel := slot.closeAdmissionLocked(); cancel != nil {
				cancels = append(cancels, cancel)
			}
			slot.mu.Unlock()
		}
		registry.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
	})

	var first error
	for {
		entries := registry.stopEntries()
		if len(entries) == 0 {
			break
		}
		iterationFailed := false
		for _, item := range entries {
			if err := registry.teardown(item.slot, item.entry, ctx); err != nil {
				iterationFailed = true
				if first == nil {
					first = err
				}
			}
		}
		if iterationFailed || ctx.Err() != nil {
			break
		}
	}
	<-registry.reaperDone

	registry.mu.Lock()
	complete := true
	for _, slot := range registry.slots {
		if slot.current != nil {
			complete = false
			break
		}
	}
	stopRoot := complete && !registry.rootStopped
	if stopRoot {
		registry.rootStopped = true
	}
	registry.mu.Unlock()
	if stopRoot {
		registry.cancel()
	}
	return first
}

func (registry *Registry) stopEntries() []struct {
	slot  *accountSlot
	entry *runtimeEntry
} {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entries := make([]struct {
		slot  *accountSlot
		entry *runtimeEntry
	}, 0, len(registry.slots))
	for _, slot := range registry.slots {
		if slot.current != nil {
			entries = append(entries, struct {
				slot  *accountSlot
				entry *runtimeEntry
			}{slot: slot, entry: slot.current})
		}
	}
	return entries
}

func (registry *Registry) reserve(
	ctx context.Context,
	target operatoraccountauth.AuthTarget,
) (*runtimeEntry, error) {
	key := keyFor(target)
	for {
		registry.mu.Lock()
		if registry.stopped {
			registry.mu.Unlock()
			return nil, ErrRegistryStopped
		}
		slot := registry.slots[key]
		if slot == nil {
			evicted := registry.makeCapacityLocked()
			if evicted != nil {
				registry.mu.Unlock()
				if failure := registry.teardown(evicted.slot, evicted, ctx); failure != nil {
					return nil, failure
				}
				continue
			}
			if registry.liveCountLocked() >= registry.config.Capacity {
				registry.mu.Unlock()
				return nil, ErrRuntimeCapacity
			}
			slot = newAccountSlot()
			registry.slots[key] = slot
		}

		slot.mu.Lock()
		current := slot.current
		stopping := slot.stopping
		closed := slot.closed
		fenced := slot.fenced
		fenceVersion := slot.fenceVersion
		if current == nil && closed && fenced && !stopping && target.Version > fenceVersion {
			slot.closed = false
			slot.fenced = false
			closed = false
		}
		slot.mu.Unlock()

		if current != nil {
			switch {
			case current.target == target && !stopping:
				slot.mu.Lock()
				reserved := slot.reserveRefLocked(current)
				slot.mu.Unlock()
				registry.mu.Unlock()
				if !reserved {
					return nil, ErrAccountStopped
				}
				return current, nil
			case target.Version < current.target.Version:
				registry.mu.Unlock()
				return nil, ErrStaleAdmission
			case target.Version == current.target.Version:
				registry.mu.Unlock()
				return nil, ErrInvalidAdmission
			default:
				// Keep the old entry installed while it drains. A replacement
				// cannot construct or start a second client for this account.
				if !stopping {
					slot.mu.Lock()
					slot.closed = true
					slot.stopping = true
					cancel := slot.closeAdmissionLocked()
					slot.mu.Unlock()
					registry.mu.Unlock()
					if cancel != nil {
						cancel()
					}
				} else {
					registry.mu.Unlock()
				}
				if failure := registry.teardown(slot, current, ctx); failure != nil {
					return nil, failure
				}
				continue
			}
		}

		if stopping || closed {
			registry.mu.Unlock()
			return nil, ErrAccountStopped
		}
		entry := registry.newEntry(slot, target)
		slot.mu.Lock()
		canPublish := !registry.stopped && slot.current == nil && !slot.closed && !slot.stopping
		if canPublish {
			slot.current = entry
			slot.generation++
			entry.generation = slot.generation
			canPublish = slot.reserveRefLocked(entry)
		}
		slot.mu.Unlock()
		registry.mu.Unlock()
		if !canPublish {
			return nil, ErrAccountStopped
		}
		go registry.buildEntry(entry)
		return entry, nil
	}
}

func (registry *Registry) newEntry(
	slot *accountSlot,
	target operatoraccountauth.AuthTarget,
) *runtimeEntry {
	return &runtimeEntry{
		registry: registry,
		slot:     slot,
		target:   target,
		building: true,
		built:    make(chan struct{}),
		runDone:  make(chan struct{}),
	}
}

func (registry *Registry) buildEntry(entry *runtimeEntry) {
	scope := transporttelegram.SessionScope{
		OperatorID: entry.target.Actor.OperatorID,
		AccountID:  entry.target.AccountID.UUID(),
	}
	owner, failure := registry.build(registry.config.Factory, scope, registry.config.AppID, registry.config.AppHash)

	registry.mu.Lock()
	entry.slot.mu.Lock()
	closed := entry.slot.closed
	entry.slot.mu.Unlock()
	valid := !registry.stopped && entry.slot.current == entry && !closed
	if failure != nil || !valid {
		if failure == nil {
			failure = ErrAccountStopped
		}
		if failure != nil && valid && entry.slot.current == entry {
			entry.slot.mu.Lock()
			entry.slot.current = nil
			entry.slot.mu.Unlock()
			entry.mu.Lock()
			entry.failed = true
			entry.mu.Unlock()
		}
	}
	entry.finishBuild(owner, failure)
	registry.mu.Unlock()

	if failure != nil {
		if valid && owner != nil {
			owner.Stop()
		}
		if valid {
			registry.cleanupFailedEntry(entry)
		}
		return
	}

	go registry.runEntry(entry, owner)
}

func (entry *runtimeEntry) finishBuild(owner ownerRuntime, failure error) {
	entry.mu.Lock()
	entry.owner = owner
	entry.buildErr = failure
	entry.building = false
	close(entry.built)
	entry.mu.Unlock()
}

func (entry *runtimeEntry) waitBuilt(ctx context.Context) error {
	select {
	case <-entry.built:
		entry.mu.Lock()
		failure := entry.buildErr
		entry.mu.Unlock()
		return failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (entry *runtimeEntry) getOwner() ownerRuntime {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.owner
}

func (registry *Registry) cleanupFailedEntry(entry *runtimeEntry) {
	entry.slot.mu.Lock()
	refs := entry.slot.refs
	current := entry.slot.current
	entry.slot.mu.Unlock()
	if refs == 0 && current == nil {
		registry.removeSlot(entry.slot)
	}
}

func (registry *Registry) runEntry(entry *runtimeEntry, owner ownerRuntime) {
	failure := owner.Run(registry.context)
	entry.runOnce.Do(func() { close(entry.runDone) })
	registry.mu.Lock()
	if entry.slot.current == entry {
		entry.slot.mu.Lock()
		if !entry.slot.stopping {
			entry.slot.current = nil
		}
		entry.slot.lastUsed = time.Now()
		entry.slot.mu.Unlock()
	}
	registry.mu.Unlock()
	entry.mu.Lock()
	entry.buildErr = failure
	entry.mu.Unlock()
	registry.finishStoppedEntry(entry)
}

func (registry *Registry) checkAdmission(
	entry *runtimeEntry,
	target operatoraccountauth.AuthTarget,
) error {
	entry.slot.mu.Lock()
	defer entry.slot.mu.Unlock()
	return admissionErrorLocked(entry.slot, entry, target)
}

func admissionErrorLocked(
	slot *accountSlot,
	entry *runtimeEntry,
	target operatoraccountauth.AuthTarget,
) error {
	if slot.closed || slot.stopping {
		return ErrAccountStopped
	}
	if slot.current != entry {
		return ErrStaleAdmission
	}
	if entry.target.Version != target.Version || entry.target.Actor != target.Actor || entry.target.AccountID != target.AccountID {
		return ErrStaleAdmission
	}
	if entry.target.Status != target.Status {
		return ErrInvalidAdmission
	}
	return nil
}

func (entry *runtimeEntry) execute(
	ctx context.Context,
	target operatoraccountauth.AuthTarget,
	callback ClientCallback,
) error {
	slot := entry.slot
	select {
	case <-slot.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { slot.gate <- struct{}{} }()

	operationContext, cancel := context.WithCancel(ctx)
	slot.mu.Lock()
	if failure := admissionErrorLocked(slot, entry, target); failure != nil {
		slot.mu.Unlock()
		cancel()
		return failure
	}
	if slot.active == 0 {
		slot.activeDone = make(chan struct{})
	}
	slot.active++
	slot.activeCancel = cancel
	slot.lastUsed = time.Now()
	slot.mu.Unlock()
	defer func() {
		cancel()
		slot.mu.Lock()
		slot.active--
		slot.activeCancel = nil
		if slot.active == 0 {
			close(slot.activeDone)
		}
		slot.lastUsed = time.Now()
		slot.mu.Unlock()
		entry.registry.finishStoppedEntry(entry)
	}()

	owner := entry.getOwner()
	if owner == nil {
		return ErrAccountStopped
	}
	if failure := owner.WaitReady(operationContext); failure != nil {
		return failure
	}
	slot.mu.Lock()
	failure := admissionErrorLocked(slot, entry, target)
	slot.mu.Unlock()
	if failure != nil {
		return failure
	}
	failure = owner.Execute(operationContext, callback)
	slot.mu.Lock()
	admissionFailure := admissionErrorLocked(slot, entry, target)
	slot.mu.Unlock()
	if admissionFailure != nil {
		return admissionFailure
	}
	return failure
}

func (registry *Registry) finishStoppedEntry(entry *runtimeEntry) {
	select {
	case <-entry.runDone:
	default:
		return
	}
	registry.mu.Lock()
	if entry.slot.current != nil {
		registry.mu.Unlock()
		return
	}
	entry.slot.mu.Lock()
	active := entry.slot.active
	if active == 0 && entry.slot.fenced {
		entry.slot.stopping = false
	}
	entry.slot.mu.Unlock()
	registry.mu.Unlock()
	if active == 0 {
		registry.removeSlot(entry.slot)
	}
}

func (entry *runtimeEntry) releaseRef() {
	entry.slot.mu.Lock()
	if entry.slot.refs > 0 {
		entry.slot.refs--
	}
	entry.slot.lastUsed = time.Now()
	refs := entry.slot.refs
	current := entry.slot.current
	entry.slot.mu.Unlock()
	entry.mu.Lock()
	failed := entry.failed
	entry.mu.Unlock()
	if failed && refs == 0 && current == nil {
		entry.registry.removeSlot(entry.slot)
	}
}

func newAccountSlot() *accountSlot {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &accountSlot{
		gate:       gate,
		activeDone: closedSignal(),
		lastUsed:   time.Now(),
	}
}

func closedSignal() chan struct{} {
	closed := make(chan struct{})
	close(closed)
	return closed
}

func (slot *accountSlot) reserveRefLocked(entry *runtimeEntry) bool {
	if slot.closed || slot.stopping {
		return false
	}
	if entry != nil && slot.current != entry {
		return false
	}
	slot.refs++
	slot.lastUsed = time.Now()
	return true
}

func (slot *accountSlot) closeAdmissionLocked() context.CancelFunc {
	cancel := slot.activeCancel
	slot.activeCancel = nil
	return cancel
}

func (slot *accountSlot) waitDrained(ctx context.Context) error {
	slot.mu.Lock()
	done := slot.activeDone
	slot.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (registry *Registry) teardown(slot *accountSlot, entry *runtimeEntry, ctx context.Context) error {
	slot.teardownMu.Lock()
	defer slot.teardownMu.Unlock()

	drainContext, cancel := registry.boundedContext(ctx)
	defer cancel()

	if failure := entry.waitBuilt(drainContext); failure != nil && !errors.Is(failure, ErrAccountStopped) {
		if !isContextFailure(failure) {
			registry.finishTeardown(slot, entry)
		}
		return failure
	}
	if failure := slot.waitDrained(drainContext); failure != nil {
		return failure
	}
	if owner := entry.getOwner(); owner != nil {
		// Callback drain is complete before this point. Stopping gotd before
		// this wait would allow client.Run to return underneath an admitted
		// callback.
		owner.Stop()
		if failure := owner.Wait(drainContext); failure != nil && !errors.Is(failure, ErrStopped) {
			if !isContextFailure(failure) {
				registry.finishTeardown(slot, entry)
			}
			return failure
		}
	}
	registry.finishTeardown(slot, entry)
	return nil
}

func (registry *Registry) finishTeardown(slot *accountSlot, entry *runtimeEntry) {
	registry.mu.Lock()
	if slot.current == entry {
		slot.mu.Lock()
		slot.current = nil
		slot.stopping = false
		if slot.fenced {
			slot.closed = true
		} else {
			slot.closed = false
		}
		slot.mu.Unlock()
	}
	registry.mu.Unlock()
	registry.removeSlot(slot)
}

func isContextFailure(failure error) bool {
	return errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded)
}

func (registry *Registry) boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if registry.config.DrainTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, registry.config.DrainTimeout)
}

func (registry *Registry) removeSlot(slot *accountSlot) {
	registry.mu.Lock()
	for key, candidate := range registry.slots {
		candidate.mu.Lock()
		keepFence := candidate.fenced
		candidate.mu.Unlock()
		if candidate == slot && candidate.current == nil && !keepFence {
			delete(registry.slots, key)
			break
		}
	}
	registry.mu.Unlock()
}

func (registry *Registry) makeCapacityLocked() *runtimeEntry {
	if registry.liveCountLocked() < registry.config.Capacity {
		return nil
	}
	now := time.Now()
	for _, slot := range registry.slots {
		slot.mu.Lock()
		idle := slot.current != nil && !slot.closed && !slot.stopping && slot.refs == 0 && slot.active == 0 && now.Sub(slot.lastUsed) >= registry.config.IdleTimeout
		if idle {
			entry := slot.current
			slot.closed = true
			slot.stopping = true
			slot.mu.Unlock()
			return entry
		}
		slot.mu.Unlock()
	}
	return nil
}

func (registry *Registry) liveCountLocked() int {
	count := 0
	for _, slot := range registry.slots {
		slot.mu.Lock()
		live := slot.current != nil && !slot.closed && !slot.stopping
		slot.mu.Unlock()
		if live {
			count++
		}
	}
	return count
}

func (registry *Registry) reapIdle() {
	interval := registry.config.IdleTimeout
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(registry.reaperDone)
	for {
		select {
		case <-ticker.C:
			registry.evictIdle()
		case <-registry.stopReaper:
			return
		}
	}
}

func (registry *Registry) evictIdle() {
	var retired []struct {
		slot  *accountSlot
		entry *runtimeEntry
	}
	now := time.Now()
	registry.mu.Lock()
	for _, slot := range registry.slots {
		slot.mu.Lock()
		if slot.current != nil && !slot.closed && !slot.stopping && slot.refs == 0 && slot.active == 0 && now.Sub(slot.lastUsed) >= registry.config.IdleTimeout {
			entry := slot.current
			slot.closed = true
			slot.stopping = true
			retired = append(retired, struct {
				slot  *accountSlot
				entry *runtimeEntry
			}{slot: slot, entry: entry})
		}
		slot.mu.Unlock()
	}
	registry.mu.Unlock()

	for _, item := range retired {
		_ = registry.teardown(item.slot, item.entry, context.Background())
	}
}

func validateAdmission(target operatoraccountauth.AuthTarget) error {
	if failure := validateTarget(target); failure != nil {
		return failure
	}
	switch target.Status {
	case operatoraccount.StatusAuthenticating, operatoraccount.StatusActive:
		return nil
	default:
		return ErrInvalidAdmission
	}
}

func validateStopTarget(target operatoraccountauth.AuthTarget) error {
	if failure := validateTarget(target); failure != nil {
		return failure
	}
	switch target.Status {
	case operatoraccount.StatusAuthenticating, operatoraccount.StatusDisconnecting:
		return nil
	default:
		return ErrInvalidAdmission
	}
}

func validateTarget(target operatoraccountauth.AuthTarget) error {
	if target.Actor.OperatorID == uuid.Nil || target.AccountID.IsZero() || target.Version == 0 {
		return ErrInvalidAdmission
	}
	return nil
}

func keyFor(target operatoraccountauth.AuthTarget) accountKey {
	return accountKey{
		operatorID: target.Actor.OperatorID,
		accountID:  target.AccountID.UUID(),
	}
}
