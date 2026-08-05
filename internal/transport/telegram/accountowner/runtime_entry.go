package accountowner

import (
	"context"
	"sync"
	"time"

	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// runtimeEntry represents concrete lifecycle version of an account owner.
type runtimeEntry struct {
	registry      *Registry
	slot          *accountSlot
	target        operatoraccounts.RuntimeTarget
	privateRevoke bool

	mu       sync.Mutex
	owner    ownerRuntime
	buildErr error
	built    chan struct{}
	runDone  chan struct{}
	runOnce  sync.Once
}

func (entry *runtimeEntry) newRuntimeEntry(
	registry *Registry,
	slot *accountSlot,
	target operatoraccounts.RuntimeTarget,
) *runtimeEntry {
	return &runtimeEntry{
		registry: registry,
		slot:     slot,
		target:   target,
		built:    make(chan struct{}),
		runDone:  make(chan struct{}),
	}
}

func (entry *runtimeEntry) Owner() ownerRuntime {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.owner
}

func (entry *runtimeEntry) execute(
	ctx context.Context,
	target operatoraccounts.RuntimeTarget,
	callback ClientCallback,
) error {
	slot := entry.slot
	select {
	case <-slot.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		slot.gate <- struct{}{}
	}()

	operationContext, cancel := context.WithCancel(ctx)
	if failure := slot.beginOperation(entry, target, cancel); failure != nil {
		cancel()
		return failure
	}
	defer func() {
		cancel()
		slot.finishOperation()
		entry.registry.finishStoppedEntry(entry)
	}()

	owner := entry.Owner()
	if owner == nil {
		return ErrAccountStopped
	}
	if failure := owner.WaitReady(operationContext); failure != nil {
		return entry.operationFailure(target, failure)
	}

	failure := owner.Execute(operationContext, func(
		callbackContext context.Context,
		client *gotdtelegram.Client,
	) error {
		if admissionFailure := entry.registry.checkAdmission(entry, target); admissionFailure != nil {
			return admissionFailure
		}
		if callbackFailure := callbackContext.Err(); callbackFailure != nil {
			return callbackFailure
		}
		return callback(callbackContext, client)
	})
	return entry.operationFailure(target, failure)
}

func (entry *runtimeEntry) releaseHandle() {
	entry.slot.mu.Lock()
	if entry.slot.handles > 0 {
		entry.slot.handles--
	}
	entry.slot.lastUsed = time.Now()
	handles := entry.slot.handles
	current := entry.slot.current
	active := entry.slot.active
	stopping := entry.slot.stopping
	entry.slot.mu.Unlock()
	if handles == 0 && current == nil && active == 0 && !stopping {
		entry.registry.removeSlot(entry.slot)
	}
}

func (entry *runtimeEntry) operationFailure(
	target operatoraccounts.RuntimeTarget,
	failure error,
) error {
	if admissionFailure := entry.registry.checkAdmission(entry, target); admissionFailure != nil {
		return admissionFailure
	}
	return failure
}

func (registry *Registry) runEntry(entry *runtimeEntry, owner ownerRuntime) {
	failure := owner.Run(registry.context)
	entry.runOnce.Do(func() { close(entry.runDone) })
	entry.slot.mu.Lock()
	if entry.slot.current == entry {
		if !entry.slot.stopping {
			entry.slot.current = nil
		}
		entry.slot.lastUsed = time.Now()
	}
	entry.slot.mu.Unlock()
	entry.mu.Lock()
	entry.buildErr = failure
	entry.mu.Unlock()
	registry.finishStoppedEntry(entry)
}

func (entry *runtimeEntry) finishBuild(owner ownerRuntime, failure error) {
	entry.mu.Lock()
	entry.owner = owner
	entry.buildErr = failure
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

func (registry *Registry) finishStoppedEntry(entry *runtimeEntry) {
	select {
	case <-entry.runDone:
	default:
		return
	}
	entry.slot.mu.Lock()
	if entry.slot.current != nil {
		entry.slot.mu.Unlock()
		return
	}
	active, handles := entry.slot.active, entry.slot.handles
	entry.slot.mu.Unlock()
	if active == 0 && handles == 0 {
		registry.removeSlot(entry.slot)
	}
}

func (registry *Registry) cleanupFailedEntry(entry *runtimeEntry) {
	entry.slot.mu.Lock()
	handles := entry.slot.handles
	current := entry.slot.current
	entry.slot.mu.Unlock()
	if handles == 0 && current == nil {
		registry.removeSlot(entry.slot)
	}
}
