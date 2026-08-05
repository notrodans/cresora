package accountowner

import (
	"context"
	"errors"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// StopAccount closes admission for the exact lifecycle target, cancels the
// current operation, then waits for bounded drain and owner teardown. The
// registry lock is never held while waiting on a callback or gotd.
func (registry *Registry) StopAccount(
	ctx context.Context,
	target operatoraccounts.RuntimeTarget,
) error {
	if failure := validateStopTarget(target); failure != nil {
		return failure
	}
	key := accountKeyFromTarget(target)

	registry.mu.Lock()
	if registry.stopped {
		registry.mu.Unlock()
		return nil
	}
	slot := registry.slots[key]
	if slot == nil {
		failure := registry.recordFence(key, target.Version, false)
		registry.mu.Unlock()
		return failure
	}
	rentry := slot.currentEntry()
	if rentry != nil {
		if rentry.target.Actor != target.Actor || rentry.target.AccountID != target.AccountID || rentry.target.Version != target.Version {
			slot.mu.Unlock()
			registry.mu.Unlock()
			return ErrStaleAdmission
		}
		if rentry.target.Status != target.Status {
			slot.mu.Unlock()
			registry.mu.Unlock()
			return ErrInvalidAdmission
		}
	}
	if failure := registry.recordFence(key, target.Version, rentry != nil); failure != nil {
		registry.mu.Unlock()
		return failure
	}

	var cancel context.CancelFunc
	if rentry != nil && !slot.stopping {
		cancel = slot.closeAdmission()
	}
	registry.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rentry == nil {
		return nil
	}
	return registry.teardown(ctx, slot, rentry)
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
			if slot.currentEntry() == nil {
				continue
			}
			if cancel := slot.closeAdmission(); cancel != nil {
				cancels = append(cancels, cancel)
			}
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
			if err := registry.teardown(ctx, item.slot, item.entry); err != nil {
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
		current := slot.currentEntry()
		if current != nil {
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
		rentry := slot.currentEntry()
		if rentry != nil {
			entries = append(entries, struct {
				slot  *accountSlot
				entry *runtimeEntry
			}{slot: slot, entry: rentry})
		}
	}
	return entries
}

func (registry *Registry) teardown(ctx context.Context, slot *accountSlot, entry *runtimeEntry) error {
	if failure := registry.waitForRevoke(slot, ctx); failure != nil {
		return failure
	}
	select {
	case <-slot.teardownGate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { slot.teardownGate <- struct{}{} }()

	slot.mu.Lock()
	current := slot.current
	slot.mu.Unlock()
	if current != entry {
		return nil
	}

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
	if owner := entry.Owner(); owner != nil {
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
	slot.mu.Lock()
	if slot.current == entry {
		slot.current, slot.stopping, slot.closed = nil, false, false
	}
	slot.mu.Unlock()
	registry.unprotectFenceLocked(accountKeyFromTarget(entry.target), entry.target.Version)
	registry.removeSlot(slot)
}

func (registry *Registry) removeSlot(slot *accountSlot) {
	registry.mu.Lock()
	for key, candidate := range registry.slots {
		if candidate != slot {
			continue
		}
		candidate.mu.Lock()
		remove := candidate.current == nil && candidate.handles == 0 && candidate.active == 0 &&
			!candidate.stopping && !candidate.revokeRunning && candidate.revokeWaiters == 0
		candidate.mu.Unlock()
		if remove {
			delete(registry.slots, key)
			break
		}
	}
	registry.mu.Unlock()
}

func (registry *Registry) boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if registry.config.DrainTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, registry.config.DrainTimeout)
}
