package accountowner

import (
	"context"
	"errors"

	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// RevokeAndStop fences the disconnecting lifecycle version, drains ordinary
// callbacks, runs exactly one privileged callback, and then tears down the
// account owner. The privileged callback is deliberately outside the ordinary
// admission path: it is the final operation allowed after the N+1 fence.
//
// A callback panic is recovered only long enough to stop and join the owner;
// the original panic value is then re-raised. This keeps owner teardown
// unconditional without converting programmer errors into runtime failures.
func (registry *Registry) RevokeAndStop(
	ctx context.Context,
	disconnecting operatoraccounts.RuntimeTarget,
	callback ClientCallback,
) (failure error) {
	if failure := validateRevokeTarget(disconnecting); failure != nil {
		return failure
	}

	slot, rentry, cancel, failure := registry.prepareRevoke(ctx, disconnecting)
	if failure != nil {
		return failure
	}
	if cancel != nil {
		cancel()
	}

	callbackFailure, panicValue, panicked := registry.executeRevoke(ctx, slot, rentry, callback)
	teardownFailure := registry.teardownRevoke(slot, rentry, disconnecting.Version, callbackFailure)
	registry.releaseRevoke(slot)

	if panicked {
		panic(panicValue)
	}
	if teardownFailure != nil {
		return teardownFailure
	}
	return callbackFailure
}
func (registry *Registry) prepareRevoke(
	ctx context.Context,
	disconnecting operatoraccounts.RuntimeTarget,
) (*accountSlot, *runtimeEntry, context.CancelFunc, error) {
	key := accountKeyFromTarget(disconnecting)
	slot, failure := registry.acquireRevoke(ctx, key)
	if failure != nil {
		return nil, nil, nil, failure
	}

	for {
		var (
			rentry       *runtimeEntry
			cancel       context.CancelFunc
			build        bool
			prepareErr   error
			stalePrivate *runtimeEntry
		)
		registry.mu.Lock()
		if registry.stopped {
			prepareErr = ErrRegistryStopped
		} else {
			rentry = slot.currentEntry()
			if rentry != nil && rentry.privateRevoke && rentry.target == disconnecting {
				stalePrivate = rentry
			} else if rentry != nil {
				sameIdentity := rentry.target.Actor == disconnecting.Actor &&
					rentry.target.AccountID == disconnecting.AccountID
				if !sameIdentity || rentry.target.Version != disconnecting.Version-1 {
					prepareErr = ErrStaleAdmission
				} else {
					switch rentry.target.Status {
					case operatoraccount.StatusActive, operatoraccount.StatusReauthRequired:
					default:
						prepareErr = ErrInvalidAdmission
					}
				}
			}

			if stalePrivate == nil && prepareErr == nil {
				prepareErr = registry.recordFence(key, disconnecting.Version, true)
			}
			if stalePrivate == nil && prepareErr == nil {
				cancel = slot.closeAdmissionLocked()
				if rentry == nil {
					rentry = rentry.newRuntimeEntry(registry, slot, disconnecting)
					rentry.privateRevoke = true
					slot.current = rentry
					build = true
				}
			}
		}
		registry.mu.Unlock()

		if stalePrivate != nil {
			cleanupFailure := registry.teardownRevoke(slot, stalePrivate, disconnecting.Version, nil)
			registry.mu.Lock()
			gone := slot.currentEntry() != stalePrivate
			registry.mu.Unlock()
			if !gone {
				if cleanupFailure == nil {
					cleanupFailure = ErrAccountStopped
				}
				registry.releaseRevoke(slot)
				return nil, nil, nil, cleanupFailure
			}
			continue
		}

		if prepareErr != nil {
			registry.releaseRevoke(slot)
			return nil, nil, nil, prepareErr
		}
		if build {
			go registry.buildEntry(rentry)
		}
		return slot, rentry, cancel, nil
	}
}

func (registry *Registry) waitForRevoke(slot *accountSlot, ctx context.Context) error {
	for {
		registry.mu.Lock()
		busy := slot.revokeRunning || slot.revokeWaiters > 0
		changed := slot.revokeChanged
		registry.mu.Unlock()
		if !busy {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// acquireRevoke claims the account's single teardown gate before publishing a
// revoke as running. Every teardown uses the same gate, so an owner cannot be
// stopped between revoke admission and the privileged callback. Waiters are
// counted before blocking and use the gate itself rather than a one-shot
// completion channel, which keeps queued retries from being missed.
func (registry *Registry) acquireRevoke(ctx context.Context, key accountKey) (*accountSlot, error) {
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
			return registry.acquireRevoke(ctx, key)
		}
		if len(registry.slots) >= registry.config.Capacity {
			registry.mu.Unlock()
			return nil, ErrRuntimeCapacity
		}
		slot = newAccountSlot()
		registry.slots[key] = slot
	}
	slot.revokeWaiters++
	slot.signalRevokeLocked()
	registry.mu.Unlock()

	select {
	case <-slot.teardownGate:
	case <-ctx.Done():
		registry.mu.Lock()
		slot.revokeWaiters--
		slot.signalRevokeLocked()
		registry.mu.Unlock()
		return nil, ctx.Err()
	}

	registry.mu.Lock()
	slot.revokeWaiters--
	if registry.stopped {
		slot.signalRevokeLocked()
		registry.mu.Unlock()
		slot.teardownGate <- struct{}{}
		return nil, ErrRegistryStopped
	}
	slot.revokeRunning = true
	slot.signalRevokeLocked()
	registry.mu.Unlock()
	return slot, nil
}

func (registry *Registry) executeRevoke(
	ctx context.Context,
	slot *accountSlot,
	entry *runtimeEntry,
	callback ClientCallback,
) (failure error, panicValue any, panicked bool) {
	defer func() {
		if value := recover(); value != nil {
			panicked = true
			panicValue = value
			failure = errRevokeCallbackPanicked
		}
	}()
	drainContext, cancel := registry.boundedContext(ctx)
	defer cancel()
	if failure := entry.waitBuilt(drainContext); failure != nil {
		return failure, nil, false
	}
	if failure := slot.waitDrained(drainContext); failure != nil {
		return failure, nil, false
	}
	owner := entry.Owner()
	if owner == nil {
		return ErrAccountStopped, nil, false
	}
	if failure := owner.WaitReady(drainContext); failure != nil {
		return failure, nil, false
	}
	if failure := ctx.Err(); failure != nil {
		return failure, nil, false
	}

	failure = owner.Execute(ctx, func(callbackContext context.Context, client *gotdtelegram.Client) (callbackFailure error) {
		defer func() {
			if value := recover(); value != nil {
				panicked = true
				panicValue = value
				callbackFailure = errRevokeCallbackPanicked
			}
		}()
		return callback(callbackContext, client)
	})
	return failure, panicValue, panicked
}

func (registry *Registry) teardownRevoke(
	slot *accountSlot,
	entry *runtimeEntry,
	fencedVersion operatoraccount.Version,
	callbackFailure error,
) error {
	cleanupContext, cancel := registry.boundedContext(context.Background())
	defer cancel()

	var first error
	built := true
	if failure := entry.waitBuilt(cleanupContext); failure != nil {
		built = !isContextFailure(failure)
		if !errors.Is(failure, ErrAccountStopped) {
			first = failure
		}
	}
	if failure := slot.waitDrained(cleanupContext); failure != nil && first == nil {
		first = failure
	}

	owner := entry.Owner()
	joined := owner == nil
	if owner != nil {
		owner.Stop()
		failure := owner.Wait(cleanupContext)
		if callbackFailure != nil && errors.Is(failure, callbackFailure) {
			failure = nil
		}
		joined = failure == nil || errors.Is(failure, ErrStopped) || !isContextFailure(failure)
		if failure != nil && !errors.Is(failure, ErrStopped) && first == nil {
			first = failure
		}
	}
	if built && joined {
		registry.finishTeardown(slot, entry)
		registry.unprotectFenceLocked(accountKeyFromTarget(entry.target), fencedVersion)
	}
	return first
}

func (registry *Registry) releaseRevoke(slot *accountSlot) {
	registry.mu.Lock()
	if slot.revokeRunning {
		slot.revokeRunning = false
		slot.signalRevokeLocked()
	}
	slot.mu.Lock()
	if slot.current == nil && slot.active == 0 {
		slot.closed = false
		slot.stopping = false
	}
	remove := slot.revokeWaiters == 0
	slot.mu.Unlock()
	if remove {
		remove = slot.removable()
	}
	if remove {
		for key, candidate := range registry.slots {
			if candidate == slot {
				delete(registry.slots, key)
				break
			}
		}
	}
	registry.mu.Unlock()
	slot.teardownGate <- struct{}{}
}
