package accountowner

import (
	"context"

	"github.com/google/uuid"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func (registry *Registry) reserve(
	ctx context.Context,
	target operatoraccounts.RuntimeTarget,
) (*runtimeEntry, error) {
	key := accountKeyFromTarget(target)
	for {
		registry.mu.Lock()
		if registry.stopped {
			registry.mu.Unlock()
			return nil, ErrRegistryStopped
		}
		if fence, exists := registry.fences.records[key]; exists && target.Version <= fence.version {
			registry.mu.Unlock()
			return nil, ErrAccountStopped
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
			if len(registry.slots) >= registry.config.Capacity {
				registry.mu.Unlock()
				return nil, ErrRuntimeCapacity
			}
			slot = newAccountSlot()
			registry.slots[key] = slot
		}

		currREntry := slot.currentEntry()
		slot.mu.Lock()
		stopping := slot.stopping
		closed := slot.closed
		slot.mu.Unlock()
		if slot.revokeRunning || slot.revokeWaiters > 0 {
			registry.mu.Unlock()
			return nil, ErrAccountStopped
		}

		if currREntry != nil {
			switch {
			case currREntry.target == target && !stopping:
				slot.mu.Lock()
				reserved := slot.reserveRefLocked(currREntry)
				slot.mu.Unlock()
				registry.mu.Unlock()
				if !reserved {
					return nil, ErrAccountStopped
				}
				return currREntry, nil
			case target.Version < currREntry.target.Version:
				registry.mu.Unlock()
				return nil, ErrStaleAdmission
			case target.Version == currREntry.target.Version:
				registry.mu.Unlock()
				return nil, ErrInvalidAdmission
			default:
				// Keep the old entry installed while it drains. A replacement
				// cannot construct or start a second client for this account.
				if !stopping {
					cancel := slot.closeAdmissionLocked()
					registry.mu.Unlock()
					if cancel != nil {
						cancel()
					}
				} else {
					registry.mu.Unlock()
				}
				if failure := registry.teardown(slot, currREntry, ctx); failure != nil {
					return nil, failure
				}
				continue
			}
		}

		if stopping || closed {
			registry.mu.Unlock()
			return nil, ErrAccountStopped
		}
		entry := currREntry.newRuntimeEntry(registry, slot, target)
		slot.mu.Lock()
		canPublish := !registry.stopped && slot.current == nil && !slot.closed && !slot.stopping
		if canPublish {
			slot.current = entry
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

func (registry *Registry) checkAdmission(
	entry *runtimeEntry,
	target operatoraccounts.RuntimeTarget,
) error {
	entry.slot.mu.Lock()
	defer entry.slot.mu.Unlock()
	return admissionErrorLocked(entry.slot, entry, target)
}

func admissionErrorLocked(
	slot *accountSlot,
	entry *runtimeEntry,
	target operatoraccounts.RuntimeTarget,
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

func validateAdmission(target operatoraccounts.RuntimeTarget) error {
	if failure := validateTarget(target); failure != nil {
		return failure
	}
	switch target.Status {
	case operatoraccount.StatusAuthenticating,
		operatoraccount.StatusActive,
		operatoraccount.StatusReauthRequired:
		return nil
	default:
		return ErrInvalidAdmission
	}
}

func validateStopTarget(target operatoraccounts.RuntimeTarget) error {
	if failure := validateTarget(target); failure != nil {
		return failure
	}
	switch target.Status {
	case operatoraccount.StatusAuthenticating,
		operatoraccount.StatusActive,
		operatoraccount.StatusReauthRequired,
		operatoraccount.StatusDisconnecting:
		return nil
	default:
		return ErrInvalidAdmission
	}
}

func validateRevokeTarget(target operatoraccounts.RuntimeTarget) error {
	if failure := validateTarget(target); failure != nil {
		return failure
	}
	if target.Status != operatoraccount.StatusDisconnecting || target.Version <= operatoraccount.InitialVersion {
		return ErrInvalidAdmission
	}
	return nil
}

func validateTarget(target operatoraccounts.RuntimeTarget) error {
	if target.Actor.OperatorID == uuid.Nil || target.AccountID.IsZero() || target.Version == 0 {
		return ErrInvalidAdmission
	}
	return nil
}

func accountKeyFromTarget(target operatoraccounts.RuntimeTarget) accountKey {
	return accountKey{
		operatorID: target.Actor.OperatorID,
		accountID:  target.AccountID.UUID(),
	}
}
