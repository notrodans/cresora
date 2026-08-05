package accountowner

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// accountKey deliberately excludes lifecycle version. All versions of one
// operator/account must pass through one serialized gate.
type accountKey struct {
	operatorID uuid.UUID
	accountID  uuid.UUID
}

// accountSlot serializes the operations of a single operator account.
type accountSlot struct {
	mu sync.Mutex

	// mu guards current, closed, stopping, handles, active, activeDone,
	// activeCancel and lastUsed. gate serializes account operations, but it is
	// not the admission boundary: beginOperation is.
	gate     chan struct{}
	current  *runtimeEntry
	closed   bool
	stopping bool

	handles      int // Count of open handles
	active       int
	activeDone   chan struct{}
	activeCancel context.CancelFunc
	lastUsed     time.Time

	// Registry.mu guards revokeRunning, revokeWaiters and revokeChanged. They
	// form a bounded per-account rendezvous for privileged revoke operations.
	// The slot is retained while either count is non-zero, so a waiting
	// same-intent revoke cannot race a replacement owner.
	revokeRunning bool
	revokeWaiters int
	revokeChanged chan struct{}
	teardownGate  chan struct{}
}

func newAccountSlot() *accountSlot {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	teardownGate := make(chan struct{}, 1)
	teardownGate <- struct{}{}
	return &accountSlot{
		gate:          gate,
		activeDone:    closedSignal(),
		lastUsed:      time.Now(),
		revokeChanged: make(chan struct{}),
		teardownGate:  teardownGate,
	}
}

func (slot *accountSlot) Living() bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.current != nil && !slot.closed && !slot.stopping
}

func (slot *accountSlot) Idling(idleTimeout time.Duration, now time.Time) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	idle := slot.current != nil && !slot.closed && !slot.stopping && slot.handles == 0 && slot.active == 0 && now.Sub(slot.lastUsed) >= idleTimeout
	return idle
}

func closedSignal() chan struct{} {
	closed := make(chan struct{})
	close(closed)
	return closed
}

func (slot *accountSlot) reserveHandle(rentry *runtimeEntry) bool {
	if slot.closed || slot.stopping {
		return false
	}
	if rentry != nil && slot.current != rentry {
		return false
	}
	slot.handles++
	slot.lastUsed = time.Now()
	return true
}

func (slot *accountSlot) closeAdmission() context.CancelFunc {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.closed = true
	slot.stopping = true
	cancel := slot.activeCancel
	slot.activeCancel = nil
	return cancel
}

// beginOperation is the account-operation linearization point. Under slot.mu,
// either the operation becomes active before admission closes, or it observes
// the closed state and is rejected. An admitted operation may subsequently be
// canceled and drained by StopAccount.
func (slot *accountSlot) beginOperation(
	rentry *runtimeEntry,
	target operatoraccounts.RuntimeTarget,
	cancel context.CancelFunc,
) error {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if failure := admissionError(slot, rentry, target); failure != nil {
		return failure
	}
	if slot.active == 0 {
		slot.activeDone = make(chan struct{})
	}
	slot.active++
	slot.activeCancel = cancel
	slot.lastUsed = time.Now()
	return nil
}

func (slot *accountSlot) finishOperation() {
	slot.mu.Lock()
	slot.active--
	slot.activeCancel = nil
	if slot.active == 0 {
		close(slot.activeDone)
	}
	slot.lastUsed = time.Now()
	slot.mu.Unlock()
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

func (slot *accountSlot) signalRevoke() {
	previous := slot.revokeChanged
	slot.revokeChanged = make(chan struct{})
	close(previous)
}

func (slot *accountSlot) currentREntry() *runtimeEntry {
	slot.mu.Lock()
	defer slot.mu.Unlock()

	return slot.current
}

func (slot *accountSlot) removable() bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()

	return slot.current == nil &&
		slot.handles == 0 &&
		slot.active == 0 &&
		!slot.stopping
}
