package operatoraccountauth

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// admissionGate serializes the durable admission + challenge reservation phase
// of Start for one actor at a time. The gate reader lock is held for the whole
// acquire; shutdown acquires the writer lock to stop new admissions.
type admissionGate struct {
	gate    sync.RWMutex
	stripes [actorStartLockCount]sync.Mutex
	closed  bool
}

// admission is one held actor-scoped admission.
type admission struct {
	gate     *admissionGate
	stripe   *sync.Mutex
	released atomic.Bool
}

// Acquire admits one Start for the actor or reports the service is closed.
func (gate *admissionGate) Acquire(actorID uuid.UUID) (*admission, error) {
	gate.gate.RLock()
	stripe := &gate.stripes[actorStartLockIndex(actorID)]
	stripe.Lock()
	if gate.closed {
		stripe.Unlock()
		gate.gate.RUnlock()
		return nil, ErrServiceClosed
	}
	return &admission{
		gate:   gate,
		stripe: stripe,
	}, nil
}

// Release is idempotent and may be called once from the deferred handler and
// once to hand the admission back before the provider round-trip.
func (admission *admission) Release() {
	if admission == nil || admission.released.Swap(true) {
		return
	}
	admission.stripe.Unlock()
	admission.gate.gate.RUnlock()
}

// Close blocks new admissions. It must be called while the shutdown snapshot
// runs so no new challenge can be admitted during it.
func (gate *admissionGate) Close() {
	gate.gate.Lock()
	gate.closed = true
	gate.gate.Unlock()
}

func actorStartLockIndex(actorID uuid.UUID) int {
	const (
		fnvOffset64 = uint64(14695981039346656037)
		fnvPrime64  = uint64(1099511628211)
	)
	hash := fnvOffset64
	for _, value := range actorID {
		hash ^= uint64(value)
		hash *= fnvPrime64
	}
	return int(hash % actorStartLockCount)
}
