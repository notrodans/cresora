package operatoraccountauth

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// challengeRecord is the per-challenge mutable state owned by the process-local
// registry. The registry mutex guards membership and slot maps; this record
// mutex serializes provider RPC and durable transitions for one challenge.
type challengeRecord struct {
	mu sync.Mutex

	requestID        uuid.UUID
	target           AuthTarget
	phone            string
	delivery         string
	stage            Stage
	expires          atomic.Value
	hash             PhoneCodeHash
	ready            bool
	codeAttempts     int
	passwordAttempts int
	pendingProfile   *Profile
	operationCancel  atomic.Value
}

// terminalResult is the bounded completion record kept after a challenge ends
// so a duplicate client retry can observe the terminal outcome.
type terminalResult struct {
	actorID       uuid.UUID
	expires       time.Time
	accountResult *Account
	aborted       bool
}

// challengeReservation is the atomic outcome of Prepare. It either resumes an
// existing challenge (Existing reports true) or holds a newly created one that
// the caller must Commit or Rollback.
type challengeReservation struct {
	registry  *challengeRegistry
	record    *challengeRecord
	challenge Challenge
	created   bool
}

func (reservation challengeReservation) Existing() bool {
	return !reservation.created
}

func (reservation challengeReservation) Challenge() Challenge {
	return reservation.challenge
}

// Commit applies the provider send-code result to the reserved challenge.
func (reservation challengeReservation) Commit(
	ctx context.Context,
	result SendCodeResult,
) (Challenge, error) {
	return reservation.registry.commit(ctx, reservation, result)
}

// Rollback removes a newly created reservation that will not be attached. It
// is a no-op for a resumed existing challenge.
func (reservation challengeReservation) Rollback(ctx context.Context) error {
	if !reservation.created {
		return nil
	}
	return reservation.registry.rollback(ctx, reservation)
}

func (record *challengeRecord) expiry() time.Time {
	return record.expires.Load().(time.Time)
}

func (record *challengeRecord) cancelBusy() {
	record.operationCancel.Load().(context.CancelFunc)()
}
