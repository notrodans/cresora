package operatoraccountauth

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

var (
	errRuntimeChallengeExpired       = errors.New("authentication challenge expired")
	errRuntimeChallengeNotFound      = errors.New("authentication challenge not found")
	errRuntimeChallengeAlreadyActive = errors.New("authentication challenge already active")
	errRuntimeAccountStateConflict   = errors.New("authentication account state conflict")
	errRuntimeCapacity               = errors.New("authentication challenge capacity reached")
	errRuntimeClosed                 = errors.New("authentication challenge coordinator closed")
)

// challengeRegistry is the application service's process-local phone
// challenge owner. It is kept in this package because the shared challenge
// projection package is intentionally transport-neutral and cannot depend
// back on this service without creating an import cycle.
type challengeRegistry struct {
	mu          sync.Mutex
	clock       func() time.Time
	newRequest  func() uuid.UUID
	ttl         time.Duration
	capacity    int
	challenges  map[uuid.UUID]*challengeRecord
	actorSlots  map[uuid.UUID]uuid.UUID
	targetSlots map[AuthTarget]uuid.UUID
	tombstones  map[uuid.UUID]terminalResult
	closed      bool
}

func newChallengeRegistry(clock func() time.Time, ttl time.Duration) *challengeRegistry {
	return &challengeRegistry{
		clock:       clock,
		newRequest:  uuid.New,
		ttl:         ttl,
		capacity:    maxLiveChallenges,
		challenges:  make(map[uuid.UUID]*challengeRecord),
		actorSlots:  make(map[uuid.UUID]uuid.UUID),
		targetSlots: make(map[AuthTarget]uuid.UUID),
		tombstones:  make(map[uuid.UUID]terminalResult),
	}
}

func (registry *challengeRegistry) closeAdmission() {
	registry.mu.Lock()
	registry.closed = true
	registry.mu.Unlock()
}

func (registry *challengeRegistry) isClosed() bool {
	registry.mu.Lock()
	closed := registry.closed
	registry.mu.Unlock()
	return closed
}

func (registry *challengeRegistry) snapshot(ctx context.Context) ([]Challenge, error) {
	registry.mu.Lock()
	records := make([]*challengeRecord, 0, len(registry.challenges))
	for _, record := range registry.challenges {
		records = append(records, record)
	}
	registry.mu.Unlock()

	result := make([]Challenge, 0, len(records))
	for _, record := range records {
		for !record.mu.TryLock() {
			record.cancelBusy()
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			default:
				runtime.Gosched()
			}
		}
		if registry.current(record.target.Actor, record.requestID, record) {
			result = append(result, registry.projection(record))
		}
		record.mu.Unlock()
	}
	return result, nil
}

func (registry *challengeRegistry) clearTombstones() {
	registry.mu.Lock()
	registry.tombstones = make(map[uuid.UUID]terminalResult)
	registry.mu.Unlock()
}

// Prepare atomically resolves the actor/target admission slot. It cleans
// expired terminal results, resumes an existing challenge with its expiry
// bounded by the durable auth expiry, rejects an already-active actor slot or
// full capacity, and otherwise creates and registers a new challenge record.
func (registry *challengeRegistry) Prepare(
	ctx context.Context,
	target AuthTarget,
	phone string,
	expiresAt time.Time,
) (challengeReservation, error) {
	if err := runtimeContextError(ctx); err != nil {
		return challengeReservation{}, err
	}
	if err := validateTarget(target); err != nil || target.Status != operatoraccount.StatusAuthenticating || strings.TrimSpace(phone) == "" || expiresAt.IsZero() {
		return challengeReservation{}, ErrInvalidInput
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return challengeReservation{}, errRuntimeClosed
	}
	registry.cleanupLocked()

	if requestID, found := registry.targetSlots[target]; found {
		if record := registry.challenges[requestID]; record != nil {
			if expiresAt.Before(record.expiry()) {
				record.expires.Store(expiresAt)
			}
			challenge := registry.projection(record)
			reservation := challengeReservation{registry: registry, record: record, challenge: challenge}
			if !registry.clock().Before(challenge.ExpiresAt) {
				return reservation, errRuntimeChallengeExpired
			}
			return reservation, nil
		}
	}
	if requestID, occupied := registry.actorSlots[target.Actor.OperatorID]; occupied {
		if record := registry.challenges[requestID]; record != nil {
			challenge := registry.projection(record)
			reservation := challengeReservation{registry: registry, record: record, challenge: challenge}
			if !registry.clock().Before(challenge.ExpiresAt) {
				return reservation, errRuntimeChallengeExpired
			}
			return challengeReservation{}, errRuntimeChallengeAlreadyActive
		}
	}
	if len(registry.challenges)+len(registry.tombstones) >= registry.capacity {
		return challengeReservation{}, errRuntimeCapacity
	}
	requestID := registry.nextRequestIDLocked()
	record := &challengeRecord{
		requestID: requestID,
		target:    target,
		phone:     phone,
		stage:     StageCode,
	}
	record.expires.Store(expiresAt)
	record.operationCancel.Store(context.CancelFunc(func() {}))
	registry.challenges[requestID] = record
	registry.actorSlots[target.Actor.OperatorID] = requestID
	registry.targetSlots[target] = requestID
	return challengeReservation{
		registry:  registry,
		record:    record,
		challenge: registry.projection(record),
		created:   true,
	}, nil
}

func (registry *challengeRegistry) commit(
	ctx context.Context,
	reservation challengeReservation,
	sent SendCodeResult,
) (Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Challenge{}, err
	}
	record := reservation.record
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return Challenge{}, errRuntimeClosed
	}
	if record == nil {
		registry.mu.Unlock()
		return Challenge{}, errRuntimeChallengeNotFound
	}
	registry.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if !registry.current(record.target.Actor, record.requestID, record) {
		return Challenge{}, errRuntimeChallengeNotFound
	}
	if record.ready {
		return registry.projection(record), nil
	}
	if sent.PhoneCodeHash.IsZero() {
		return Challenge{}, ErrProviderUnavailable
	}
	if !registry.clock().Before(record.expiry()) {
		return registry.projection(record), errRuntimeChallengeExpired
	}
	if !sent.ExpiresAt.IsZero() && sent.ExpiresAt.Before(record.expiry()) {
		record.expires.Store(sent.ExpiresAt)
	}
	if !registry.clock().Before(record.expiry()) {
		return registry.projection(record), errRuntimeChallengeExpired
	}
	record.hash = sent.PhoneCodeHash
	record.delivery = strings.TrimSpace(sent.Delivery)
	if record.delivery == "" {
		record.delivery = "Telegram code"
	}
	record.ready = true
	return registry.projection(record), nil
}

func (registry *challengeRegistry) rollback(ctx context.Context, reservation challengeReservation) error {
	if err := runtimeContextError(ctx); err != nil {
		return err
	}
	record := reservation.record
	registry.mu.Lock()
	if record == nil || registry.challenges[record.requestID] != record {
		registry.mu.Unlock()
		return errRuntimeChallengeNotFound
	}
	registry.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	registry.remove(record)
	return nil
}

func (registry *challengeRegistry) Status(ctx context.Context, actor applicationroot.Actor) (*Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}
	if actor.OperatorID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil, errRuntimeClosed
	}
	registry.cleanupLocked()
	requestID, ok := registry.actorSlots[actor.OperatorID]
	record := registry.challenges[requestID]
	registry.mu.Unlock()
	if !ok || record == nil {
		return nil, nil
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if !registry.current(actor, requestID, record) {
		return nil, nil
	}
	if !registry.clock().Before(record.expiry()) {
		projection := registry.projection(record)
		return &projection, errRuntimeChallengeExpired
	}
	projection := registry.projection(record)
	return &projection, nil
}

func (registry *challengeRegistry) Operation(
	ctx context.Context,
	actor applicationroot.Actor,
	requestID uuid.UUID,
	action func(*challengeOperation) error,
) (Result, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Result{}, err
	}
	if action == nil {
		return Result{}, ErrInvalidInput
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return Result{}, errRuntimeClosed
	}
	record := registry.challenges[requestID]
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		stone, hasStone := registry.tombstones[requestID]
		registry.mu.Unlock()
		if hasStone && stone.actorID == actor.OperatorID && registry.clock().Before(stone.expires) {
			if stone.accountResult != nil {
				account := *stone.accountResult
				return Result{Account: &account}, nil
			}
			if stone.aborted {
				return Result{}, ErrAuthenticationAborted
			}
		}
		return Result{}, errRuntimeChallengeNotFound
	}
	registry.mu.Unlock()

	record.mu.Lock()
	defer record.mu.Unlock()
	operationContext, operationCancel := context.WithCancel(ctx)
	record.operationCancel.Store(operationCancel)
	defer func() {
		record.operationCancel.Store(context.CancelFunc(func() {}))
		operationCancel()
	}()
	if !registry.current(actor, requestID, record) {
		return Result{}, errRuntimeChallengeNotFound
	}
	if registry.isClosed() {
		return Result{}, errRuntimeClosed
	}
	if !registry.clock().Before(record.expiry()) {
		challenge := registry.projection(record)
		return Result{Challenge: &challenge}, errRuntimeChallengeExpired
	}
	if !record.ready && record.pendingProfile == nil {
		challenge := registry.projection(record)
		return Result{Challenge: &challenge}, ErrProviderUnavailable
	}
	operation := &challengeOperation{registry: registry, record: record, ctx: operationContext}
	failure := action(operation)
	if operation.completed {
		account := operation.account
		return Result{Account: &account}, failure
	}
	if operation.aborted {
		return Result{}, failure
	}
	challenge := registry.projection(record)
	return Result{Challenge: &challenge}, failure
}

func (registry *challengeRegistry) Remove(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	if err := runtimeContextError(ctx); err != nil {
		return err
	}
	registry.mu.Lock()
	record := registry.challenges[requestID]
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		registry.mu.Unlock()
		return errRuntimeChallengeNotFound
	}
	registry.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if !registry.current(actor, requestID, record) {
		return errRuntimeChallengeNotFound
	}
	registry.remove(record)
	record.hash = PhoneCodeHash{}
	record.ready = false
	record.pendingProfile = nil
	return nil
}

func (registry *challengeRegistry) clearShutdownState(challenge Challenge) {
	// A failed abort remains addressable by target and request ID so a later
	// Shutdown can retry the durable lifecycle without retaining auth state.
	registry.mu.Lock()
	record := registry.challenges[challenge.RequestID]
	if record == nil || record.target != challenge.AuthTarget {
		registry.mu.Unlock()
		return
	}
	registry.mu.Unlock()

	record.mu.Lock()
	defer record.mu.Unlock()
	if !registry.current(challenge.Actor, challenge.RequestID, record) {
		return
	}
	record.phone = ""
	record.delivery = ""
	record.stage = ""
	record.expires.Store(time.Time{})
	record.hash = PhoneCodeHash{}
	record.ready = false
	record.codeAttempts = 0
	record.passwordAttempts = 0
	record.pendingProfile = nil
	record.operationCancel.Store(context.CancelFunc(func() {}))
}

func (registry *challengeRegistry) current(actor applicationroot.Actor, requestID uuid.UUID, record *challengeRecord) bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.challenges[requestID] == record && record.target.Actor.OperatorID == actor.OperatorID
}

func (registry *challengeRegistry) remove(record *challengeRecord) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.removeLocked(record)
}

func (registry *challengeRegistry) removeWithTombstone(record *challengeRecord, account *Account, aborted bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if !registry.removeLocked(record) {
		return
	}
	stone := terminalResult{
		actorID: record.target.Actor.OperatorID,
		expires: registry.clock().Add(registry.ttl),
		aborted: aborted,
	}
	if account != nil {
		copy := *account
		stone.accountResult = &copy
	}
	registry.tombstones[record.requestID] = stone
	// The caller owns record.mu. Clear the transient provider secret as soon
	// as the terminal state is recorded rather than retaining it until GC.
	record.hash = PhoneCodeHash{}
	record.ready = false
	record.pendingProfile = nil
}

func (registry *challengeRegistry) removeLocked(record *challengeRecord) bool {
	if registry.challenges[record.requestID] != record {
		return false
	}
	delete(registry.challenges, record.requestID)
	if registry.actorSlots[record.target.Actor.OperatorID] == record.requestID {
		delete(registry.actorSlots, record.target.Actor.OperatorID)
	}
	if registry.targetSlots[record.target] == record.requestID {
		delete(registry.targetSlots, record.target)
	}
	return true
}

func (registry *challengeRegistry) cleanupLocked() {
	for requestID, stone := range registry.tombstones {
		if !registry.clock().Before(stone.expires) {
			delete(registry.tombstones, requestID)
		}
	}
}

func (registry *challengeRegistry) nextRequestIDLocked() uuid.UUID {
	for {
		requestID := registry.newRequest()
		if requestID != uuid.Nil {
			if _, exists := registry.challenges[requestID]; !exists {
				if _, exists := registry.tombstones[requestID]; !exists {
					return requestID
				}
			}
		}
	}
}

func (registry *challengeRegistry) projection(record *challengeRecord) Challenge {
	return Challenge{
		RequestID:  record.requestID,
		AuthTarget: record.target,
		Phone:      record.phone,
		Delivery:   record.delivery,
		Stage:      record.stage,
		ExpiresAt:  record.expiry(),
	}
}

func mapChallengeError(err error) error {
	switch {
	case errors.Is(err, errRuntimeChallengeExpired):
		return ErrChallengeExpired
	case errors.Is(err, errRuntimeChallengeNotFound):
		return ErrChallengeNotFound
	case errors.Is(err, errRuntimeChallengeAlreadyActive):
		return ErrAccountStateConflict
	case errors.Is(err, errRuntimeAccountStateConflict):
		return ErrAccountStateConflict
	case errors.Is(err, errRuntimeCapacity):
		return ErrChallengeCapacity
	case errors.Is(err, errRuntimeClosed):
		return ErrServiceClosed
	default:
		return err
	}
}

func runtimeContextError(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	return ctx.Err()
}
