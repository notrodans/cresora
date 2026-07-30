// Package challenges coordinates the short-lived, operator-scoped state used
// while a Telegram account is being authenticated.
//
// This package is deliberately an application boundary.  It knows about
// opaque provider handles, but never exposes them in a projection and never
// constructs a gotd client.  The state is process local by design: creating a
// new Coordinator starts with an empty challenge set. Provider cancellation is
// best effort: removals dispatch it to a fixed-size, bounded executor and may
// drop it when that executor is full. A provider which ignores its cleanup
// context cannot be forcibly interrupted, but it cannot block a coordinator
// state transition.
package challenges

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	common "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

const (
	// PhoneChallengeTTL is the fixed lifetime of a phone challenge.
	PhoneChallengeTTL = 10 * time.Minute
	// QRChallengeTTL is intentionally also bounded.  A provider may have a
	// shorter native token lifetime, but it may not extend application state.
	QRChallengeTTL = 10 * time.Minute
	// DefaultCleanupTimeout bounds best-effort provider cleanup. Cleanup must
	// never be allowed to strand a caller on a provider which stops honoring
	// cancellation.
	DefaultCleanupTimeout = 5 * time.Second
	// MaxCodeAttempts is the atomic per-challenge phone verification limit.
	MaxCodeAttempts = 5
	// MaxChallenges bounds all live and cancelled tombstone entries held by a
	// coordinator.  It is a process-local capacity, not a database quota.
	MaxChallenges = 4096
)

var (
	// ErrChallengeUnavailable is intentionally the same error for a foreign,
	// random, cancelled, or expired request ID.  Callers must not be able to
	// use the coordinator as an ownership or existence oracle.
	ErrChallengeUnavailable = errors.New("authentication challenge unavailable")
	// ErrChallengeAlreadyActive reports a second start of the same kind for an
	// operator.  The existing challenge is never replaced by that operation.
	ErrChallengeAlreadyActive = errors.New("authentication challenge already active")
	// ErrChallengeCapacity reports that the bounded process-local store is full.
	ErrChallengeCapacity = errors.New("authentication challenge capacity reached")
	// ErrAttemptsExceeded reports a phone challenge which has used all five
	// verification attempts.
	ErrAttemptsExceeded = errors.New("authentication challenge attempts exceeded")
	// ErrCodeRejected is the safe application error for a provider rejection.
	// Provider errors are not returned because they may contain Telegram data.
	ErrCodeRejected = errors.New("authentication code rejected")
	// ErrProviderUnavailable is returned when no inert-safe provider has been
	// explicitly composed, or when a provider cannot start an operation.
	ErrProviderUnavailable = errors.New("telegram authentication provider unavailable")
	// ErrCoordinatorClosed is returned after the coordinator has been stopped.
	// It is deliberately distinct from provider failure: a stopped coordinator
	// cannot be made usable again and must not accept a new operation.
	ErrCoordinatorClosed = errors.New("authentication challenge coordinator closed")
	// ErrInvalidInput is reserved for malformed command input, not request-ID
	// ownership failures.
	ErrInvalidInput = errors.New("invalid authentication challenge input")
)

// Kind identifies one independent challenge slot for an operator.
type Kind string

const (
	KindPhone Kind = "phone"
	KindQR    Kind = "qr"
)

// State is the safe state exposed by a status query. Refreshing is represented
// as active; an exhausted phone challenge remains reserved until its final
// provider calls return, then is removed.
type State string

const (
	StateStarting  State = "starting"
	StateActive    State = "active"
	StateExhausted State = "exhausted"
)

// ProviderHandle is intentionally opaque to callers of the coordinator.  It
// is held only in coordinator state and is never included in a projection.
// Provider implementations may create one from their server-side token at
// the transport boundary; HTTP code must not serialize it.
type ProviderHandle struct{ value string }

// NewProviderHandle creates an opaque handle for an injected provider.  The
// returned value has no exported token accessor by design.
func NewProviderHandle(value string) ProviderHandle {
	return ProviderHandle{value: value}
}

func (handle ProviderHandle) empty() bool { return strings.TrimSpace(handle.value) == "" }

// String prevents accidental provider-token disclosure through ordinary
// logging of a provider result. The coordinator never needs to stringify a
// handle for a browser projection.
func (handle ProviderHandle) String() string {
	if handle.empty() {
		return "<empty opaque provider handle>"
	}
	return "<opaque provider handle>"
}

// PhoneStart is the only input a phone provider receives from this boundary.
// It contains no operator or account identity; the coordinator owns that
// scope before invoking the provider.
type PhoneStart struct {
	Phone string
}

// PhoneStarted is a provider result. Delivery is safe display metadata;
// Handle remains server-only.
type PhoneStarted struct {
	Handle   ProviderHandle
	Delivery string
}

// PhoneVerified is an optional safe account projection returned by a fake or a
// future provider. The coordinator persists nothing.
type PhoneVerified struct {
	Account common.Account
}

// PhoneProvider is the narrow, inert-friendly provider port for phone auth.
// Implementations must honor context cancellation. Cancel is best effort and
// is dispatched outside the coordinator mutex to a bounded cleanup executor.
type PhoneProvider interface {
	StartPhone(context.Context, PhoneStart) (PhoneStarted, error)
	VerifyPhone(context.Context, ProviderHandle, string) (PhoneVerified, error)
	CancelPhone(context.Context, ProviderHandle) error
}

// QRStart is the only input a QR provider receives from this boundary.
type QRStart struct{}

// QRStarted contains the safe-to-render QR URL and its server-only handle.
type QRStarted struct {
	Handle ProviderHandle
	URL    string
}

// QRProvider is the narrow provider port for QR auth. Refresh preserves the
// opaque request ID while updating the provider URL. A provider may return
// the same opaque handle; the coordinator treats that handle as retained and
// never cancels it as if it were a replacement. QR cancellation is best effort
// and is dispatched outside the coordinator mutex.
type QRProvider interface {
	StartQR(context.Context, QRStart) (QRStarted, error)
	RefreshQR(context.Context, ProviderHandle) (QRStarted, error)
	CancelQR(context.Context, ProviderHandle) error
}

// PhoneProjection is the browser-safe phone challenge projection. It has no
// provider handle and no actor/account identity.
type PhoneProjection struct {
	RequestID         uuid.UUID
	Phone             string
	Delivery          string
	ExpiresAt         time.Time
	State             State
	AttemptsUsed      int
	AttemptsRemaining int
}

// QRProjection is the browser-safe QR challenge projection. URL is the
// provider's intended QR presentation value; the provider handle is never
// included.
type QRProjection struct {
	RequestID uuid.UUID
	URL       string
	ExpiresAt time.Time
	State     State
}

// StatusProjection is a race-free actor-scoped snapshot.
type StatusProjection struct {
	Phone *PhoneProjection
	QR    *QRProjection
}

// Submission is the safe result of a phone-code submission. Account is only a
// provider projection; no account row or session is written by this package.
type Submission struct {
	Completed         bool
	AttemptsUsed      int
	AttemptsRemaining int
	Account           common.Account
}

// Config controls deterministic seams and bounds. Zero values select the
// fixed production policy. Providers may be nil, in which case operations
// fail safely with ErrProviderUnavailable and never panic.
type Config struct {
	PhoneProvider PhoneProvider
	QRProvider    QRProvider
	Clock         func() time.Time
	NewRequestID  func() uuid.UUID
	Capacity      int
	// PhoneTTL and QRTTL bound both the live challenge and the matching
	// kind-specific cancellation/terminal tombstone policy.
	PhoneTTL    time.Duration
	QRTTL       time.Duration
	MaxAttempts int
	// CleanupTimeout bounds each best-effort provider cancellation. It is
	// intentionally independent from the request context being cleaned up.
	CleanupTimeout time.Duration
	// CleanupWorkers and CleanupQueueSize configure the bounded, nonblocking
	// provider-cleanup executor. A full queue drops best-effort cleanup rather
	// than delaying a state transition. Zero values select production defaults.
	CleanupWorkers   int
	CleanupQueueSize int
}

// Option customizes a coordinator without making tests depend on wall clock
// time or random request IDs.
type Option func(*Config)

func WithClock(clock func() time.Time) Option {
	return func(config *Config) { config.Clock = clock }
}

func WithRequestIDGenerator(generator func() uuid.UUID) Option {
	return func(config *Config) { config.NewRequestID = generator }
}

func WithCapacity(capacity int) Option {
	return func(config *Config) { config.Capacity = capacity }
}

func WithPhoneTTL(ttl time.Duration) Option {
	return func(config *Config) { config.PhoneTTL = ttl }
}

func WithQRTTL(ttl time.Duration) Option {
	return func(config *Config) { config.QRTTL = ttl }
}

func WithMaxAttempts(attempts int) Option {
	return func(config *Config) { config.MaxAttempts = attempts }
}

func WithCleanupTimeout(timeout time.Duration) Option {
	return func(config *Config) { config.CleanupTimeout = timeout }
}

func WithCleanupWorkers(workers int) Option {
	return func(config *Config) { config.CleanupWorkers = workers }
}

func WithCleanupQueueSize(size int) Option {
	return func(config *Config) { config.CleanupQueueSize = size }
}

// Coordinator is a thread-safe process-local challenge state machine.
type Coordinator struct {
	mu sync.Mutex
	// closed is guarded by mu. Once set, it is never cleared; every command and
	// query re-checks it while linearizing against state changes.
	closed bool

	// closeOnce serializes lifecycle shutdown without using the coordinator
	// mutex while cleanup work is being offered to the executor.
	closeOnce       sync.Once
	cleanupCond     *sync.Cond
	cleanupPending  int
	cleanupStopping bool

	clock           func() time.Time
	newRequestID    func() uuid.UUID
	capacity        int
	phoneTTL        time.Duration
	qrTTL           time.Duration
	maxAttempts     int
	cleanupTimeout  time.Duration
	phone           PhoneProvider
	qr              QRProvider
	cleanupExecutor *cleanupExecutor

	challenges       map[uuid.UUID]*challenge
	phoneSlots       map[uuid.UUID]uuid.UUID
	qrSlots          map[uuid.UUID]uuid.UUID
	tombstones       map[uuid.UUID]tombstone
	version          uint64
	operationVersion uint64
}

type challenge struct {
	requestID uuid.UUID
	actorID   uuid.UUID
	kind      Kind
	state     State

	phone    string
	delivery string
	url      string
	expires  time.Time
	handle   ProviderHandle

	attempts int
	inflight int

	// generation changes whenever the challenge is removed. A provider result
	// from an older generation can therefore never recreate state.
	generation uint64
	operations map[uint64]context.CancelFunc
	refreshing bool
}

type tombstone struct {
	actorID   uuid.UUID
	kind      Kind
	exhausted bool
	expires   time.Time
}

type providerCleanup struct {
	kind       Kind
	provider   ProviderHandle
	operations []context.CancelFunc
	pending    bool
}

// New constructs an empty process-local coordinator.
func New(config Config) *Coordinator {
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.NewRequestID == nil {
		config.NewRequestID = uuid.New
	}
	if config.Capacity <= 0 {
		config.Capacity = MaxChallenges
	}
	if config.PhoneTTL <= 0 {
		config.PhoneTTL = PhoneChallengeTTL
	}
	if config.QRTTL <= 0 {
		config.QRTTL = QRChallengeTTL
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = MaxCodeAttempts
	}
	if config.CleanupTimeout <= 0 {
		config.CleanupTimeout = DefaultCleanupTimeout
	}
	coordinator := &Coordinator{
		clock:           config.Clock,
		newRequestID:    config.NewRequestID,
		capacity:        config.Capacity,
		phoneTTL:        config.PhoneTTL,
		qrTTL:           config.QRTTL,
		maxAttempts:     config.MaxAttempts,
		cleanupTimeout:  config.CleanupTimeout,
		phone:           config.PhoneProvider,
		qr:              config.QRProvider,
		cleanupExecutor: newCleanupExecutor(config.CleanupWorkers, config.CleanupQueueSize, config.CleanupTimeout, config.PhoneProvider, config.QRProvider),
		challenges:      make(map[uuid.UUID]*challenge),
		phoneSlots:      make(map[uuid.UUID]uuid.UUID),
		qrSlots:         make(map[uuid.UUID]uuid.UUID),
		tombstones:      make(map[uuid.UUID]tombstone),
	}
	coordinator.cleanupCond = sync.NewCond(&coordinator.mu)
	return coordinator
}

// Stop closes the coordinator and prevents new state or cleanup work from
// being created. Active records are invalidated before the cleanup executor is
// stopped. Cleanup which cannot be offered because the bounded queue is full
// may be dropped; accepted tasks are drained by workers. Provider calls
// already running in bounded executor workers are allowed to finish on their
// own. Stop is intentionally nonblocking.
func (coordinator *Coordinator) Stop() {
	coordinator.closeOnce.Do(func() {
		coordinator.closeState()
	})
}

// Close is the nonblocking lifecycle shutdown for a coordinator. It marks the
// coordinator closed, cancels in-flight operations, queues known provider
// handles for best-effort cancellation, and then stops the cleanup executor.
// Cleanup calls which ignore their context cannot be forcibly interrupted.
func (coordinator *Coordinator) Close() {
	coordinator.Stop()
}

// Shutdown stops the cleanup executor and waits until its workers exit or ctx
// expires. The context is only a bound for waiting; it cannot interrupt a
// provider cleanup call which ignores its deadline.
func (coordinator *Coordinator) Shutdown(ctx context.Context) error {
	coordinator.Stop()
	return coordinator.cleanupExecutor.waitForStop(ctx)
}

// closeState performs the state transition exactly once. The closed bit and
// generation invalidation are committed under Coordinator.mu. Cancellation
// functions and cleanup tasks are deliberately invoked after unlocking: even
// a context cancellation callback must not run while the coordinator mutex is
// held, and provider cancellation is always owned by cleanupExecutor.
func (coordinator *Coordinator) closeState() {
	coordinator.mu.Lock()
	coordinator.closed = true
	cleanups := make([]providerCleanup, 0, len(coordinator.challenges))
	for _, record := range coordinator.challenges {
		cleanups = append(cleanups, coordinator.removeLocked(record, false))
	}
	for requestID := range coordinator.tombstones {
		delete(coordinator.tombstones, requestID)
	}
	coordinator.mu.Unlock()

	// This is the required ordering: all known operation cancellations and
	// provider handles are offered while the executor still accepts tasks.
	for _, cleanup := range cleanups {
		coordinator.runCleanup(cleanup)
	}
	coordinator.waitForCleanupOffers()
	coordinator.cleanupExecutor.stopWorkers()
}

// waitForCleanupOffers waits only for concurrent state removals to hand their
// cleanup to the executor. It never waits for a provider call. This closes the
// race where Cancel removes a record, Close stops the executor, and the late
// Cancel cleanup would otherwise be silently discarded.
func (coordinator *Coordinator) waitForCleanupOffers() {
	coordinator.mu.Lock()
	for coordinator.cleanupPending != 0 {
		coordinator.cleanupCond.Wait()
	}
	// Prevent a late result from registering an offer after the final pending
	// check and racing the executor stop. Results arriving after this point are
	// outside the close linearization and are best-effort dropped by the stopped
	// executor.
	coordinator.cleanupStopping = true
	coordinator.mu.Unlock()
}

func (coordinator *Coordinator) isClosed() bool {
	coordinator.mu.Lock()
	closed := coordinator.closed
	coordinator.mu.Unlock()
	return closed
}

// registerOperationLocked records every provider operation which must be
// canceled by Close. The caller owns Coordinator.mu.
func (coordinator *Coordinator) registerOperationLocked(record *challenge, cancel context.CancelFunc) uint64 {
	coordinator.operationVersion++
	operationID := coordinator.operationVersion
	if record.operations == nil {
		record.operations = make(map[uint64]context.CancelFunc)
	}
	record.operations[operationID] = cancel
	return operationID
}

// completeOperation removes a completed operation reference after its child
// CancelFunc has been invoked by the caller. It is generation-safe so a late
// provider return cannot delete a replacement record's operation.
func (coordinator *Coordinator) completeOperation(requestID uuid.UUID, generation, operationID uint64, _ context.CancelFunc) {
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if record != nil && record.generation == generation {
		delete(record.operations, operationID)
	}
	coordinator.mu.Unlock()
}

// NewCoordinator is a descriptive constructor alias.
func NewCoordinator(config Config) *Coordinator { return New(config) }

// NewWithProviders is a convenient composition constructor for tests and
// explicit development graphs.
func NewWithProviders(phone PhoneProvider, qr QRProvider, options ...Option) *Coordinator {
	config := Config{PhoneProvider: phone, QRProvider: qr}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return New(config)
}

// StartPhone starts one phone challenge for actor. A same-kind challenge is
// reserved before the provider call, so a blocked provider cannot be raced by
// a replacement start.
func (coordinator *Coordinator) StartPhone(ctx context.Context, actor applicationroot.Actor, phone string) (common.PhoneChallenge, error) {
	projection, failure := coordinator.StartPhoneChallenge(ctx, actor, phone)
	if failure != nil {
		return common.PhoneChallenge{}, failure
	}
	return common.PhoneChallenge{
		RequestID: projection.RequestID,
		Phone:     projection.Phone,
		Delivery:  projection.Delivery,
		ExpiresAt: projection.ExpiresAt,
	}, nil
}

// StartPhoneChallenge returns the full safe phone projection.
func (coordinator *Coordinator) StartPhoneChallenge(ctx context.Context, actor applicationroot.Actor, phone string) (PhoneProjection, error) {
	if coordinator.isClosed() {
		return PhoneProjection{}, ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return PhoneProjection{}, failure
	}
	normalized, failure := normalizePhone(phone)
	if failure != nil {
		return PhoneProjection{}, failure
	}
	coordinator.cleanup()
	now := coordinator.clock()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return PhoneProjection{}, ErrCoordinatorClosed
	}
	if _, occupied := coordinator.phoneSlots[actor.OperatorID]; occupied {
		coordinator.mu.Unlock()
		return PhoneProjection{}, ErrChallengeAlreadyActive
	}
	if !coordinator.reserveCapacityLocked(now) {
		coordinator.mu.Unlock()
		return PhoneProjection{}, ErrChallengeCapacity
	}
	requestID := coordinator.nextRequestIDLocked()
	coordinator.version++
	generation := coordinator.version
	operationContext, operationCancel := context.WithCancel(ctx)
	record := &challenge{
		requestID:  requestID,
		actorID:    actor.OperatorID,
		kind:       KindPhone,
		state:      StateStarting,
		phone:      normalized,
		expires:    now.Add(coordinator.phoneTTL),
		generation: generation,
		operations: make(map[uint64]context.CancelFunc),
	}
	coordinator.challenges[requestID] = record
	coordinator.phoneSlots[actor.OperatorID] = requestID
	operationID := coordinator.registerOperationLocked(record, operationCancel)
	coordinator.mu.Unlock()

	if coordinator.phone == nil {
		coordinator.removeIfCurrent(requestID, actor.OperatorID, generation, false)
		return PhoneProjection{}, ErrProviderUnavailable
	}

	finished := make(chan struct{})
	go coordinator.watchStarting(ctx, operationContext, requestID, actor.OperatorID, generation, finished)
	started, providerFailure := coordinator.phone.StartPhone(operationContext, PhoneStart{Phone: normalized})
	close(finished)
	// Start has returned. Always release the child context and remove its
	// reference, including successful starts and late results after Close.
	operationCancel()
	coordinator.completeOperation(requestID, generation, operationID, operationCancel)
	return coordinator.finishPhoneStart(ctx, actor, requestID, generation, started, providerFailure)
}

// StartQR starts one QR challenge independently of the phone slot.
func (coordinator *Coordinator) StartQR(ctx context.Context, actor applicationroot.Actor) (common.QRChallenge, error) {
	projection, failure := coordinator.StartQRChallenge(ctx, actor)
	if failure != nil {
		return common.QRChallenge{}, failure
	}
	return common.QRChallenge{RequestID: projection.RequestID, URL: projection.URL, ExpiresAt: projection.ExpiresAt}, nil
}

// StartQRChallenge returns the safe QR projection.
func (coordinator *Coordinator) StartQRChallenge(ctx context.Context, actor applicationroot.Actor) (QRProjection, error) {
	if coordinator.isClosed() {
		return QRProjection{}, ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return QRProjection{}, failure
	}
	coordinator.cleanup()
	now := coordinator.clock()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrCoordinatorClosed
	}
	if _, occupied := coordinator.qrSlots[actor.OperatorID]; occupied {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrChallengeAlreadyActive
	}
	if !coordinator.reserveCapacityLocked(now) {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrChallengeCapacity
	}
	requestID := coordinator.nextRequestIDLocked()
	coordinator.version++
	generation := coordinator.version
	operationContext, operationCancel := context.WithCancel(ctx)
	record := &challenge{
		requestID:  requestID,
		actorID:    actor.OperatorID,
		kind:       KindQR,
		state:      StateStarting,
		expires:    now.Add(coordinator.qrTTL),
		generation: generation,
		operations: make(map[uint64]context.CancelFunc),
	}
	coordinator.challenges[requestID] = record
	coordinator.qrSlots[actor.OperatorID] = requestID
	operationID := coordinator.registerOperationLocked(record, operationCancel)
	coordinator.mu.Unlock()

	if coordinator.qr == nil {
		coordinator.removeIfCurrent(requestID, actor.OperatorID, generation, false)
		return QRProjection{}, ErrProviderUnavailable
	}
	finished := make(chan struct{})
	go coordinator.watchStarting(ctx, operationContext, requestID, actor.OperatorID, generation, finished)
	started, providerFailure := coordinator.qr.StartQR(operationContext, QRStart{})
	close(finished)
	operationCancel()
	coordinator.completeOperation(requestID, generation, operationID, operationCancel)
	return coordinator.finishQRStart(ctx, actor, requestID, generation, started, providerFailure)
}

// VerifyPhone is the legacy CQS-shaped completion command. It returns only a
// provider account projection; this coordinator performs no persistence.
func (coordinator *Coordinator) VerifyPhone(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (common.Account, error) {
	result, failure := coordinator.SubmitPhoneCode(ctx, actor, requestID, code)
	return result.Account, failure
}

// SubmitPhoneCode atomically reserves one of the five attempts before calling
// the provider. Concurrent submissions therefore cannot exceed the cap.
func (coordinator *Coordinator) SubmitPhoneCode(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (Submission, error) {
	if coordinator.isClosed() {
		return Submission{}, ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return Submission{}, failure
	}
	if requestID == uuid.Nil || strings.TrimSpace(code) == "" {
		return Submission{}, ErrInvalidInput
	}
	coordinator.cleanup()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Submission{}, ErrCoordinatorClosed
	}
	record := coordinator.challenges[requestID]
	if record == nil {
		if previous, ok := coordinator.tombstones[requestID]; ok && previous.actorID == actor.OperatorID && previous.kind == KindPhone && previous.exhausted && coordinator.clock().Before(previous.expires) {
			coordinator.mu.Unlock()
			return Submission{}, ErrAttemptsExceeded
		}
		coordinator.mu.Unlock()
		return Submission{}, ErrChallengeUnavailable
	}
	if record.actorID != actor.OperatorID || record.kind != KindPhone || record.handle.empty() {
		coordinator.mu.Unlock()
		return Submission{}, ErrChallengeUnavailable
	}
	if record.state == StateExhausted {
		coordinator.mu.Unlock()
		return Submission{}, ErrAttemptsExceeded
	}
	if record.state != StateActive {
		coordinator.mu.Unlock()
		return Submission{}, ErrChallengeUnavailable
	}
	if record.attempts >= coordinator.maxAttempts {
		coordinator.mu.Unlock()
		return Submission{}, ErrAttemptsExceeded
	}
	provider := coordinator.phone
	handle := record.handle
	generation := record.generation
	operationContext, operationCancel := context.WithCancel(ctx)
	operationID := coordinator.registerOperationLocked(record, operationCancel)
	record.attempts++
	record.inflight++
	if record.attempts >= coordinator.maxAttempts {
		record.state = StateExhausted
	}
	attemptsUsed := record.attempts
	attemptsRemaining := coordinator.maxAttempts - attemptsUsed
	coordinator.mu.Unlock()

	if provider == nil {
		operationCancel()
		coordinator.completeOperation(requestID, generation, operationID, operationCancel)
		return coordinator.finishPhoneVerification(ctx, actor, requestID, generation, attemptsUsed, attemptsRemaining, PhoneVerified{}, ErrProviderUnavailable)
	}
	verified, providerFailure := provider.VerifyPhone(operationContext, handle, strings.TrimSpace(code))
	operationCancel()
	coordinator.completeOperation(requestID, generation, operationID, operationCancel)
	return coordinator.finishPhoneVerification(ctx, actor, requestID, generation, attemptsUsed, attemptsRemaining, verified, providerFailure)
}

// RefreshQR rotates a QR provider token while retaining the application
// request ID. Provider calls never run while coordinator.mu is held.
func (coordinator *Coordinator) RefreshQR(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) (common.QRChallenge, error) {
	projection, failure := coordinator.RefreshQRChallenge(ctx, actor, requestID)
	if failure != nil {
		return common.QRChallenge{}, failure
	}
	return common.QRChallenge{RequestID: projection.RequestID, URL: projection.URL, ExpiresAt: projection.ExpiresAt}, nil
}

// RefreshQRChallenge refreshes the safe QR projection.
func (coordinator *Coordinator) RefreshQRChallenge(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) (QRProjection, error) {
	if coordinator.isClosed() {
		return QRProjection{}, ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return QRProjection{}, failure
	}
	if requestID == uuid.Nil {
		return QRProjection{}, ErrChallengeUnavailable
	}
	coordinator.cleanup()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrCoordinatorClosed
	}
	record := coordinator.challenges[requestID]
	if record == nil || record.actorID != actor.OperatorID || record.kind != KindQR || record.state != StateActive || record.handle.empty() {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrChallengeUnavailable
	}
	if record.refreshing {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrChallengeAlreadyActive
	}
	provider := coordinator.qr
	if provider == nil {
		coordinator.mu.Unlock()
		return QRProjection{}, ErrProviderUnavailable
	}
	record.refreshing = true
	operationContext, operationCancel := context.WithCancel(ctx)
	operationID := coordinator.registerOperationLocked(record, operationCancel)
	generation := record.generation
	oldHandle := record.handle
	coordinator.mu.Unlock()

	refreshed, providerFailure := provider.RefreshQR(operationContext, oldHandle)
	operationCancel()
	coordinator.completeOperation(requestID, generation, operationID, operationCancel)
	return coordinator.finishQRRefresh(ctx, actor, requestID, generation, oldHandle, refreshed, providerFailure)
}

// Cancel is an owning, idempotent cancellation command. A repeated cancel by
// the same actor succeeds via a bounded tombstone; another actor receives the
// same unavailable result as a random ID.
func (coordinator *Coordinator) Cancel(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	if coordinator.isClosed() {
		return ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return failure
	}
	if requestID == uuid.Nil {
		return ErrChallengeUnavailable
	}
	coordinator.cleanup()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return ErrCoordinatorClosed
	}
	record := coordinator.challenges[requestID]
	if record == nil {
		if previous, ok := coordinator.tombstones[requestID]; ok && previous.actorID == actor.OperatorID && coordinator.clock().Before(previous.expires) {
			coordinator.mu.Unlock()
			return nil
		}
		coordinator.mu.Unlock()
		return ErrChallengeUnavailable
	}
	if record.actorID != actor.OperatorID {
		coordinator.mu.Unlock()
		return ErrChallengeUnavailable
	}
	cleanup := coordinator.removeLocked(record, true)
	coordinator.mu.Unlock()
	coordinator.runCleanup(cleanup)
	return nil
}

// CancelPhone and CancelQR are explicit aliases for callers whose command
// types already encode the challenge kind.
func (coordinator *Coordinator) CancelPhone(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	return coordinator.cancelKind(ctx, actor, requestID, KindPhone)
}

func (coordinator *Coordinator) CancelQR(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	return coordinator.cancelKind(ctx, actor, requestID, KindQR)
}

func (coordinator *Coordinator) cancelKind(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, kind Kind) error {
	if coordinator.isClosed() {
		return ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return failure
	}
	if requestID == uuid.Nil {
		return ErrChallengeUnavailable
	}
	coordinator.cleanup()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return ErrCoordinatorClosed
	}
	record := coordinator.challenges[requestID]
	if record == nil || record.actorID != actor.OperatorID || record.kind != kind {
		if record == nil {
			if previous, ok := coordinator.tombstones[requestID]; ok && previous.actorID == actor.OperatorID && previous.kind == kind && previous.expires.After(coordinator.clock()) {
				coordinator.mu.Unlock()
				return nil
			}
		}
		coordinator.mu.Unlock()
		return ErrChallengeUnavailable
	}
	cleanup := coordinator.removeLocked(record, true)
	coordinator.mu.Unlock()
	coordinator.runCleanup(cleanup)
	return nil
}

// Query returns an actor-scoped safe snapshot with no account or provider
// identity fields.
func (coordinator *Coordinator) Query(ctx context.Context, actor applicationroot.Actor) (StatusProjection, error) {
	if coordinator.isClosed() {
		return StatusProjection{}, ErrCoordinatorClosed
	}
	if failure := validateCommandContext(ctx, actor); failure != nil {
		return StatusProjection{}, failure
	}
	coordinator.cleanup()
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.closed {
		return StatusProjection{}, ErrCoordinatorClosed
	}
	status := StatusProjection{}
	if requestID, ok := coordinator.phoneSlots[actor.OperatorID]; ok {
		if record := coordinator.challenges[requestID]; record != nil {
			projection := coordinator.phoneProjectionLocked(record)
			status.Phone = &projection
		}
	}
	if requestID, ok := coordinator.qrSlots[actor.OperatorID]; ok {
		if record := coordinator.challenges[requestID]; record != nil {
			projection := coordinator.qrProjectionLocked(record)
			status.QR = &projection
		}
	}
	return status, nil
}

// Get is the descriptive alias for Query used by callers that model the
// coordinator as a lifecycle-owned store. It has the same closed semantics.
func (coordinator *Coordinator) Get(ctx context.Context, actor applicationroot.Actor) (StatusProjection, error) {
	return coordinator.Query(ctx, actor)
}

// Status adapts the safe challenge query to the existing transport-neutral
// status projection. It intentionally has no account rows.
func (coordinator *Coordinator) Status(ctx context.Context, actor applicationroot.Actor) (common.Status, error) {
	status, failure := coordinator.Query(ctx, actor)
	if failure != nil {
		return common.Status{}, failure
	}
	result := common.Status{}
	if status.Phone != nil {
		phone := common.PhoneChallenge{RequestID: status.Phone.RequestID, Phone: status.Phone.Phone, Delivery: status.Phone.Delivery, ExpiresAt: status.Phone.ExpiresAt}
		result.PhoneChallenge = &phone
	}
	if status.QR != nil {
		qr := common.QRChallenge{RequestID: status.QR.RequestID, URL: status.QR.URL, ExpiresAt: status.QR.ExpiresAt}
		result.QRChallenge = &qr
	}
	return result, nil
}

func (coordinator *Coordinator) finishPhoneStart(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, generation uint64, started PhoneStarted, providerFailure error) (PhoneProjection, error) {
	if providerFailure != nil {
		cleanup, current := coordinator.removeStartingCurrent(requestID, actor.OperatorID, generation, false)
		coordinator.runCleanup(cleanup)
		if !started.Handle.empty() {
			coordinator.runCleanup(providerCleanup{kind: KindPhone, provider: started.Handle})
		}
		if current {
			if ctx.Err() != nil {
				return PhoneProjection{}, ctx.Err()
			}
			if coordinator.isClosed() {
				return PhoneProjection{}, ErrCoordinatorClosed
			}
			return PhoneProjection{}, ErrProviderUnavailable
		}
		if coordinator.isClosed() {
			return PhoneProjection{}, ErrCoordinatorClosed
		}
		return PhoneProjection{}, ErrChallengeUnavailable
	}
	if started.Handle.empty() {
		cleanup, _ := coordinator.removeStartingCurrent(requestID, actor.OperatorID, generation, false)
		coordinator.runCleanup(cleanup)
		if coordinator.isClosed() {
			return PhoneProjection{}, ErrCoordinatorClosed
		}
		return PhoneProjection{}, ErrProviderUnavailable
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	valid := !coordinator.closed && record != nil && record.actorID == actor.OperatorID && record.generation == generation && record.state == StateStarting && coordinator.clock().Before(record.expires) && ctx.Err() == nil
	if !valid {
		var cleanup providerCleanup
		if record != nil && record.actorID == actor.OperatorID && record.generation == generation && record.state == StateStarting {
			cleanup = coordinator.removeLocked(record, false)
		}
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
		coordinator.runCleanup(providerCleanup{kind: KindPhone, provider: started.Handle})
		if coordinator.isClosed() {
			return PhoneProjection{}, ErrCoordinatorClosed
		}
		return PhoneProjection{}, ErrChallengeUnavailable
	}
	record.state = StateActive
	record.handle = started.Handle
	record.delivery = started.Delivery
	if record.delivery == "" {
		record.delivery = "Telegram code"
	}
	projection := coordinator.phoneProjectionLocked(record)
	coordinator.mu.Unlock()
	return projection, nil
}

func (coordinator *Coordinator) finishQRStart(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, generation uint64, started QRStarted, providerFailure error) (QRProjection, error) {
	if providerFailure != nil {
		cleanup, current := coordinator.removeStartingCurrent(requestID, actor.OperatorID, generation, false)
		coordinator.runCleanup(cleanup)
		if !started.Handle.empty() {
			coordinator.runCleanup(providerCleanup{kind: KindQR, provider: started.Handle})
		}
		if current {
			if ctx.Err() != nil {
				return QRProjection{}, ctx.Err()
			}
			if coordinator.isClosed() {
				return QRProjection{}, ErrCoordinatorClosed
			}
			return QRProjection{}, ErrProviderUnavailable
		}
		if coordinator.isClosed() {
			return QRProjection{}, ErrCoordinatorClosed
		}
		return QRProjection{}, ErrChallengeUnavailable
	}
	if started.Handle.empty() || strings.TrimSpace(started.URL) == "" {
		cleanup, _ := coordinator.removeStartingCurrent(requestID, actor.OperatorID, generation, false)
		coordinator.runCleanup(cleanup)
		if !started.Handle.empty() {
			coordinator.runCleanup(providerCleanup{kind: KindQR, provider: started.Handle})
		}
		if coordinator.isClosed() {
			return QRProjection{}, ErrCoordinatorClosed
		}
		return QRProjection{}, ErrProviderUnavailable
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	valid := !coordinator.closed && record != nil && record.actorID == actor.OperatorID && record.generation == generation && record.state == StateStarting && coordinator.clock().Before(record.expires) && ctx.Err() == nil
	if !valid {
		var cleanup providerCleanup
		if record != nil && record.actorID == actor.OperatorID && record.generation == generation && record.state == StateStarting {
			cleanup = coordinator.removeLocked(record, false)
		}
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
		coordinator.runCleanup(providerCleanup{kind: KindQR, provider: started.Handle})
		if coordinator.isClosed() {
			return QRProjection{}, ErrCoordinatorClosed
		}
		return QRProjection{}, ErrChallengeUnavailable
	}
	record.state = StateActive
	record.handle = started.Handle
	record.url = started.URL
	projection := coordinator.qrProjectionLocked(record)
	coordinator.mu.Unlock()
	return projection, nil
}

func (coordinator *Coordinator) finishPhoneVerification(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, generation uint64, attemptsUsed, attemptsRemaining int, verified PhoneVerified, providerFailure error) (Submission, error) {
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Submission{}, ErrCoordinatorClosed
	}
	if record == nil || record.actorID != actor.OperatorID || record.kind != KindPhone || record.generation != generation || (record.state != StateActive && record.state != StateExhausted) {
		coordinator.mu.Unlock()
		return Submission{}, ErrChallengeUnavailable
	}
	if !coordinator.clock().Before(record.expires) {
		cleanup := coordinator.removeLocked(record, false)
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
		return Submission{}, ErrChallengeUnavailable
	}
	if providerFailure == nil && ctx.Err() != nil {
		providerFailure = ctx.Err()
	}
	if record.inflight > 0 {
		record.inflight--
	}
	if providerFailure == nil {
		cleanup := coordinator.removeLocked(record, false)
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
		return Submission{Completed: true, AttemptsUsed: attemptsUsed, AttemptsRemaining: attemptsRemaining, Account: verified.Account}, nil
	}
	shouldRemove := record.attempts >= coordinator.maxAttempts && record.inflight == 0
	if shouldRemove {
		cleanup := coordinator.removeLockedReason(record, false, true)
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
	} else {
		coordinator.mu.Unlock()
	}
	if ctx.Err() != nil {
		return Submission{AttemptsUsed: attemptsUsed, AttemptsRemaining: max(0, attemptsRemaining)}, ctx.Err()
	}
	return Submission{AttemptsUsed: attemptsUsed, AttemptsRemaining: max(0, attemptsRemaining)}, ErrCodeRejected
}

func (coordinator *Coordinator) finishQRRefresh(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, generation uint64, oldHandle ProviderHandle, refreshed QRStarted, providerFailure error) (QRProjection, error) {
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if coordinator.closed {
		coordinator.mu.Unlock()
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		return QRProjection{}, ErrCoordinatorClosed
	}
	if record == nil || record.actorID != actor.OperatorID || record.kind != KindQR || record.generation != generation || record.state != StateActive || !record.refreshing {
		coordinator.mu.Unlock()
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		return QRProjection{}, ErrChallengeUnavailable
	}
	if !coordinator.clock().Before(record.expires) {
		cleanup := coordinator.removeLocked(record, false)
		coordinator.mu.Unlock()
		coordinator.runCleanup(cleanup)
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		return QRProjection{}, ErrChallengeUnavailable
	}
	record.refreshing = false
	if ctx.Err() != nil {
		coordinator.mu.Unlock()
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		return QRProjection{}, ctx.Err()
	}
	if providerFailure != nil {
		coordinator.mu.Unlock()
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		if ctx.Err() != nil {
			return QRProjection{}, ctx.Err()
		}
		return QRProjection{}, ErrProviderUnavailable
	}
	if refreshed.Handle.empty() || strings.TrimSpace(refreshed.URL) == "" {
		coordinator.mu.Unlock()
		coordinator.cancelQRRefreshResult(oldHandle, refreshed.Handle)
		return QRProjection{}, ErrProviderUnavailable
	}
	record.handle = refreshed.Handle
	record.url = refreshed.URL
	var oldCleanup providerCleanup
	if !oldHandle.empty() && oldHandle != refreshed.Handle {
		// Reserve this offer before releasing Coordinator.mu. Close can then
		// wait for the old provider handle to be enqueued rather than racing the
		// refresh result's post-commit cleanup.
		oldCleanup = coordinator.reserveCleanupLocked(providerCleanup{kind: KindQR, provider: oldHandle})
	}
	projection := coordinator.qrProjectionLocked(record)
	coordinator.mu.Unlock()
	if oldCleanup.pending {
		coordinator.runCleanup(oldCleanup)
	}
	return projection, nil
}

func (coordinator *Coordinator) cancelQRRefreshResult(oldHandle, refreshedHandle ProviderHandle) {
	if refreshedHandle.empty() || oldHandle == refreshedHandle {
		return
	}
	coordinator.runCleanup(providerCleanup{kind: KindQR, provider: refreshedHandle})
}

func (coordinator *Coordinator) watchStarting(parentContext, operationContext context.Context, requestID, actorID uuid.UUID, generation uint64, finished <-chan struct{}) {
	select {
	case <-parentContext.Done():
		coordinator.removeIfCurrent(requestID, actorID, generation, true)
	case <-operationContext.Done():
		// A normal provider return closes finished before the start path cancels
		// the child context. If the child is canceled first (Close or caller
		// cancellation), remove the still-starting record immediately.
		select {
		case <-finished:
			if parentContext.Err() != nil {
				coordinator.removeIfCurrent(requestID, actorID, generation, true)
			}
		default:
			coordinator.removeIfCurrent(requestID, actorID, generation, true)
		}
	case <-finished:
		// Both channels can be ready at once. Re-check cancellation so a
		// finished signal cannot make the watcher silently abandon a still
		// starting record. The removal helper linearizes this with the finish
		// transition and is a no-op once StateActive has been committed.
		if parentContext.Err() != nil {
			coordinator.removeIfCurrent(requestID, actorID, generation, true)
		}
	}
}

func (coordinator *Coordinator) removeIfCurrent(requestID, actorID uuid.UUID, generation uint64, tombstoneIt bool) {
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if record == nil || record.actorID != actorID || record.generation != generation || record.state != StateStarting {
		coordinator.mu.Unlock()
		return
	}
	cleanup := coordinator.removeLocked(record, tombstoneIt)
	coordinator.mu.Unlock()
	coordinator.runCleanup(cleanup)
}

func (coordinator *Coordinator) removeStartingCurrent(requestID, actorID uuid.UUID, generation uint64, tombstoneIt bool) (providerCleanup, bool) {
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if record == nil || record.actorID != actorID || record.generation != generation || record.state != StateStarting {
		coordinator.mu.Unlock()
		return providerCleanup{}, false
	}
	cleanup := coordinator.removeLocked(record, tombstoneIt)
	coordinator.mu.Unlock()
	return cleanup, true
}

func (coordinator *Coordinator) removeLocked(record *challenge, tombstoneIt bool) providerCleanup {
	return coordinator.removeLockedReason(record, tombstoneIt, false)
}

func (coordinator *Coordinator) removeLockedReason(record *challenge, tombstoneIt, exhausted bool) providerCleanup {
	delete(coordinator.challenges, record.requestID)
	switch record.kind {
	case KindPhone:
		if coordinator.phoneSlots[record.actorID] == record.requestID {
			delete(coordinator.phoneSlots, record.actorID)
		}
	case KindQR:
		if coordinator.qrSlots[record.actorID] == record.requestID {
			delete(coordinator.qrSlots, record.actorID)
		}
	}
	if tombstoneIt || exhausted {
		coordinator.tombstones[record.requestID] = tombstone{actorID: record.actorID, kind: record.kind, exhausted: exhausted, expires: coordinator.clock().Add(coordinator.tombstoneTTL(record.kind))}
	}
	record.generation++
	operations := make([]context.CancelFunc, 0, len(record.operations))
	for _, operationCancel := range record.operations {
		operations = append(operations, operationCancel)
	}
	record.operations = nil
	return coordinator.reserveCleanupLocked(providerCleanup{kind: record.kind, provider: record.handle, operations: operations})
}

// reserveCleanupLocked linearizes a cleanup offer with the state transition
// which produced it. The caller owns Coordinator.mu.
func (coordinator *Coordinator) reserveCleanupLocked(cleanup providerCleanup) providerCleanup {
	coordinator.cleanupPending++
	cleanup.pending = true
	return cleanup
}

func (coordinator *Coordinator) tombstoneTTL(kind Kind) time.Duration {
	// Tombstones use the same explicit kind policy as their live challenges:
	// phone terminal/cancel tombstones use PhoneTTL and QR cancellation
	// tombstones use QRTTL.
	switch kind {
	case KindQR:
		return coordinator.qrTTL
	case KindPhone:
		return coordinator.phoneTTL
	default:
		return coordinator.phoneTTL
	}
}

func (coordinator *Coordinator) cleanup() {
	if coordinator == nil {
		return
	}
	now := coordinator.clock()
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return
	}
	cleanups := make([]providerCleanup, 0)
	for requestID, previous := range coordinator.tombstones {
		if !now.Before(previous.expires) {
			delete(coordinator.tombstones, requestID)
		}
	}
	for _, record := range coordinator.challenges {
		if !now.Before(record.expires) {
			cleanups = append(cleanups, coordinator.removeLocked(record, false))
		}
	}
	coordinator.mu.Unlock()
	for _, cleanup := range cleanups {
		coordinator.runCleanup(cleanup)
	}
}

func (coordinator *Coordinator) reserveCapacityLocked(now time.Time) bool {
	for requestID, previous := range coordinator.tombstones {
		if !now.Before(previous.expires) {
			delete(coordinator.tombstones, requestID)
		}
	}
	if len(coordinator.challenges)+len(coordinator.tombstones) < coordinator.capacity {
		return true
	}
	// Tombstones are only for idempotent cancel and are safe to evict first.
	for requestID := range coordinator.tombstones {
		delete(coordinator.tombstones, requestID)
		if len(coordinator.challenges)+len(coordinator.tombstones) < coordinator.capacity {
			return true
		}
	}
	return len(coordinator.challenges) < coordinator.capacity
}

func (coordinator *Coordinator) nextRequestIDLocked() uuid.UUID {
	for {
		requestID := coordinator.newRequestID()
		if requestID != uuid.Nil {
			if _, exists := coordinator.challenges[requestID]; !exists {
				if _, tombstoned := coordinator.tombstones[requestID]; !tombstoned {
					return requestID
				}
			}
		}
	}
}

func (coordinator *Coordinator) phoneProjectionLocked(record *challenge) PhoneProjection {
	return PhoneProjection{
		RequestID: record.requestID, Phone: record.phone, Delivery: record.delivery,
		ExpiresAt: record.expires, State: record.state,
		AttemptsUsed: record.attempts, AttemptsRemaining: max(0, coordinator.maxAttempts-record.attempts),
	}
}

func (coordinator *Coordinator) qrProjectionLocked(record *challenge) QRProjection {
	return QRProjection{RequestID: record.requestID, URL: record.url, ExpiresAt: record.expires, State: record.state}
}

func (coordinator *Coordinator) runCleanup(cleanup providerCleanup) {
	if !cleanup.pending && coordinator != nil {
		coordinator.mu.Lock()
		if !coordinator.cleanupStopping {
			coordinator.cleanupPending++
			cleanup.pending = true
		}
		coordinator.mu.Unlock()
	}
	if cleanup.pending {
		defer coordinator.finishCleanupOffer()
	}
	for _, operationCancel := range cleanup.operations {
		// Cancel operations immediately so a cooperative start/refresh/verify can
		// release its caller promptly even when the cleanup queue is full.
		if operationCancel != nil {
			operationCancel()
		}
	}
	if cleanup.provider.empty() {
		return
	}
	// This is intentionally a nonblocking offer. In particular, never call a
	// provider while holding coordinator.mu or while serving a state-changing
	// request. A full queue means best-effort provider cancellation is dropped.
	if coordinator != nil && coordinator.cleanupExecutor != nil {
		coordinator.cleanupExecutor.enqueue(cleanupTask{kind: cleanup.kind, handle: cleanup.provider})
	}
}

func (coordinator *Coordinator) finishCleanupOffer() {
	coordinator.mu.Lock()
	if coordinator.cleanupPending > 0 {
		coordinator.cleanupPending--
	}
	if coordinator.cleanupPending == 0 {
		coordinator.cleanupCond.Broadcast()
	}
	coordinator.mu.Unlock()
}

func validateCommandContext(ctx context.Context, actor applicationroot.Actor) error {
	if failure := ctx.Err(); failure != nil {
		return failure
	}
	if actor.OperatorID == uuid.Nil {
		return ErrInvalidInput
	}
	return nil
}

func normalizePhone(phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", ErrInvalidInput
	}
	var normalized strings.Builder
	for index, character := range phone {
		switch {
		case character == '+' && index == 0:
			normalized.WriteRune(character)
		case unicode.IsDigit(character):
			normalized.WriteRune(character)
		case character == ' ' || character == '-' || character == '(' || character == ')' || character == '.':
		default:
			return "", ErrInvalidInput
		}
	}
	value := normalized.String()
	if after, ok := strings.CutPrefix(value, "00"); ok {
		value = "+" + after
	}
	if !strings.HasPrefix(value, "+") {
		value = "+" + value
	}
	digits := strings.TrimPrefix(value, "+")
	if len(digits) < 7 || len(digits) > 15 || digits[0] == '0' {
		return "", ErrInvalidInput
	}
	return value, nil
}

// Compile-time checks for the existing CQS ports. Keeping these adapters on
// the coordinator lets an explicit development composition pass it directly
// to the current HTTP boundary without introducing another mutable store.
var _ interface {
	StartPhone(context.Context, applicationroot.Actor, string) (common.PhoneChallenge, error)
	VerifyPhone(context.Context, applicationroot.Actor, uuid.UUID, string) (common.Account, error)
	StartQR(context.Context, applicationroot.Actor) (common.QRChallenge, error)
	RefreshQR(context.Context, applicationroot.Actor, uuid.UUID) (common.QRChallenge, error)
	Status(context.Context, applicationroot.Actor) (common.Status, error)
} = (*Coordinator)(nil)
