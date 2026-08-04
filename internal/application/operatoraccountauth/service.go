package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

const (
	defaultChallengeTTL = 10 * time.Minute
	maxLiveChallenges   = 4096
	maxCodeAttempts     = 5
	maxPasswordAttempts = 5
	maxPasswordBytes    = 4096
	actorStartLockCount = 64
)

var (
	// ErrAttemptsExceeded means the fixed provider-attempt budget is spent.
	ErrAttemptsExceeded = errors.New("authentication challenge attempts exceeded")
	// ErrServiceClosed means shutdown has closed new authentication admission.
	ErrServiceClosed = errors.New("operator account authentication service closed")
	// ErrChallengeCapacity means the fixed process-local challenge bound is full.
	ErrChallengeCapacity = errors.New("authentication challenge capacity reached")
)

// ServiceConfig contains the small number of process-local seams needed by
// the application workflow. It does not contain provider or persistence
// configuration; those belong to their adapters.
type ServiceConfig struct {
	Clock        func() time.Time
	ChallengeTTL time.Duration
}

// ServiceOption customizes a Service for a composition root or a deterministic
// unit test.
type ServiceOption func(*ServiceConfig)

// WithClock makes challenge expiry and retry capping deterministic.
func WithClock(clock func() time.Time) ServiceOption {
	return func(config *ServiceConfig) {
		if clock != nil {
			config.Clock = clock
		}
	}
}

// WithChallengeTTL bounds every in-memory challenge and its completion
// tombstone.
func WithChallengeTTL(ttl time.Duration) ServiceOption {
	return func(config *ServiceConfig) {
		if ttl > 0 {
			config.ChallengeTTL = ttl
		}
	}
}

// Service coordinates durable operator-account admission, Telegram phone
// authentication, and runtime lifecycle. It never stores a phone-code hash
// itself; that secret remains in its private process-local coordinator.
type Service struct {
	persistence AuthenticationPersistence
	provider    PhoneProvider
	stopper     RuntimeStopper
	coordinator *runtimeCoordinator
	clock       func() time.Time
	ttl         time.Duration

	startGate    sync.RWMutex
	startByActor [actorStartLockCount]sync.Mutex
	closed       bool
}

// NewService constructs the approved runtime-backed phone-auth workflow.
// Dependencies are application ports; no transport or database type crosses
// this boundary.
func NewService(
	persistence AuthenticationPersistence,
	provider PhoneProvider,
	stopper RuntimeStopper,
	options ...ServiceOption,
) *Service {
	config := ServiceConfig{
		Clock:        time.Now,
		ChallengeTTL: defaultChallengeTTL,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.ChallengeTTL <= 0 {
		config.ChallengeTTL = defaultChallengeTTL
	}
	return &Service{
		persistence: persistence,
		provider:    provider,
		stopper:     stopper,
		coordinator: newRuntimeCoordinator(config.Clock, config.ChallengeTTL),
		clock:       config.Clock,
		ttl:         config.ChallengeTTL,
	}
}

// Start admits an account durably before sending Telegram's phone code. An
// active account is returned immediately; an in-process admission resumes its
// existing challenge instead of creating a second provider session.
func (service *Service) Start(ctx context.Context, actor applicationroot.Actor, phone string) (Result, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Result{}, err
	}
	service.startGate.RLock()
	actorStart := &service.startByActor[actorStartLockIndex(actor.OperatorID)]
	actorStart.Lock()
	admissionReleased := false
	releaseAdmission := func() {
		if admissionReleased {
			return
		}
		admissionReleased = true
		actorStart.Unlock()
		service.startGate.RUnlock()
	}
	defer releaseAdmission()
	if service.closed {
		return Result{}, ErrServiceClosed
	}
	normalized, err := normalizePhone(phone)
	if err != nil {
		return Result{}, err
	}
	requestedExpiry := service.clock().Add(service.ttl)
	begin, err := service.persistence.BeginOrResume(ctx, actor, normalized, requestedExpiry)
	if err != nil {
		return Result{}, fmt.Errorf("begin operator account authentication: %w", err)
	}
	if err := validateBeginResult(begin); err != nil {
		return Result{}, err
	}
	if begin.Outcome == BeginAlreadyActive {
		account := begin.Account
		return Result{Account: &account}, nil
	}

	target := AuthTarget{
		Actor:     actor,
		AccountID: operatoraccount.Identity(begin.Account.ID),
		Status:    begin.Account.Status,
		Version:   begin.Account.Version,
	}
	if existing, lookupErr := service.coordinator.FindForTarget(ctx, target); lookupErr == nil {
		limited, limitErr := service.coordinator.LimitExpiry(ctx, actor, existing.RequestID, begin.AuthExpiresAt)
		if limitErr != nil {
			if errors.Is(limitErr, errRuntimeChallengeExpired) {
				return service.expireChallenge(ctx, limited)
			}
			return Result{}, mapChallengeError(limitErr)
		}
		return Result{Challenge: &limited}, nil
	} else if errors.Is(lookupErr, errRuntimeChallengeExpired) {
		return service.expireChallenge(ctx, existing)
	}

	reserved, reserveErr := service.coordinator.Reserve(ctx, target, normalized, begin.AuthExpiresAt)
	if reserveErr != nil {
		if errors.Is(reserveErr, errRuntimeChallengeExpired) && reserved.RequestID != uuid.Nil && reserved.AuthTarget == target {
			return service.expireChallenge(ctx, reserved)
		}
		mapped := mapChallengeError(reserveErr)
		failures := []error{mapped}
		if errors.Is(reserveErr, errRuntimeChallengeExpired) && reserved.RequestID != uuid.Nil {
			if _, expireErr := service.expireChallenge(ctx, reserved); expireErr != ErrChallengeExpired {
				failures = append(failures, expireErr)
			}
		}
		if abortErr := service.abort(ctx, target); abortErr != nil {
			failures = append(failures, abortErr)
		}
		if len(failures) > 1 {
			return Result{}, errors.Join(failures...)
		}
		return Result{}, mapped
	}
	releaseAdmission()
	if service.provider == nil {
		_ = service.coordinator.Release(context.Background(), actor, reserved.RequestID)
		return service.abortProviderFailure(ctx, target, ErrProviderUnavailable)
	}

	sent, sendErr := service.provider.SendCode(ctx, target, normalized)
	if sendErr != nil {
		if !service.clock().Before(begin.AuthExpiresAt) {
			return service.expireChallenge(ctx, reserved)
		}
		if ctx.Err() != nil {
			_ = service.coordinator.Release(context.Background(), actor, reserved.RequestID)
			return Result{}, ctx.Err()
		}
		if isAbortProviderFailure(sendErr) {
			_ = service.coordinator.Release(context.Background(), actor, reserved.RequestID)
			return service.abortProviderFailure(ctx, target, classifyProviderFailure(sendErr, begin.AuthExpiresAt, service.clock()))
		}
		_ = service.coordinator.Release(context.Background(), actor, reserved.RequestID)
		return Result{}, classifyProviderFailure(sendErr, begin.AuthExpiresAt, service.clock())
	}
	challenge, attachErr := service.coordinator.Attach(ctx, actor, reserved.RequestID, sent)
	if attachErr != nil {
		if errors.Is(attachErr, errRuntimeChallengeExpired) {
			if _, expireErr := service.expireChallenge(ctx, challenge); expireErr != ErrChallengeExpired {
				return Result{}, expireErr
			}
			return Result{}, ErrChallengeExpired
		}
		if errors.Is(attachErr, errRuntimeClosed) {
			if abortErr := service.abort(ctx, target); abortErr != nil {
				return Result{}, errors.Join(ErrServiceClosed, abortErr)
			}
			return Result{}, ErrServiceClosed
		}
		_ = service.coordinator.Release(context.Background(), actor, reserved.RequestID)
		return Result{}, mapChallengeError(attachErr)
	}
	return Result{Challenge: &challenge}, nil
}

// Code submits a phone code using only the hash retained by the coordinator.
// Provider RPC and finalization run under the challenge's per-record mutex.
func (service *Service) Code(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (Result, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Result{}, err
	}
	if requestID == uuid.Nil || strings.TrimSpace(code) == "" {
		return Result{}, ErrInvalidInput
	}
	result, operationErr := service.coordinator.Operation(ctx, actor, requestID, func(operation *runtimeOperation) error {
		challenge := operation.Challenge()
		if pending, ok := operation.PendingProfile(); ok {
			return service.finalize(ctx, actor, operation, pending)
		}
		if challenge.Stage != StageCode {
			return ErrPasswordRequired
		}
		if !operation.ReserveCodeAttempt() {
			return ErrAttemptsExceeded
		}
		operationContext := operation.Context()
		if service.provider == nil {
			return service.abortOperation(operationContext, operation, ErrProviderUnavailable)
		}
		profile, providerErr := service.provider.SignIn(operationContext, operation.AuthTarget(), challenge.Phone, strings.TrimSpace(code), operation.PhoneCodeHash())
		if providerErr != nil {
			if operationContext.Err() != nil {
				return operationContext.Err()
			}
			if errors.Is(providerErr, ErrPasswordRequired) {
				_ = operation.SetStage(StagePassword)
				return ErrPasswordRequired
			}
			return service.handleProviderFailure(operationContext, operation, providerErr)
		}
		operation.SetPendingProfile(profile)
		return service.finalize(operationContext, actor, operation, profile)
	})
	if errors.Is(operationErr, errRuntimeChallengeExpired) && result.Challenge != nil {
		return service.expireChallenge(ctx, *result.Challenge)
	}
	return result, mapChallengeError(operationErr)
}

// Password submits Telegram's 2FA password after Code has selected the
// password stage. It is serialized with Code for the same challenge.
func (service *Service) Password(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, password string) (Result, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Result{}, err
	}
	if requestID == uuid.Nil || password == "" || len(password) > maxPasswordBytes {
		return Result{}, ErrInvalidInput
	}
	result, operationErr := service.coordinator.Operation(ctx, actor, requestID, func(operation *runtimeOperation) error {
		challenge := operation.Challenge()
		if pending, ok := operation.PendingProfile(); ok {
			return service.finalize(ctx, actor, operation, pending)
		}
		if challenge.Stage != StagePassword {
			return ErrPasswordRequired
		}
		if !operation.ReservePasswordAttempt() {
			return ErrAttemptsExceeded
		}
		operationContext := operation.Context()
		if service.provider == nil {
			return service.abortOperation(operationContext, operation, ErrProviderUnavailable)
		}
		profile, providerErr := service.provider.Password(operationContext, operation.AuthTarget(), password)
		if providerErr != nil {
			if operationContext.Err() != nil {
				return operationContext.Err()
			}
			return service.handleProviderFailure(operationContext, operation, providerErr)
		}
		operation.SetPendingProfile(profile)
		return service.finalize(operationContext, actor, operation, profile)
	})
	if errors.Is(operationErr, errRuntimeChallengeExpired) && result.Challenge != nil {
		return service.expireChallenge(ctx, *result.Challenge)
	}
	return result, mapChallengeError(operationErr)
}

// Cancel executes the same durable/runtime abort protocol as provider
// authorization failures. CompleteAbort is never reached when StopAccount
// fails.
func (service *Service) Cancel(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	if err := validateServiceContext(ctx, actor); err != nil {
		return err
	}
	if requestID == uuid.Nil {
		return ErrChallengeNotFound
	}
	result, err := service.coordinator.Operation(ctx, actor, requestID, func(operation *runtimeOperation) error {
		if abortErr := service.abort(ctx, operation.AuthTarget()); abortErr != nil {
			return abortErr
		}
		operation.Abort()
		return nil
	})
	if errors.Is(err, errRuntimeChallengeExpired) && result.Challenge != nil {
		if _, expireErr := service.expireChallenge(ctx, *result.Challenge); expireErr == ErrChallengeExpired {
			return nil
		} else {
			return expireErr
		}
	}
	if errors.Is(err, ErrAuthenticationAborted) {
		return nil
	}
	if errors.Is(err, errRuntimeChallengeNotFound) {
		return ErrChallengeNotFound
	}
	if err != nil {
		return err
	}
	return nil
}

// Status merges durable actor-owned accounts with the process-local challenge.
func (service *Service) Status(ctx context.Context, actor applicationroot.Actor) (Status, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Status{}, err
	}
	accounts, err := service.persistence.ListAccounts(ctx, actor)
	if err != nil {
		return Status{}, fmt.Errorf("list operator accounts: %w", err)
	}
	challenge, err := service.coordinator.Status(ctx, actor)
	if err != nil {
		if errors.Is(err, errRuntimeChallengeExpired) && challenge != nil {
			if _, expireErr := service.expireChallenge(ctx, *challenge); expireErr != ErrChallengeExpired {
				return Status{}, expireErr
			}
			accounts, err = service.persistence.ListAccounts(ctx, actor)
			if err != nil {
				return Status{}, fmt.Errorf("list operator accounts: %w", err)
			}
			challenge = nil
		} else {
			return Status{}, mapChallengeError(err)
		}
	}
	return Status{Accounts: append([]Account(nil), accounts...), Challenge: challenge}, nil
}

// Shutdown closes new starts, snapshots all local challenges, clears their
// process-local authentication state, strictly aborts each durable
// authentication, and finally clears tombstones. Provider and persistence
// calls happen outside coordinator locks.
func (service *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service.startGate.Lock()
	if !service.closed {
		service.closed = true
		service.coordinator.closeAdmission()
	}
	snapshot, snapshotErr := service.coordinator.snapshot(ctx)
	service.startGate.Unlock()

	failures := make([]error, 0, len(snapshot)+1)
	if snapshotErr != nil {
		failures = append(failures, snapshotErr)
	}
	for _, challenge := range snapshot {
		service.coordinator.clearShutdownState(challenge)
		if failure := service.abort(ctx, challenge.AuthTarget); failure != nil {
			failures = append(failures, failure)
			continue
		}
		if failure := service.coordinator.Remove(context.Background(), challenge.Actor, challenge.RequestID); failure != nil {
			failures = append(failures, mapChallengeError(failure))
		}
	}
	service.coordinator.clearTombstones()
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func (service *Service) handleProviderFailure(ctx context.Context, operation *runtimeOperation, providerErr error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	failure := classifyProviderFailure(providerErr, operation.Challenge().ExpiresAt, service.clock())
	if !isAbortProviderFailure(providerErr) {
		return failure
	}
	return service.abortOperation(ctx, operation, failure)
}

func (service *Service) abortOperation(ctx context.Context, operation *runtimeOperation, failure error) error {
	if abortErr := service.abort(ctx, operation.AuthTarget()); abortErr != nil {
		return errors.Join(failure, abortErr)
	}
	operation.Abort()
	return errors.Join(failure, ErrAuthenticationAborted)
}

func (service *Service) abortProviderFailure(ctx context.Context, target AuthTarget, failure error) (Result, error) {
	if abortErr := service.abort(ctx, target); abortErr != nil {
		return Result{}, errors.Join(failure, abortErr)
	}
	return Result{}, errors.Join(failure, ErrAuthenticationAborted)
}

func (service *Service) finalize(ctx context.Context, actor applicationroot.Actor, operation *runtimeOperation, profile Profile) error {
	challenge := operation.Challenge()
	if !service.clock().Before(challenge.ExpiresAt) {
		return ErrChallengeExpired
	}
	account, err := service.persistence.Finalize(ctx, actor, challenge.AccountID, challenge.Version, profile)
	if err != nil {
		return fmt.Errorf("finalize operator account authentication: %w", err)
	}
	operation.Complete(account)
	return nil
}

func (service *Service) expireChallenge(ctx context.Context, challenge Challenge) (Result, error) {
	if challenge.RequestID == uuid.Nil {
		return Result{}, ErrChallengeExpired
	}
	if err := service.abort(ctx, challenge.AuthTarget); err != nil {
		return Result{}, errors.Join(ErrChallengeExpired, err)
	}
	if err := service.coordinator.Remove(ctx, challenge.Actor, challenge.RequestID); err != nil {
		return Result{}, errors.Join(ErrChallengeExpired, mapChallengeError(err))
	}
	return Result{}, ErrChallengeExpired
}

func (service *Service) abort(ctx context.Context, target AuthTarget) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	version, err := service.persistence.BeginAbort(ctx, target.Actor, target.AccountID, target.Version)
	if err != nil {
		return fmt.Errorf("begin operator account abort: %w", err)
	}
	if version == 0 {
		return ErrAccountVersionConflict
	}
	if service.stopper == nil {
		return ErrProviderUnavailable
	}
	if err := service.stopper.StopAccount(ctx, target); err != nil {
		return fmt.Errorf("stop telegram account runtime: %w", err)
	}
	if err := service.persistence.CompleteAbort(ctx, target.Actor, target.AccountID, version); err != nil {
		return fmt.Errorf("complete operator account abort: %w", err)
	}
	return nil
}

func validateBeginResult(begin BeginResult) error {
	if err := begin.Validate(); err != nil {
		return err
	}
	if begin.Account.ID == uuid.Nil || begin.Account.Version == 0 {
		return ErrInvalidInput
	}
	switch begin.Outcome {
	case BeginStarted, BeginResumed, BeginInProgress, BeginAlreadyActive:
		return nil
	default:
		return ErrInvalidInput
	}
}

func validateTarget(target AuthTarget) error {
	if target.Actor.OperatorID == uuid.Nil || target.AccountID.IsZero() || target.Version == 0 ||
		(target.Status != operatoraccount.StatusAuthenticating && target.Status != operatoraccount.StatusDisconnecting) {
		return ErrInvalidInput
	}
	return nil
}

func validateServiceContext(ctx context.Context, actor applicationroot.Actor) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if actor.OperatorID == uuid.Nil {
		return ErrInvalidInput
	}
	return nil
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
		case character >= '0' && character <= '9':
			normalized.WriteRune(character)
		case unicode.IsSpace(character) || character == '-' || character == '(' || character == ')' || character == '.':
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
	if len(digits) < 7 || len(digits) > 15 || digits == "" || digits[0] == '0' {
		return "", ErrInvalidInput
	}
	return value, nil
}

func isAbortProviderFailure(err error) bool {
	return errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrSessionUnavailable) || errors.Is(err, ErrProviderUnavailable)
}

func classifyProviderFailure(err error, expiresAt, now time.Time) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if retry, ok := errors.AsType[*RetryAfterError](err); ok {
		remaining := expiresAt.Sub(now)
		if remaining <= 0 {
			return ErrChallengeExpired
		}
		after := min(retry.RetryAfter(), remaining)
		bounded, boundedErr := NewRetryAfterError(after)
		if boundedErr == nil {
			return bounded
		}
		return ErrFloodWait
	}
	var providerFailure *ProviderFailureError
	if errors.As(err, &providerFailure) && providerFailure != nil && validProviderFailureKind(providerFailure.Kind()) {
		return providerFailure
	}
	switch {
	case errors.Is(err, ErrInvalidCode):
		return ErrInvalidCode
	case errors.Is(err, ErrInvalidPassword):
		return ErrInvalidPassword
	case errors.Is(err, ErrPasswordRequired):
		return ErrPasswordRequired
	case errors.Is(err, ErrUnauthorized):
		return ErrUnauthorized
	case errors.Is(err, ErrSessionUnavailable):
		return ErrSessionUnavailable
	case errors.Is(err, ErrProviderUnavailable):
		return ErrProviderUnavailable
	case errors.Is(err, ErrProviderTransient):
		return ErrProviderTransient
	case errors.Is(err, ErrFloodWait):
		return ErrFloodWait
	default:
		return ErrProviderTransient
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

var (
	errRuntimeChallengeExpired       = errors.New("authentication challenge expired")
	errRuntimeChallengeNotFound      = errors.New("authentication challenge not found")
	errRuntimeChallengeAlreadyActive = errors.New("authentication challenge already active")
	errRuntimeAccountStateConflict   = errors.New("authentication account state conflict")
	errRuntimeCapacity               = errors.New("authentication challenge capacity reached")
	errRuntimeClosed                 = errors.New("authentication challenge coordinator closed")
)

// runtimeCoordinator is the application service's process-local phone
// challenge owner. It is kept in this package because the shared challenge
// projection package is intentionally transport-neutral and cannot depend
// back on this service without creating an import cycle.
type runtimeCoordinator struct {
	mu          sync.Mutex
	clock       func() time.Time
	newRequest  func() uuid.UUID
	ttl         time.Duration
	capacity    int
	challenges  map[uuid.UUID]*runtimeChallenge
	actorSlots  map[uuid.UUID]uuid.UUID
	targetSlots map[AuthTarget]uuid.UUID
	tombstones  map[uuid.UUID]runtimeTombstone
	closed      bool
}

type runtimeChallenge struct {
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

type runtimeTombstone struct {
	actorID       uuid.UUID
	expires       time.Time
	accountResult *Account
	aborted       bool
}

func (record *runtimeChallenge) expiry() time.Time {
	return record.expires.Load().(time.Time)
}

func (record *runtimeChallenge) cancelBusy() {
	record.operationCancel.Load().(context.CancelFunc)()
}

func newRuntimeCoordinator(clock func() time.Time, ttl time.Duration) *runtimeCoordinator {
	return &runtimeCoordinator{
		clock:       clock,
		newRequest:  uuid.New,
		ttl:         ttl,
		capacity:    maxLiveChallenges,
		challenges:  make(map[uuid.UUID]*runtimeChallenge),
		actorSlots:  make(map[uuid.UUID]uuid.UUID),
		targetSlots: make(map[AuthTarget]uuid.UUID),
		tombstones:  make(map[uuid.UUID]runtimeTombstone),
	}
}

func (coordinator *runtimeCoordinator) closeAdmission() {
	coordinator.mu.Lock()
	coordinator.closed = true
	coordinator.mu.Unlock()
}

func (coordinator *runtimeCoordinator) isClosed() bool {
	coordinator.mu.Lock()
	closed := coordinator.closed
	coordinator.mu.Unlock()
	return closed
}

func (coordinator *runtimeCoordinator) snapshot(ctx context.Context) ([]Challenge, error) {
	coordinator.mu.Lock()
	records := make([]*runtimeChallenge, 0, len(coordinator.challenges))
	for _, record := range coordinator.challenges {
		records = append(records, record)
	}
	coordinator.mu.Unlock()

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
		if coordinator.current(record.target.Actor, record.requestID, record) {
			result = append(result, coordinator.projection(record))
		}
		record.mu.Unlock()
	}
	return result, nil
}

func (coordinator *runtimeCoordinator) clearTombstones() {
	coordinator.mu.Lock()
	coordinator.tombstones = make(map[uuid.UUID]runtimeTombstone)
	coordinator.mu.Unlock()
}

func (coordinator *runtimeCoordinator) Reserve(
	ctx context.Context,
	target AuthTarget,
	phone string,
	expiresAt time.Time,
) (Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Challenge{}, err
	}
	if err := validateTarget(target); err != nil || target.Status != operatoraccount.StatusAuthenticating || strings.TrimSpace(phone) == "" || expiresAt.IsZero() {
		return Challenge{}, ErrInvalidInput
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeClosed
	}
	coordinator.cleanupLocked()
	if requestID, ok := coordinator.actorSlots[target.Actor.OperatorID]; ok {
		if record := coordinator.challenges[requestID]; record != nil {
			coordinator.mu.Unlock()
			record.mu.Lock()
			projection := coordinator.projection(record)
			expired := !coordinator.clock().Before(projection.ExpiresAt)
			record.mu.Unlock()
			if expired {
				return projection, errRuntimeChallengeExpired
			}
			return projection, errRuntimeChallengeAlreadyActive
		}
	}
	if requestID, ok := coordinator.targetSlots[target]; ok {
		if record := coordinator.challenges[requestID]; record != nil {
			coordinator.mu.Unlock()
			record.mu.Lock()
			projection := coordinator.projection(record)
			expired := !coordinator.clock().Before(projection.ExpiresAt)
			record.mu.Unlock()
			if expired {
				return projection, errRuntimeChallengeExpired
			}
			return projection, errRuntimeChallengeAlreadyActive
		}
	}
	if len(coordinator.challenges)+len(coordinator.tombstones) >= coordinator.capacity {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeCapacity
	}
	requestID := coordinator.nextRequestIDLocked()
	record := &runtimeChallenge{
		requestID: requestID,
		target:    target,
		phone:     phone,
		stage:     StageCode,
	}
	record.expires.Store(expiresAt)
	record.operationCancel.Store(context.CancelFunc(func() {}))
	coordinator.challenges[requestID] = record
	coordinator.actorSlots[target.Actor.OperatorID] = requestID
	coordinator.targetSlots[target] = requestID
	projection := coordinator.projection(record)
	coordinator.mu.Unlock()
	return projection, nil
}

func (coordinator *runtimeCoordinator) Attach(
	ctx context.Context,
	actor applicationroot.Actor,
	requestID uuid.UUID,
	sent SendCodeResult,
) (Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Challenge{}, err
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeClosed
	}
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeChallengeNotFound
	}
	coordinator.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(actor, requestID, record) {
		return Challenge{}, errRuntimeChallengeNotFound
	}
	if record.ready {
		return coordinator.projection(record), nil
	}
	if sent.PhoneCodeHash.IsZero() {
		return Challenge{}, ErrProviderUnavailable
	}
	if !coordinator.clock().Before(record.expiry()) {
		return coordinator.projection(record), errRuntimeChallengeExpired
	}
	if !sent.ExpiresAt.IsZero() && sent.ExpiresAt.Before(record.expiry()) {
		record.expires.Store(sent.ExpiresAt)
	}
	if !coordinator.clock().Before(record.expiry()) {
		return coordinator.projection(record), errRuntimeChallengeExpired
	}
	record.hash = sent.PhoneCodeHash
	record.delivery = strings.TrimSpace(sent.Delivery)
	if record.delivery == "" {
		record.delivery = "Telegram code"
	}
	record.ready = true
	return coordinator.projection(record), nil
}

func (coordinator *runtimeCoordinator) LimitExpiry(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, expiresAt time.Time) (Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Challenge{}, err
	}
	if expiresAt.IsZero() {
		return Challenge{}, ErrInvalidAuthenticationExpiry
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeClosed
	}
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeChallengeNotFound
	}
	coordinator.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(actor, requestID, record) {
		return Challenge{}, errRuntimeChallengeNotFound
	}
	if expiresAt.Before(record.expiry()) {
		record.expires.Store(expiresAt)
	}
	projection := coordinator.projection(record)
	if !coordinator.clock().Before(projection.ExpiresAt) {
		return projection, errRuntimeChallengeExpired
	}
	return projection, nil
}

func (coordinator *runtimeCoordinator) FindForTarget(ctx context.Context, target AuthTarget) (Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Challenge{}, err
	}
	if err := validateTarget(target); err != nil {
		return Challenge{}, ErrInvalidInput
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Challenge{}, errRuntimeClosed
	}
	coordinator.cleanupLocked()
	requestID, ok := coordinator.targetSlots[target]
	record := coordinator.challenges[requestID]
	coordinator.mu.Unlock()
	if !ok || record == nil {
		return Challenge{}, errRuntimeChallengeNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(target.Actor, requestID, record) {
		return Challenge{}, errRuntimeChallengeNotFound
	}
	if !coordinator.clock().Before(record.expiry()) {
		return coordinator.projection(record), errRuntimeChallengeExpired
	}
	return coordinator.projection(record), nil
}

func (coordinator *runtimeCoordinator) Status(ctx context.Context, actor applicationroot.Actor) (*Challenge, error) {
	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}
	if actor.OperatorID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, errRuntimeClosed
	}
	coordinator.cleanupLocked()
	requestID, ok := coordinator.actorSlots[actor.OperatorID]
	record := coordinator.challenges[requestID]
	coordinator.mu.Unlock()
	if !ok || record == nil {
		return nil, nil
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(actor, requestID, record) {
		return nil, nil
	}
	if !coordinator.clock().Before(record.expiry()) {
		projection := coordinator.projection(record)
		return &projection, errRuntimeChallengeExpired
	}
	projection := coordinator.projection(record)
	return &projection, nil
}

func (coordinator *runtimeCoordinator) Operation(
	ctx context.Context,
	actor applicationroot.Actor,
	requestID uuid.UUID,
	action func(*runtimeOperation) error,
) (Result, error) {
	if err := runtimeContextError(ctx); err != nil {
		return Result{}, err
	}
	if action == nil {
		return Result{}, ErrInvalidInput
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return Result{}, errRuntimeClosed
	}
	record := coordinator.challenges[requestID]
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		stone, hasStone := coordinator.tombstones[requestID]
		coordinator.mu.Unlock()
		if hasStone && stone.actorID == actor.OperatorID && coordinator.clock().Before(stone.expires) {
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
	coordinator.mu.Unlock()

	record.mu.Lock()
	defer record.mu.Unlock()
	operationContext, operationCancel := context.WithCancel(ctx)
	record.operationCancel.Store(operationCancel)
	defer func() {
		record.operationCancel.Store(context.CancelFunc(func() {}))
		operationCancel()
	}()
	if !coordinator.current(actor, requestID, record) {
		return Result{}, errRuntimeChallengeNotFound
	}
	if coordinator.isClosed() {
		return Result{}, errRuntimeClosed
	}
	if !coordinator.clock().Before(record.expiry()) {
		challenge := coordinator.projection(record)
		return Result{Challenge: &challenge}, errRuntimeChallengeExpired
	}
	if !record.ready && record.pendingProfile == nil {
		challenge := coordinator.projection(record)
		return Result{Challenge: &challenge}, ErrProviderUnavailable
	}
	operation := &runtimeOperation{coordinator: coordinator, record: record, ctx: operationContext}
	failure := action(operation)
	if operation.completed {
		account := operation.account
		return Result{Account: &account}, failure
	}
	if operation.aborted {
		return Result{}, failure
	}
	challenge := coordinator.projection(record)
	return Result{Challenge: &challenge}, failure
}

func (coordinator *runtimeCoordinator) Release(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	if err := runtimeContextError(ctx); err != nil {
		return err
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		coordinator.mu.Unlock()
		return errRuntimeChallengeNotFound
	}
	coordinator.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.ready {
		return errRuntimeAccountStateConflict
	}
	coordinator.remove(record)
	return nil
}

type runtimeOperation struct {
	coordinator *runtimeCoordinator
	record      *runtimeChallenge
	ctx         context.Context
	completed   bool
	aborted     bool
	account     Account
}

func (operation *runtimeOperation) Challenge() Challenge {
	return operation.coordinator.projection(operation.record)
}

func (operation *runtimeOperation) AuthTarget() AuthTarget { return operation.record.target }

func (operation *runtimeOperation) PhoneCodeHash() PhoneCodeHash { return operation.record.hash }

func (operation *runtimeOperation) Context() context.Context { return operation.ctx }

func (operation *runtimeOperation) PendingProfile() (Profile, bool) {
	if operation.record.pendingProfile == nil {
		return Profile{}, false
	}
	return *operation.record.pendingProfile, true
}

func (operation *runtimeOperation) ReserveCodeAttempt() bool {
	if operation.record.codeAttempts >= maxCodeAttempts {
		return false
	}
	operation.record.codeAttempts++
	return true
}

func (operation *runtimeOperation) ReservePasswordAttempt() bool {
	if operation.record.passwordAttempts >= maxPasswordAttempts {
		return false
	}
	operation.record.passwordAttempts++
	return true
}

func (operation *runtimeOperation) SetPendingProfile(profile Profile) {
	copy := profile
	operation.record.pendingProfile = &copy
	operation.record.hash = PhoneCodeHash{}
	operation.record.ready = false
}

func (operation *runtimeOperation) SetStage(stage Stage) error {
	if stage != StageCode && stage != StagePassword {
		return ErrInvalidInput
	}
	operation.record.stage = stage
	return nil
}

func (operation *runtimeOperation) Complete(account Account) {
	if operation.completed || operation.aborted {
		return
	}
	operation.coordinator.removeWithTombstone(operation.record, &account, false)
	operation.completed = true
	operation.account = account
}

func (operation *runtimeOperation) Abort() {
	if operation.completed || operation.aborted {
		return
	}
	operation.coordinator.removeWithTombstone(operation.record, nil, true)
	operation.aborted = true
}

func (coordinator *runtimeCoordinator) Remove(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID) error {
	if err := runtimeContextError(ctx); err != nil {
		return err
	}
	coordinator.mu.Lock()
	record := coordinator.challenges[requestID]
	if record == nil || record.target.Actor.OperatorID != actor.OperatorID {
		coordinator.mu.Unlock()
		return errRuntimeChallengeNotFound
	}
	coordinator.mu.Unlock()
	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(actor, requestID, record) {
		return errRuntimeChallengeNotFound
	}
	coordinator.remove(record)
	record.hash = PhoneCodeHash{}
	record.ready = false
	record.pendingProfile = nil
	return nil
}

func (coordinator *runtimeCoordinator) clearShutdownState(challenge Challenge) {
	// A failed abort remains addressable by target and request ID so a later
	// Shutdown can retry the durable lifecycle without retaining auth state.
	coordinator.mu.Lock()
	record := coordinator.challenges[challenge.RequestID]
	if record == nil || record.target != challenge.AuthTarget {
		coordinator.mu.Unlock()
		return
	}
	coordinator.mu.Unlock()

	record.mu.Lock()
	defer record.mu.Unlock()
	if !coordinator.current(challenge.Actor, challenge.RequestID, record) {
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

func (coordinator *runtimeCoordinator) current(actor applicationroot.Actor, requestID uuid.UUID, record *runtimeChallenge) bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.challenges[requestID] == record && record.target.Actor.OperatorID == actor.OperatorID
}

func (coordinator *runtimeCoordinator) remove(record *runtimeChallenge) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.removeLocked(record)
}

func (coordinator *runtimeCoordinator) removeWithTombstone(record *runtimeChallenge, account *Account, aborted bool) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if !coordinator.removeLocked(record) {
		return
	}
	stone := runtimeTombstone{
		actorID: record.target.Actor.OperatorID,
		expires: coordinator.clock().Add(coordinator.ttl),
		aborted: aborted,
	}
	if account != nil {
		copy := *account
		stone.accountResult = &copy
	}
	coordinator.tombstones[record.requestID] = stone
	// The caller owns record.mu. Clear the transient provider secret as soon
	// as the terminal state is recorded rather than retaining it until GC.
	record.hash = PhoneCodeHash{}
	record.ready = false
	record.pendingProfile = nil
}

func (coordinator *runtimeCoordinator) removeLocked(record *runtimeChallenge) bool {
	if coordinator.challenges[record.requestID] != record {
		return false
	}
	delete(coordinator.challenges, record.requestID)
	if coordinator.actorSlots[record.target.Actor.OperatorID] == record.requestID {
		delete(coordinator.actorSlots, record.target.Actor.OperatorID)
	}
	if coordinator.targetSlots[record.target] == record.requestID {
		delete(coordinator.targetSlots, record.target)
	}
	return true
}

func (coordinator *runtimeCoordinator) cleanupLocked() {
	for requestID, stone := range coordinator.tombstones {
		if !coordinator.clock().Before(stone.expires) {
			delete(coordinator.tombstones, requestID)
		}
	}
}

func (coordinator *runtimeCoordinator) nextRequestIDLocked() uuid.UUID {
	for {
		requestID := coordinator.newRequest()
		if requestID != uuid.Nil {
			if _, exists := coordinator.challenges[requestID]; !exists {
				if _, exists := coordinator.tombstones[requestID]; !exists {
					return requestID
				}
			}
		}
	}
}

func (coordinator *runtimeCoordinator) projection(record *runtimeChallenge) Challenge {
	return Challenge{
		RequestID:  record.requestID,
		AuthTarget: record.target,
		Phone:      record.phone,
		Delivery:   record.delivery,
		Stage:      record.stage,
		ExpiresAt:  record.expiry(),
	}
}

func runtimeContextError(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	return ctx.Err()
}
