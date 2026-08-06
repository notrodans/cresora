package operatoraccountauth

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
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
// itself; that secret remains in its private process-local registry.
type Service struct {
	persistence AuthenticationPersistence
	provider    PhoneProvider
	stopper     RuntimeStopper
	lifecycle   accountLifecycle
	registry    *challengeRegistry
	admission   admissionGate
	clock       func() time.Time
	ttl         time.Duration
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
		lifecycle: accountLifecycle{
			persistence: persistence,
			stopper:     stopper,
		},
		registry: newChallengeRegistry(config.Clock, config.ChallengeTTL),
		clock:    config.Clock,
		ttl:      config.ChallengeTTL,
	}
}

// Recover completes durable authentication lifecycles which have no
// process-local runtime owner after startup.
func (service *Service) Recover(ctx context.Context) error {
	return service.lifecycle.Recover(ctx)
}

// Shutdown closes new starts, snapshots all local challenges, clears their
// process-local authentication state, strictly aborts each durable
// authentication, and finally clears tombstones. Provider and persistence
// calls happen outside registry locks.
func (service *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service.admission.Close()
	if service.admission.closed {
		service.registry.closeAdmission()
	}
	snapshot, snapshotErr := service.registry.snapshot(ctx)

	failures := make([]error, 0, len(snapshot)+1)
	if snapshotErr != nil {
		failures = append(failures, snapshotErr)
	}
	for _, challenge := range snapshot {
		service.registry.clearShutdownState(challenge)
		if failure := service.abort(ctx, challenge.AuthTarget); failure != nil {
			failures = append(failures, failure)
			continue
		}
		if failure := service.registry.Remove(context.Background(), challenge.Actor, challenge.RequestID); failure != nil {
			failures = append(failures, mapChallengeError(failure))
		}
	}
	service.registry.clearTombstones()
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func (service *Service) expireChallenge(ctx context.Context, challenge Challenge) (Result, error) {
	if challenge.RequestID == uuid.Nil {
		return Result{}, ErrChallengeExpired
	}
	if err := service.abort(ctx, challenge.AuthTarget); err != nil {
		return Result{}, errors.Join(ErrChallengeExpired, err)
	}
	if err := service.registry.Remove(ctx, challenge.Actor, challenge.RequestID); err != nil {
		return Result{}, errors.Join(ErrChallengeExpired, mapChallengeError(err))
	}
	return Result{}, ErrChallengeExpired
}

func (service *Service) abort(ctx context.Context, target AuthTarget) error {
	return service.lifecycle.Abort(ctx, target)
}
