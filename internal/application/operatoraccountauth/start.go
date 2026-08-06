package operatoraccountauth

import (
	"context"
	"errors"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// Start admits an account durably before sending Telegram's phone code. An
// active account is returned immediately; an in-process admission resumes its
// existing challenge instead of creating a second provider session.
func (service *Service) Start(ctx context.Context, actor applicationroot.Actor, phone string) (Result, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Result{}, err
	}

	normalized, err := normalizePhone(phone)
	if err != nil {
		return Result{}, err
	}

	// One Start per actor may be in flight at a time: the durable admission
	// and challenge reservation below are serialized by the admission gate.
	admission, err := service.admission.Acquire(actor.OperatorID)
	if err != nil {
		return Result{}, err
	}
	defer admission.Release()

	begin, err := service.lifecycle.Begin(ctx, actor, normalized, service.clock().Add(service.ttl))
	if err != nil {
		return Result{}, err
	}
	if begin.Outcome == BeginAlreadyActive {
		account := begin.Account
		return Result{Account: &account}, nil
	}

	target := authTarget(actor, begin.Account)

	reservation, err := service.registry.Prepare(ctx, target, normalized, begin.AuthExpiresAt)
	if err != nil {
		return service.handlePrepareFailure(ctx, target, reservation, err)
	}
	if reservation.Existing() {
		challenge := reservation.Challenge()
		return Result{Challenge: &challenge}, nil
	}

	// Another Start of this actor cannot create a second challenge while the
	// reservation is held by the registry.
	admission.Release()

	return service.sendCode(ctx, target, normalized, reservation)
}

func (service *Service) sendCode(
	ctx context.Context,
	target AuthTarget,
	phone string,
	reservation challengeReservation,
) (Result, error) {
	if service.provider == nil {
		_ = reservation.Rollback(context.Background())
		return service.abortProviderFailure(ctx, target, ErrProviderUnavailable)
	}

	sent, err := service.provider.SendCode(ctx, target, phone)
	if err != nil {
		return service.handleSendCodeFailure(ctx, target, reservation, err)
	}

	challenge, err := reservation.Commit(ctx, sent)
	if err != nil {
		return service.handleAttachFailure(ctx, target, reservation, err)
	}
	return Result{Challenge: &challenge}, nil
}

func (service *Service) handlePrepareFailure(
	ctx context.Context,
	target AuthTarget,
	reservation challengeReservation,
	err error,
) (Result, error) {
	if errors.Is(err, errRuntimeChallengeExpired) {
		expireResult, expireErr := service.expireChallenge(ctx, reservation.Challenge())
		if expireErr == ErrChallengeExpired && reservation.Challenge().AuthTarget == target {
			return expireResult, expireErr
		}
		failures := []error{ErrChallengeExpired}
		if expireErr != nil && expireErr != ErrChallengeExpired {
			failures = append(failures, expireErr)
		}
		if abortErr := service.abort(ctx, target); abortErr != nil {
			failures = append(failures, abortErr)
		}
		return Result{}, errors.Join(failures...)
	}
	mapped := mapChallengeError(err)
	if abortErr := service.abort(ctx, target); abortErr != nil {
		return Result{}, errors.Join(mapped, abortErr)
	}
	return Result{}, mapped
}

func (service *Service) handleSendCodeFailure(
	ctx context.Context,
	target AuthTarget,
	reservation challengeReservation,
	err error,
) (Result, error) {
	challenge := reservation.Challenge()
	if !service.clock().Before(challenge.ExpiresAt) {
		return service.expireChallenge(ctx, challenge)
	}
	_ = reservation.Rollback(context.Background())
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	if isAbortProviderFailure(err) {
		return service.abortProviderFailure(ctx, target, classifyProviderFailure(err, challenge.ExpiresAt, service.clock()))
	}
	return Result{}, classifyProviderFailure(err, challenge.ExpiresAt, service.clock())
}

func (service *Service) handleAttachFailure(
	ctx context.Context,
	target AuthTarget,
	reservation challengeReservation,
	err error,
) (Result, error) {
	if errors.Is(err, errRuntimeChallengeExpired) {
		if _, expireErr := service.expireChallenge(ctx, reservation.Challenge()); expireErr != ErrChallengeExpired {
			return Result{}, expireErr
		}
		return Result{}, ErrChallengeExpired
	}
	if errors.Is(err, errRuntimeClosed) {
		if abortErr := service.abort(ctx, target); abortErr != nil {
			return Result{}, errors.Join(ErrServiceClosed, abortErr)
		}
		return Result{}, ErrServiceClosed
	}
	_ = reservation.Rollback(context.Background())
	return Result{}, mapChallengeError(err)
}

func authTarget(actor applicationroot.Actor, account Account) AuthTarget {
	return AuthTarget{
		Actor:     actor,
		AccountID: operatoraccount.Identity(account.ID),
		Status:    account.Status,
		Version:   account.Version,
	}
}
