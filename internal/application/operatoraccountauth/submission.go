package operatoraccountauth

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
)

// Code submits a phone code using only the hash retained by the registry.
// Provider RPC and finalization run under the challenge's per-record mutex.
func (service *Service) Code(ctx context.Context, actor applicationroot.Actor, requestID uuid.UUID, code string) (Result, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Result{}, err
	}
	if requestID == uuid.Nil || strings.TrimSpace(code) == "" {
		return Result{}, ErrInvalidInput
	}
	result, operationErr := service.registry.Operation(
		ctx,
		actor,
		requestID,
		func(operation *challengeOperation) error {
			challenge := operation.Challenge()
			if pending, ok := operation.PendingProfile(); ok {
				return service.finalize(ctx, operation, pending)
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
			profile, providerErr := service.provider.SignIn(
				operationContext,
				operation.AuthTarget(),
				challenge.Phone,
				strings.TrimSpace(code),
				operation.PhoneCodeHash(),
			)
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
			return service.finalize(operationContext, operation, profile)
		},
	)
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
	result, operationErr := service.registry.Operation(
		ctx,
		actor,
		requestID,
		func(operation *challengeOperation) error {
			challenge := operation.Challenge()
			if pending, ok := operation.PendingProfile(); ok {
				return service.finalize(ctx, operation, pending)
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
			return service.finalize(operationContext, operation, profile)
		},
	)
	if errors.Is(operationErr, errRuntimeChallengeExpired) && result.Challenge != nil {
		return service.expireChallenge(ctx, *result.Challenge)
	}
	return result, mapChallengeError(operationErr)
}

func (service *Service) finalize(ctx context.Context, operation *challengeOperation, profile Profile) error {
	challenge := operation.Challenge()
	if !service.clock().Before(challenge.ExpiresAt) {
		return ErrChallengeExpired
	}
	account, err := service.lifecycle.Finalize(ctx, operation.AuthTarget(), profile)
	if err != nil {
		return err
	}
	operation.Complete(account)
	return nil
}

func (service *Service) handleProviderFailure(ctx context.Context, operation *challengeOperation, providerErr error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	failure := classifyProviderFailure(providerErr, operation.Challenge().ExpiresAt, service.clock())
	if !isAbortProviderFailure(providerErr) {
		return failure
	}
	return service.abortOperation(ctx, operation, failure)
}

func (service *Service) abortOperation(ctx context.Context, operation *challengeOperation, failure error) error {
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
