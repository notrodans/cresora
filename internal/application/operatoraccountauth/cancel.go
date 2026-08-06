package operatoraccountauth

import (
	"context"
	"errors"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
)

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
	result, err := service.registry.Operation(
		ctx,
		actor,
		requestID,
		func(operation *challengeOperation) error {
			if abortErr := service.abort(ctx, operation.AuthTarget()); abortErr != nil {
				return abortErr
			}
			operation.Abort()
			return nil
		},
	)
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
