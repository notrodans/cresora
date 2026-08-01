package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// Recover completes durable authentication lifecycles which have no
// process-local runtime owner after startup. Authenticating candidates use the
// exact BeginAbort -> StopAccount -> CompleteAbort sequence; candidates already
// disconnecting skip BeginAbort and complete the remaining two steps.
func (service *Service) Recover(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	targets, err := service.persistence.ListOrphanAuthenticationLifecycles(ctx)
	if err != nil {
		return errors.Join(ErrStartupRecovery, fmt.Errorf("list orphan authentication lifecycles: %w", err))
	}
	var failures []error
	for _, target := range targets {
		if target.Actor.OperatorID == (applicationroot.Actor{}).OperatorID || target.AccountID.IsZero() || target.Version == 0 {
			failures = append(failures, ErrInvalidInput)
			continue
		}
		var failure error
		switch target.Status {
		case operatoraccount.StatusAuthenticating:
			failure = service.abort(ctx, target)
		case operatoraccount.StatusDisconnecting:
			failure = service.completeDisconnecting(ctx, target)
		default:
			failure = ErrInvalidInput
		}
		if failure != nil {
			failures = append(failures, failure)
		}
	}
	if len(failures) != 0 {
		joined := make([]error, 0, len(failures)+1)
		joined = append(joined, ErrStartupRecovery)
		joined = append(joined, failures...)
		return errors.Join(joined...)
	}
	return nil
}

func (service *Service) completeDisconnecting(ctx context.Context, target AuthTarget) error {
	if service.stopper == nil {
		return ErrProviderUnavailable
	}
	if err := service.stopper.StopAccount(ctx, target); err != nil {
		return fmt.Errorf("stop orphaned telegram account runtime: %w", err)
	}
	if err := service.persistence.CompleteAbort(ctx, target.Actor, target.AccountID, target.Version); err != nil {
		return fmt.Errorf("complete orphaned operator account abort: %w", err)
	}
	return nil
}
