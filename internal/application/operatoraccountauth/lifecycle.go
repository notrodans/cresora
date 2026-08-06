package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"
	"time"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// accountLifecycle owns the durable operator-account authentication protocol:
// BeginOrResume, Finalize, and the strict BeginAbort -> StopAccount ->
// CompleteAbort abort sequence. It is deliberately separate from the
// process-local challenge registry so the durable lifecycle can be understood
// and tested independently of in-memory challenge state.
type accountLifecycle struct {
	persistence AuthenticationPersistence
	stopper     RuntimeStopper
}

// Begin admits an authentication attempt durably and returns the authoritative
// stored auth expiry. Active accounts are a normal result, not an error.
func (lifecycle accountLifecycle) Begin(
	ctx context.Context,
	actor applicationroot.Actor,
	phone string,
	expiresAt time.Time,
) (BeginResult, error) {
	result, err := lifecycle.persistence.BeginOrResume(ctx, actor, phone, expiresAt)
	if err != nil {
		return BeginResult{}, fmt.Errorf("begin operator account authentication: %w", err)
	}
	if err := validateBeginResult(result); err != nil {
		return BeginResult{}, err
	}
	return result, nil
}

// Finalize activates the account for the exact version observed at admission.
func (lifecycle accountLifecycle) Finalize(
	ctx context.Context,
	target AuthTarget,
	profile Profile,
) (Account, error) {
	account, err := lifecycle.persistence.Finalize(ctx, target.Actor, target.AccountID, target.Version, profile)
	if err != nil {
		return Account{}, fmt.Errorf("finalize operator account authentication: %w", err)
	}
	return account, nil
}

// Abort executes the strict durable abort protocol. CompleteAbort is never
// reached when the runtime stop fails.
func (lifecycle accountLifecycle) Abort(ctx context.Context, target AuthTarget) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	version, err := lifecycle.persistence.BeginAbort(ctx, target.Actor, target.AccountID, target.Version)
	if err != nil {
		return fmt.Errorf("begin operator account abort: %w", err)
	}
	if version == 0 {
		return ErrAccountVersionConflict
	}
	if lifecycle.stopper == nil {
		return ErrProviderUnavailable
	}
	if err := lifecycle.stopper.StopAccount(ctx, target); err != nil {
		return fmt.Errorf("stop telegram account runtime: %w", err)
	}
	if err := lifecycle.persistence.CompleteAbort(ctx, target.Actor, target.AccountID, version); err != nil {
		return fmt.Errorf("complete operator account abort: %w", err)
	}
	return nil
}

// CompleteDisconnecting finishes an already-disconnecting candidate without
// a BeginAbort transition.
func (lifecycle accountLifecycle) CompleteDisconnecting(ctx context.Context, target AuthTarget) error {
	if lifecycle.stopper == nil {
		return ErrProviderUnavailable
	}
	if err := lifecycle.stopper.StopAccount(ctx, target); err != nil {
		return fmt.Errorf("stop orphaned telegram account runtime: %w", err)
	}
	if err := lifecycle.persistence.CompleteAbort(ctx, target.Actor, target.AccountID, target.Version); err != nil {
		return fmt.Errorf("complete orphaned operator account abort: %w", err)
	}
	return nil
}

// List returns only accounts owned by the actor.
func (lifecycle accountLifecycle) List(ctx context.Context, actor applicationroot.Actor) ([]Account, error) {
	return lifecycle.persistence.ListAccounts(ctx, actor)
}

// Recover completes durable authentication lifecycles which have no
// process-local runtime owner after startup. Authenticating candidates use the
// exact BeginAbort -> StopAccount -> CompleteAbort sequence; candidates already
// disconnecting skip BeginAbort and complete the remaining two steps.
func (lifecycle accountLifecycle) Recover(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	targets, err := lifecycle.persistence.ListOrphanAuthenticationLifecycles(ctx)
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
			failure = lifecycle.Abort(ctx, target)
		case operatoraccount.StatusDisconnecting:
			failure = lifecycle.CompleteDisconnecting(ctx, target)
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
