package operatoraccounts

import (
	"context"
	"errors"
	"fmt"

	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// RecoveryResult reports startup reconciliation without exposing provider
// details. Pending accounts encountered a safe, non-converged remote runtime
// result and remain durable disconnecting intents for a later retry.
type RecoveryResult struct {
	Attempted     int
	Completed     int
	Pending       int
	Skipped       int
	PendingByKind map[RemoteLogoutFailureKind]int
}

// Recover reconciles the persisted remote logout intents that have no
// process-local owner after startup. It validates each durable snapshot before
// making one runtime attempt, reloads after that attempt, and completes only
// the still-current target. Account-local remote non-convergence is returned
// as a nil error with Pending incremented; inability to enumerate or trust
// durable state returns ErrStartupRecovery.
func (service *Service) Recover(ctx context.Context) (RecoveryResult, error) {
	result := newRecoveryResult()
	if ctx == nil {
		return result, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	targets, err := service.persistence.ListRemoteLogoutIntents(ctx)
	if err != nil {
		return result, errors.Join(ErrStartupRecovery, fmt.Errorf("list remote logout intents: %w", err))
	}

	seen := make(map[operatoraccount.ID]struct{}, len(targets))
	failures := make([]error, 0, len(targets))
	for _, target := range targets {
		if _, exists := seen[target.AccountID]; exists {
			continue
		}
		seen[target.AccountID] = struct{}{}

		if err := service.recoverTarget(ctx, target, &result); err != nil {
			failures = append(failures, fmt.Errorf("recover operator account %s: %w", target.AccountID.UUID(), err))
		}
	}
	if len(failures) == 0 {
		return result, nil
	}
	joined := make([]error, 0, len(failures)+1)
	joined = append(joined, ErrStartupRecovery)
	joined = append(joined, failures...)
	return result, errors.Join(joined...)
}

func newRecoveryResult() RecoveryResult {
	return RecoveryResult{PendingByKind: make(map[RemoteLogoutFailureKind]int)}
}

func (service *Service) recoverTarget(ctx context.Context, target RuntimeTarget, result *RecoveryResult) error {
	if err := validateRuntimeTarget(target); err != nil {
		return fmt.Errorf("validate remote logout intent: %w", err)
	}

	account, err := service.persistence.LoadAccount(ctx, target.Actor, target.AccountID)
	if err != nil {
		return fmt.Errorf("load remote logout intent before runtime: %w", err)
	}
	if err := validateRecoverySnapshot(account, target); err != nil {
		return err
	}
	if !matchesRemoteLogoutTarget(account, target) {
		result.Skipped++
		return nil
	}

	result.Attempted++
	outcome := service.runtime.RevokeAndStop(ctx, target)
	if err := outcome.Validate(); err != nil {
		return fmt.Errorf("revoke and stop orphaned telegram account: %w", err)
	}
	failure, failed := outcome.Failure()

	account, err = service.persistence.LoadAccount(ctx, target.Actor, target.AccountID)
	if err != nil {
		return fmt.Errorf("reload remote logout intent after runtime: %w", err)
	}
	if err := validateRecoverySnapshot(account, target); err != nil {
		return err
	}
	if !matchesRemoteLogoutTarget(account, target) {
		if account.Status() == operatoraccount.StatusDisconnected {
			result.Completed++
		} else {
			result.Skipped++
		}
		return nil
	}

	if failed {
		result.Pending++
		result.PendingByKind[failure.Kind()]++
		return nil
	}

	pending := account
	if err := account.MarkDisconnected(); err != nil {
		return fmt.Errorf("mark recovered operator account disconnected: %w", err)
	}
	if err := service.persistence.PersistLifecycle(ctx, target.Actor, account, pending.Version()); err != nil {
		return fmt.Errorf("persist recovered operator account disconnect: %w", err)
	}
	result.Completed++
	return nil
}

func matchesRemoteLogoutTarget(account operatoraccount.Account, target RuntimeTarget) bool {
	return account.ID() == target.AccountID &&
		account.Status() == operatoraccount.StatusDisconnecting &&
		account.RemoteLogoutRequired() &&
		account.Version() == target.Version
}

func validateRecoverySnapshot(account operatoraccount.Account, target RuntimeTarget) error {
	if account.ID() != target.AccountID {
		return ErrAccountNotFound
	}
	if account.Version() == 0 {
		return ErrInvalidInput
	}
	switch account.Status() {
	case operatoraccount.StatusAuthenticating,
		operatoraccount.StatusActive,
		operatoraccount.StatusReauthRequired,
		operatoraccount.StatusDisconnected,
		operatoraccount.StatusDisconnecting:
		return nil
	default:
		return ErrInvalidInput
	}
}
