package operatoraccounts

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// ForceForgetOutcome identifies the result of a local force-forget command.
// These outcomes are intentionally separate from DisconnectResult: force
// forget is an operator override after remote logout could not be verified.
type ForceForgetOutcome string

const (
	// ForceForgetLocallyForgotten means the runtime was fenced and the local
	// account/session/audit transition committed.
	ForceForgetLocallyForgotten ForceForgetOutcome = "locally_forgotten"
	// ForceForgetAlreadyApplied means the idempotency key already has a durable
	// force-forget event. It does not advance the account version again.
	ForceForgetAlreadyApplied ForceForgetOutcome = "already_applied"
)

// ForceForgetResult is the result of one actor-scoped local force-forget.
type ForceForgetResult struct {
	Account operatoraccount.Account
	Outcome ForceForgetOutcome
}

// ForceForgetCommand contains the explicit acknowledgement and optimistic
// snapshot required by the local force-forget operation.
type ForceForgetCommand struct {
	Actor           application.Actor
	AccountID       operatoraccount.ID
	ExpectedVersion operatoraccount.Version
	Acknowledged    bool
	IdempotencyKey  uuid.UUID
}

const defaultForceForgetStopTimeout = 10 * time.Second

// ForceForgetOperatorAccount coordinates local runtime teardown and the
// dedicated atomic persistence operation. The runtime is stopped first and
// receives only the exact disconnecting snapshot loaded for this command.
type ForceForgetOperatorAccount struct {
	persistence ForceForgetPersistence
	runtime     RuntimeStopper
	stopTimeout time.Duration
}

// NewForceForgetOperatorAccount constructs the local force-forget service.
// Dependencies are application ports; no transport or database type crosses
// this boundary.
func NewForceForgetOperatorAccount(
	persistence ForceForgetPersistence,
	runtime RuntimeStopper,
) *ForceForgetOperatorAccount {
	return NewForceForgetOperatorAccountWithTimeout(persistence, runtime, defaultForceForgetStopTimeout)
}

// NewForceForgetOperatorAccountWithTimeout constructs the service with a
// bounded local runtime stop timeout. A non-positive timeout uses the safe
// default rather than allowing an unbounded stop operation.
func NewForceForgetOperatorAccountWithTimeout(
	persistence ForceForgetPersistence,
	runtime RuntimeStopper,
	stopTimeout time.Duration,
) *ForceForgetOperatorAccount {
	if stopTimeout <= 0 {
		stopTimeout = defaultForceForgetStopTimeout
	}
	return &ForceForgetOperatorAccount{
		persistence: persistence,
		runtime:     runtime,
		stopTimeout: stopTimeout,
	}
}

// Execute applies one explicitly acknowledged local force-forget command.
// Idempotency is checked before lifecycle admission so a retry can report its
// prior result without stopping the runtime or advancing persistence again.
func (service *ForceForgetOperatorAccount) Execute(
	ctx context.Context,
	command ForceForgetCommand,
) (ForceForgetResult, error) {
	if err := validateForceForgetCommand(ctx, command); err != nil {
		return ForceForgetResult{}, err
	}

	account, err := service.persistence.LoadAccount(ctx, command.Actor, command.AccountID)
	if err != nil {
		return ForceForgetResult{}, fmt.Errorf("load operator account for force forget: %w", err)
	}
	if account.ID() != command.AccountID {
		return ForceForgetResult{}, ErrAccountNotFound
	}

	applied, err := service.persistence.ForceForgetAlreadyApplied(
		ctx,
		command.Actor,
		command.AccountID,
		command.IdempotencyKey,
	)
	if err != nil {
		return ForceForgetResult{}, fmt.Errorf("check operator account force forget idempotency: %w", err)
	}
	if applied {
		return ForceForgetResult{
			Account: account,
			Outcome: ForceForgetAlreadyApplied,
		}, nil
	}

	if command.ExpectedVersion != account.Version() {
		return ForceForgetResult{}, ErrAccountVersionConflict
	}
	if account.Status() != operatoraccount.StatusDisconnecting || !account.RemoteLogoutRequired() {
		return ForceForgetResult{}, ErrAccountStateConflict
	}

	target := RuntimeTarget{
		Actor:     command.Actor,
		AccountID: account.ID(),
		Status:    account.Status(),
		Version:   account.Version(),
	}
	stopContext, cancel := context.WithTimeout(ctx, service.stopTimeout)
	stopFailure := service.runtime.StopAccount(stopContext, target)
	stopContextFailure := stopContext.Err()
	cancel()
	if stopFailure != nil {
		return ForceForgetResult{}, fmt.Errorf("stop local telegram account runtime: %w", stopFailure)
	}
	if stopContextFailure != nil {
		return ForceForgetResult{}, fmt.Errorf("stop local telegram account runtime: %w", stopContextFailure)
	}

	forgotten := account
	if err := forgotten.MarkDisconnected(); err != nil {
		return ForceForgetResult{}, fmt.Errorf("mark operator account locally forgotten: %w", err)
	}
	alreadyApplied, err := service.persistence.PersistForceForget(
		ctx,
		command.Actor,
		forgotten,
		command.ExpectedVersion,
		command.IdempotencyKey,
	)
	if err != nil {
		return ForceForgetResult{}, fmt.Errorf("persist operator account force forget: %w", err)
	}
	if alreadyApplied {
		return ForceForgetResult{
			Account: forgotten,
			Outcome: ForceForgetAlreadyApplied,
		}, nil
	}
	return ForceForgetResult{
		Account: forgotten,
		Outcome: ForceForgetLocallyForgotten,
	}, nil
}

func validateForceForgetCommand(ctx context.Context, command ForceForgetCommand) error {
	if ctx == nil || command.Actor.OperatorID == uuid.Nil || command.AccountID.IsZero() ||
		command.ExpectedVersion == 0 || command.IdempotencyKey == uuid.Nil {
		return ErrInvalidInput
	}
	if !command.Acknowledged {
		return fmt.Errorf("%w: force forget acknowledgement is required", ErrInvalidInput)
	}
	return ctx.Err()
}
