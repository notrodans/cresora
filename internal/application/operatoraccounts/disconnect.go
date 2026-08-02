package operatoraccounts

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// DisconnectOutcome identifies the bounded result of a disconnect command.
// Pending means that the durable remote logout intent remains and a later
// command or recovery pass may retry it.
type DisconnectOutcome string

const (
	// DisconnectCompleted means the remote runtime converged and the account was
	// durably marked disconnected.
	DisconnectCompleted DisconnectOutcome = "completed"
	// DisconnectAlreadyDisconnected means no work was needed. It is an idempotent
	// success and does not call the runtime.
	DisconnectAlreadyDisconnected DisconnectOutcome = "already_disconnected"
	// DisconnectPending means the account remains durably disconnecting with its
	// remote logout intent.
	DisconnectPending DisconnectOutcome = "pending"
	// DisconnectRejected means the command was rejected before a durable remote
	// logout intent was created.
	DisconnectRejected DisconnectOutcome = "rejected"
)

// DisconnectResult is the safe application result for one actor-scoped
// disconnect command. Account is the authoritative snapshot used by the
// command; when Outcome is DisconnectPending it is the durable intent that
// remains retryable.
type DisconnectResult struct {
	Account operatoraccount.Account
	Outcome DisconnectOutcome
}

// Service coordinates actor-scoped disconnect admission, the single remote
// revoke-and-stop operation, and the optimistic lifecycle completion write.
// Session deletion is deliberately delegated to the persistence adapter's
// final PersistLifecycle operation.
type Service struct {
	persistence DisconnectPersistence
	runtime     RuntimeRevoker
}

// NewService constructs the disconnect and recovery application service. The
// dependencies are application ports; no transport or database type crosses
// this boundary.
func NewService(persistence DisconnectPersistence, runtime RuntimeRevoker) *Service {
	return &Service{persistence: persistence, runtime: runtime}
}

// Disconnect requests an actor-owned account's disconnect. Active and
// reauthentication-required accounts first persist a remote logout intent at
// version N+1. A matching disconnecting remote intent resumes at its persisted
// version. Authenticating and local-only disconnecting accounts are outside
// this command's contract and are rejected without runtime work.
func (service *Service) Disconnect(
	ctx context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
) (DisconnectResult, error) {
	if err := validateDisconnectInput(ctx, actor, accountID); err != nil {
		return DisconnectResult{}, err
	}

	account, err := service.persistence.LoadAccount(ctx, actor, accountID)
	if err != nil {
		return DisconnectResult{}, fmt.Errorf("load operator account for disconnect: %w", err)
	}
	if account.ID() != accountID {
		return DisconnectResult{}, ErrAccountNotFound
	}

	switch account.Status() {
	case operatoraccount.StatusDisconnected:
		return DisconnectResult{
			Account: account,
			Outcome: DisconnectAlreadyDisconnected,
		}, nil
	case operatoraccount.StatusActive, operatoraccount.StatusReauthRequired:
		return service.beginDisconnect(ctx, actor, account)
	case operatoraccount.StatusDisconnecting:
		if !account.RemoteLogoutRequired() {
			return DisconnectResult{Account: account, Outcome: DisconnectRejected}, ErrAccountStateConflict
		}
		return service.revokeAndComplete(ctx, actor, account)
	case operatoraccount.StatusAuthenticating:
		return DisconnectResult{Account: account, Outcome: DisconnectRejected}, ErrAccountStateConflict
	default:
		return DisconnectResult{}, ErrInvalidInput
	}
}

func (service *Service) beginDisconnect(
	ctx context.Context,
	actor application.Actor,
	account operatoraccount.Account,
) (DisconnectResult, error) {
	expectedVersion := account.Version()
	if err := account.BeginDisconnect(); err != nil {
		return DisconnectResult{Account: account, Outcome: DisconnectRejected}, fmt.Errorf("begin operator account disconnect: %w", err)
	}
	if err := service.persistence.PersistLifecycle(ctx, actor, account, expectedVersion); err != nil {
		return DisconnectResult{}, fmt.Errorf("persist operator account disconnect intent: %w", err)
	}
	return service.revokeAndComplete(ctx, actor, account)
}

func (service *Service) revokeAndComplete(
	ctx context.Context,
	actor application.Actor,
	account operatoraccount.Account,
) (DisconnectResult, error) {
	target := RuntimeTarget{
		Actor:     actor,
		AccountID: account.ID(),
		Status:    account.Status(),
		Version:   account.Version(),
	}
	if err := validateRuntimeTarget(target); err != nil {
		return DisconnectResult{Account: account, Outcome: DisconnectPending}, err
	}
	outcome := service.runtime.RevokeAndStop(ctx, target)
	if err := outcome.Validate(); err != nil {
		return DisconnectResult{
			Account: account,
			Outcome: DisconnectPending,
		}, fmt.Errorf("revoke and stop telegram account: %w", err)
	}
	if failure, failed := outcome.Failure(); failed {
		return DisconnectResult{
			Account: account,
			Outcome: DisconnectPending,
		}, fmt.Errorf("revoke and stop telegram account: %w", nonConvergedRemoteFailure(failure))
	}

	pending := account
	if err := account.MarkDisconnected(); err != nil {
		return DisconnectResult{Account: pending, Outcome: DisconnectPending}, fmt.Errorf("mark operator account disconnected: %w", err)
	}
	if err := service.persistence.PersistLifecycle(ctx, actor, account, pending.Version()); err != nil {
		return DisconnectResult{Account: pending, Outcome: DisconnectPending}, fmt.Errorf("persist completed operator account disconnect: %w", err)
	}
	return DisconnectResult{Account: account, Outcome: DisconnectCompleted}, nil
}

func validateDisconnectInput(ctx context.Context, actor application.Actor, accountID operatoraccount.ID) error {
	if ctx == nil || actor.OperatorID == uuid.Nil || accountID.IsZero() {
		return ErrInvalidInput
	}
	return ctx.Err()
}

func validateRuntimeTarget(target RuntimeTarget) error {
	if target.Actor.OperatorID == uuid.Nil || target.AccountID.IsZero() || target.Version == 0 || target.Status != operatoraccount.StatusDisconnecting {
		return ErrInvalidInput
	}
	return nil
}

func nonConvergedRemoteFailure(failure *RemoteLogoutFailure) error {
	return fmt.Errorf("%w: %w", ErrRemoteLogoutNotConverged, failure)
}
