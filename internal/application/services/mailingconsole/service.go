package mailingconsole

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	application "github.com/notrodans/nebula-go/internal/application"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
)

var (
	ErrInvalidInput         = errors.New("invalid mailing console input")
	ErrNotFound             = errors.New("mailing console resource not found")
	ErrInvalidState         = errors.New("mailing console invalid state")
	ErrNoEligibleRecipients = errors.New("mailing has no eligible recipients")
)

// Console is the read-only mailing console projection used by Service.
type Console interface {
	OperatorExists(context.Context, uuid.UUID) (bool, error)
	Dashboard(context.Context, uuid.UUID) ([]Account, []SharedDialog, []PrivateDialog, []MailingSummary, error)
}

// OperatorMailings is the operator-scoped mailing table port used by Service.
type OperatorMailings interface {
	CreateDraft(context.Context, CreateDraftInput) (mailing.ID, error)
	Mailing(mailing.ID) mailing.Mailing
}

// Mailings is the root mailing table port used by Service.
type Mailings interface {
	OwnedBy(uuid.UUID) OperatorMailings
}

// Service provides transport-neutral mailing console operations.
type Service struct {
	console  Console
	mailings Mailings
}

// NewService creates a stateless mailing console service. Operator ownership
// is selected for every operation from its explicit actor.
func NewService(console Console, mailings Mailings) Service {
	validateServiceDependencies(console, mailings)
	return Service{
		console:  console,
		mailings: mailings,
	}
}

// Dashboard returns the operator's accounts, sendable dialogs, and mailings.
func (service Service) Dashboard(context context.Context, actor application.Actor) (Dashboard, error) {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return Dashboard{}, failure
	}
	accounts, dialogs, privateDialogs, mailings, failure := service.console.Dashboard(context, actor.OperatorID)
	if failure != nil {
		return Dashboard{}, fmt.Errorf("load mailing console dashboard: %w", failure)
	}
	return Dashboard{
		Accounts:       accounts,
		SharedDialogs:  dialogs,
		PrivateDialogs: privateDialogs,
		Mailings:       mailings,
	}, nil
}

// ListAccounts returns the operator's Telegram accounts.
func (service Service) ListAccounts(context context.Context, actor application.Actor) ([]Account, error) {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return nil, failure
	}
	accounts, _, _, _, failure := service.console.Dashboard(context, actor.OperatorID)
	if failure != nil {
		return nil, fmt.Errorf("list mailing console accounts: %w", failure)
	}
	return accounts, nil
}

// Accounts is a short alias for ListAccounts.
func (service Service) Accounts(context context.Context, actor application.Actor) ([]Account, error) {
	return service.ListAccounts(context, actor)
}

// ListSharedDialogs returns sendable shared dialogs for one selected account.
func (service Service) ListSharedDialogs(
	context context.Context,
	actor application.Actor,
	accountID uuid.UUID,
) ([]SharedDialog, error) {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return nil, failure
	}
	if accountID == uuid.Nil {
		return nil, fmt.Errorf("%w: selected account is required", ErrInvalidInput)
	}
	_, dialogs, _, _, failure := service.console.Dashboard(context, actor.OperatorID)
	if failure != nil {
		return nil, fmt.Errorf("list mailing console shared dialogs: %w", failure)
	}
	filtered := make([]SharedDialog, 0, len(dialogs))
	for _, dialog := range dialogs {
		if dialog.AccountID == accountID {
			filtered = append(filtered, dialog)
		}
	}
	return filtered, nil
}

// SharedDialogs is a short alias for ListSharedDialogs.
func (service Service) SharedDialogs(
	context context.Context,
	actor application.Actor,
	accountID uuid.UUID,
) ([]SharedDialog, error) {
	return service.ListSharedDialogs(context, actor, accountID)
}

// ListMailings returns summaries for the actor's operator.
func (service Service) ListMailings(context context.Context, actor application.Actor) ([]MailingSummary, error) {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return nil, failure
	}
	_, _, _, mailings, failure := service.console.Dashboard(context, actor.OperatorID)
	if failure != nil {
		return nil, fmt.Errorf("list mailing console mailings: %w", failure)
	}
	return mailings, nil
}

// Mailings is a short alias for ListMailings.
func (service Service) Mailings(context context.Context, actor application.Actor) ([]MailingSummary, error) {
	return service.ListMailings(context, actor)
}

// CreateDraft validates and persists a new operator-owned draft.
func (service Service) CreateDraft(
	context context.Context,
	actor application.Actor,
	input CreateDraftInput,
) (mailing.ID, error) {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return mailing.ID{}, failure
	}
	validated, failure := validateCreateDraftInput(input)
	if failure != nil {
		return mailing.ID{}, failure
	}
	operatorMailings, failure := service.operatorMailings(actor)
	if failure != nil {
		return mailing.ID{}, failure
	}
	draft, failure := operatorMailings.CreateDraft(context, validated)
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("create mailing console draft: %w", failure)
	}
	return draft, nil
}

// VerifyOperator confirms that the actor's operator exists before serving the
// console.
func (service Service) VerifyOperator(context context.Context, actor application.Actor) error {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return failure
	}
	exists, failure := service.console.OperatorExists(context, actor.OperatorID)
	if failure != nil {
		return fmt.Errorf("verify mailing console operator: %w", failure)
	}
	if !exists {
		return fmt.Errorf("%w: operator does not exist", ErrNotFound)
	}
	return nil
}

// Queue queues one mailing through the operator-scoped mailing row.
func (service Service) Queue(context context.Context, actor application.Actor, mailingID uuid.UUID) error {
	service.validateContext(context)
	if failure := validateActor(actor); failure != nil {
		return failure
	}
	if mailingID == uuid.Nil {
		return fmt.Errorf("%w: mailing is required", ErrInvalidInput)
	}
	operatorMailings, failure := service.operatorMailings(actor)
	if failure != nil {
		return failure
	}
	if failure = operatorMailings.Mailing(mailing.Identity(mailingID)).Queue(context); failure != nil {
		return fmt.Errorf("queue mailing console mailing %s: %w", mailingID, translateLifecycleFailure(failure))
	}
	return nil
}

func (service Service) validateContext(context context.Context) {
	if context == nil {
		panic("use mailing console service without context")
	}
	if service.console == nil {
		panic("use mailing console service without console projection")
	}
	if service.mailings == nil {
		panic("use mailing console service without mailing table")
	}
}

func validateServiceDependencies(console Console, mailings Mailings) {
	if console == nil {
		panic("create mailing console service without console projection")
	}
	if mailings == nil {
		panic("create mailing console service without mailing table")
	}
}

func validateActor(actor application.Actor) error {
	if actor.OperatorID == uuid.Nil {
		return fmt.Errorf("%w: operator identity is required", ErrInvalidInput)
	}
	return nil
}

func (service Service) operatorMailings(actor application.Actor) (OperatorMailings, error) {
	operatorMailings := service.mailings.OwnedBy(actor.OperatorID)
	if operatorMailings == nil {
		return nil, fmt.Errorf("%w: operator mailings are unavailable", ErrNotFound)
	}
	return operatorMailings, nil
}

func translateLifecycleFailure(failure error) error {
	switch {
	case errors.Is(failure, mailing.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, failure)
	case errors.Is(failure, mailing.ErrInvalidState):
		return fmt.Errorf("%w: %w", ErrInvalidState, failure)
	case errors.Is(failure, mailing.ErrNoEligibleRecipients):
		return fmt.Errorf("%w: %w", ErrNoEligibleRecipients, failure)
	default:
		return failure
	}
}

func validateCreateDraftInput(input CreateDraftInput) (CreateDraftInput, error) {
	if !utf8.ValidString(input.Name) || !utf8.ValidString(input.MessageText) {
		return CreateDraftInput{}, fmt.Errorf("%w: name and message must be valid UTF-8", ErrInvalidInput)
	}
	input.Name = strings.TrimSpace(input.Name)
	input.MessageText = strings.TrimSpace(input.MessageText)
	if input.Name == "" {
		return CreateDraftInput{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if utf8.RuneCountInString(input.Name) > 255 {
		return CreateDraftInput{}, fmt.Errorf("%w: name is too long", ErrInvalidInput)
	}
	messageLength := utf8.RuneCountInString(input.MessageText)
	if messageLength < 1 || messageLength > 4096 {
		return CreateDraftInput{}, fmt.Errorf("%w: message must contain 1 to 4096 Unicode code points", ErrInvalidInput)
	}
	if input.AccountID == uuid.Nil {
		return CreateDraftInput{}, fmt.Errorf("%w: selected account is required", ErrInvalidInput)
	}
	if len(input.SharedDialogIDs)+len(input.PrivateTargets) == 0 {
		return CreateDraftInput{}, fmt.Errorf("%w: at least one recipient is required", ErrInvalidInput)
	}
	seen := make(map[uuid.UUID]struct{}, len(input.SharedDialogIDs))
	for _, dialogID := range input.SharedDialogIDs {
		if dialogID == uuid.Nil {
			return CreateDraftInput{}, fmt.Errorf("%w: recipient identity is required", ErrInvalidInput)
		}
		if _, exists := seen[dialogID]; exists {
			return CreateDraftInput{}, fmt.Errorf("%w: duplicate recipient %s", ErrInvalidInput, dialogID)
		}
		seen[dialogID] = struct{}{}
	}
	privateSeen := make(map[PrivateTarget]struct{}, len(input.PrivateTargets))
	for _, target := range input.PrivateTargets {
		if target.PeerID == 0 || !validPeerType(target.PeerType) {
			return CreateDraftInput{}, fmt.Errorf("%w: invalid private recipient", ErrInvalidInput)
		}
		if _, exists := privateSeen[target]; exists {
			return CreateDraftInput{}, fmt.Errorf("%w: duplicate private recipient", ErrInvalidInput)
		}
		privateSeen[target] = struct{}{}
	}
	return input, nil
}

func validPeerType(peerType PeerType) bool {
	return peerType == PeerTypeUser || peerType == PeerTypeChat || peerType == PeerTypeChannel
}
