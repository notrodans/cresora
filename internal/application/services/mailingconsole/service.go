package mailingconsole

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

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
	operatorID uuid.UUID
	console    Console
	// TODO: Убрать когда в будущем появится аутентификация пользователей на базе JWT/OAuth
	// Получать operatorMailing динамически
	operatorMailing OperatorMailings
}

// NewService creates an operator-scoped mailing console service.
func NewService(operatorID uuid.UUID, console Console, mailings Mailings) Service {
	validateServiceDependencies(operatorID, console, mailings)
	operatorMailing := mailings.OwnedBy(operatorID)
	if operatorMailing == nil {
		panic("create mailing console service without operator mailings")
	}
	return Service{
		operatorID:      operatorID,
		console:         console,
		operatorMailing: operatorMailing,
	}
}

// Dashboard returns the operator's accounts, sendable dialogs, and mailings.
func (service Service) Dashboard(context context.Context) (Dashboard, error) {
	service.validateContext(context)
	accounts, dialogs, privateDialogs, mailings, failure := service.console.Dashboard(context, service.operatorID)
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
func (service Service) ListAccounts(context context.Context) ([]Account, error) {
	service.validateContext(context)
	accounts, _, _, _, failure := service.console.Dashboard(context, service.operatorID)
	if failure != nil {
		return nil, fmt.Errorf("list mailing console accounts: %w", failure)
	}
	return accounts, nil
}

// Accounts is a short alias for ListAccounts.
func (service Service) Accounts(context context.Context) ([]Account, error) {
	return service.ListAccounts(context)
}

// ListSharedDialogs returns sendable shared dialogs for one selected account.
func (service Service) ListSharedDialogs(
	context context.Context,
	accountID uuid.UUID,
) ([]SharedDialog, error) {
	service.validateContext(context)
	if accountID == uuid.Nil {
		return nil, fmt.Errorf("%w: selected account is required", ErrInvalidInput)
	}
	_, dialogs, _, _, failure := service.console.Dashboard(context, service.operatorID)
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
	accountID uuid.UUID,
) ([]SharedDialog, error) {
	return service.ListSharedDialogs(context, accountID)
}

// ListMailings returns summaries for the configured operator.
func (service Service) ListMailings(context context.Context) ([]MailingSummary, error) {
	service.validateContext(context)
	_, _, _, mailings, failure := service.console.Dashboard(context, service.operatorID)
	if failure != nil {
		return nil, fmt.Errorf("list mailing console mailings: %w", failure)
	}
	return mailings, nil
}

// Mailings is a short alias for ListMailings.
func (service Service) Mailings(context context.Context) ([]MailingSummary, error) {
	return service.ListMailings(context)
}

// CreateDraft validates and persists a new operator-owned draft.
func (service Service) CreateDraft(
	context context.Context,
	input CreateDraftInput,
) (mailing.ID, error) {
	service.validateContext(context)
	validated, failure := validateCreateDraftInput(input)
	if failure != nil {
		return mailing.ID{}, failure
	}
	draft, failure := service.operatorMailing.CreateDraft(context, validated)
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("create mailing console draft: %w", failure)
	}
	return draft, nil
}

// VerifyOperator confirms that the configured operator exists before serving
// the console.
func (service Service) VerifyOperator(context context.Context) error {
	service.validateContext(context)
	exists, failure := service.console.OperatorExists(context, service.operatorID)
	if failure != nil {
		return fmt.Errorf("verify mailing console operator: %w", failure)
	}
	if !exists {
		return fmt.Errorf("%w: configured operator %s does not exist", ErrNotFound, service.operatorID)
	}
	return nil
}

// Queue queues one mailing through the operator-scoped mailing row.
func (service Service) Queue(context context.Context, mailingID uuid.UUID) error {
	service.validateContext(context)
	if mailingID == uuid.Nil {
		return fmt.Errorf("%w: mailing is required", ErrInvalidInput)
	}
	if failure := service.operatorMailing.Mailing(mailing.Identity(mailingID)).Queue(context); failure != nil {
		return fmt.Errorf("queue mailing console mailing %s: %w", mailingID, translateLifecycleFailure(failure))
	}
	return nil
}

func (service Service) validateContext(context context.Context) {
	if context == nil {
		panic("use mailing console service without context")
	}
	if service.operatorID == uuid.Nil {
		panic("use mailing console service without operator identity")
	}
	if service.console == nil {
		panic("use mailing console service without console projection")
	}
	if service.operatorMailing == nil {
		panic("use mailing console service without operator mailings")
	}
}

func validateServiceDependencies(operatorID uuid.UUID, console Console, mailings Mailings) {
	if operatorID == uuid.Nil {
		panic("create mailing console service without operator identity")
	}
	if console == nil {
		panic("create mailing console service without console projection")
	}
	if mailings == nil {
		panic("create mailing console service without mailing table")
	}
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
