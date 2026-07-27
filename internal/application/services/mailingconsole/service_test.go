package mailingconsole

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/notrodans/nebula-go/internal/domain/mailing"
)

type fakeConsole struct {
	dashboardAccounts []Account
	dashboardDialogs  []SharedDialog
	dashboardPrivate  []PrivateDialog
	dashboardMailings []MailingSummary
	dashboardFailure  error
	operatorExists    bool
	operatorFailure   error
	dashboardCalls    int
	dashboardOperator uuid.UUID
	operatorCalls     int
	operatorID        uuid.UUID
}

func (projection *fakeConsole) OperatorExists(_ context.Context, operatorID uuid.UUID) (bool, error) {
	projection.operatorCalls++
	projection.operatorID = operatorID
	return projection.operatorExists, projection.operatorFailure
}

func (projection *fakeConsole) Dashboard(
	_ context.Context,
	operatorID uuid.UUID,
) ([]Account, []SharedDialog, []PrivateDialog, []MailingSummary, error) {
	projection.dashboardCalls++
	projection.dashboardOperator = operatorID
	return projection.dashboardAccounts, projection.dashboardDialogs, projection.dashboardPrivate, projection.dashboardMailings, projection.dashboardFailure
}

type fakeMailings struct {
	scoped            *fakeOperatorMailings
	ownedByCalls      int
	ownedByOperatorID uuid.UUID
}

func (table *fakeMailings) OwnedBy(operatorID uuid.UUID) OperatorMailings {
	table.ownedByCalls++
	table.ownedByOperatorID = operatorID
	return table.scoped
}

type fakeOperatorMailings struct {
	row          *fakeMailing
	draftID      mailing.ID
	draftFailure error
	draftCalls   int
	input        CreateDraftInput
	mailingID    mailing.ID
	mailingCalls int
}

func (table *fakeOperatorMailings) CreateDraft(_ context.Context, input CreateDraftInput) (mailing.ID, error) {
	table.draftCalls++
	table.input = input
	return table.draftID, table.draftFailure
}

func (table *fakeOperatorMailings) Mailing(identity mailing.ID) mailing.Mailing {
	table.mailingCalls++
	table.mailingID = identity
	return table.row
}

type fakeMailing struct {
	queueFailure error
	queueCalls   int
}

func (row *fakeMailing) Queue(context.Context) error {
	row.queueCalls++
	return row.queueFailure
}

func (row *fakeMailing) Stop(context.Context) error {
	return nil
}

func newServiceFixture(operatorID uuid.UUID) (*fakeConsole, *fakeMailings, *fakeOperatorMailings, *fakeMailing, Service) {
	projection := &fakeConsole{}
	row := &fakeMailing{}
	scoped := &fakeOperatorMailings{row: row}
	table := &fakeMailings{scoped: scoped}
	service := NewService(operatorID, projection, table)
	return projection, table, scoped, row, service
}

func TestValidateCreateDraftInputTrimsText(t *testing.T) {
	input, failure := validateCreateDraftInput(CreateDraftInput{
		Name:            "  draft  ",
		MessageText:     "  message  ",
		AccountID:       uuid.New(),
		SharedDialogIDs: []uuid.UUID{uuid.New()},
	})
	if failure != nil {
		t.Fatalf("validate draft input: %v", failure)
	}
	if input.Name != "draft" || input.MessageText != "message" {
		t.Fatalf("expected trimmed input, got %#v", input)
	}
}

func TestValidateCreateDraftInputAcceptsPrivateTarget(t *testing.T) {
	input, failure := validateCreateDraftInput(CreateDraftInput{
		Name:        "名",
		MessageText: "message",
		AccountID:   uuid.New(),
		PrivateTargets: []PrivateTarget{{
			PeerType: PeerTypeChat,
			PeerID:   17,
		}},
	})
	if failure != nil {
		t.Fatalf("validate private draft input: %v", failure)
	}
	if len(input.PrivateTargets) != 1 {
		t.Fatalf("expected one private target, got %d", len(input.PrivateTargets))
	}
}

func TestValidateCreateDraftInputRejectsInvalidUTF8(t *testing.T) {
	_, failure := validateCreateDraftInput(CreateDraftInput{
		Name:            string([]byte{0xff}),
		MessageText:     "message",
		AccountID:       uuid.New(),
		SharedDialogIDs: []uuid.UUID{uuid.New()},
	})
	if !errors.Is(failure, ErrInvalidInput) {
		t.Fatalf("expected invalid UTF-8 error, got %v", failure)
	}
}

func TestValidateCreateDraftInputRejectsDuplicateSharedRecipient(t *testing.T) {
	dialogID := uuid.New()
	_, failure := validateCreateDraftInput(CreateDraftInput{
		Name:            "draft",
		MessageText:     "message",
		AccountID:       uuid.New(),
		SharedDialogIDs: []uuid.UUID{dialogID, dialogID},
	})
	if !errors.Is(failure, ErrInvalidInput) {
		t.Fatalf("expected invalid input error, got %v", failure)
	}
}

func TestServiceValidatesCreateDraftInput(t *testing.T) {
	validAccount := uuid.New()
	validRecipient := uuid.New()
	tests := []struct {
		name  string
		input CreateDraftInput
	}{
		{
			name: "empty name",
			input: CreateDraftInput{
				Name: "   ", MessageText: "message", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "empty message",
			input: CreateDraftInput{
				Name: "name", MessageText: "  ", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "message too long by runes",
			input: CreateDraftInput{
				Name: "name", MessageText: repeatedRune('界', 4097), AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "invalid UTF-8",
			input: CreateDraftInput{
				Name: string([]byte{0xff}), MessageText: "message", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "name too long by runes",
			input: CreateDraftInput{
				Name: repeatedRune('名', 256), MessageText: "message", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "zero account",
			input: CreateDraftInput{
				Name: "name", MessageText: "message", SharedDialogIDs: []uuid.UUID{validRecipient},
			},
		},
		{
			name: "empty recipients",
			input: CreateDraftInput{
				Name: "name", MessageText: "message", AccountID: validAccount,
			},
		},
		{
			name: "zero recipient",
			input: CreateDraftInput{
				Name: "name", MessageText: "message", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{uuid.Nil},
			},
		},
		{
			name: "duplicate recipients",
			input: CreateDraftInput{
				Name: "name", MessageText: "message", AccountID: validAccount, SharedDialogIDs: []uuid.UUID{validRecipient, validRecipient},
			},
		},
		{
			name: "duplicate private recipients",
			input: CreateDraftInput{
				Name: "name", MessageText: "message", AccountID: validAccount, PrivateTargets: []PrivateTarget{
					{PeerType: PeerTypeUser, PeerID: 42},
					{PeerType: PeerTypeUser, PeerID: 42},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, table, scoped, _, service := newServiceFixture(uuid.New())
			_, failure := service.CreateDraft(context.Background(), test.input)
			if !errors.Is(failure, ErrInvalidInput) {
				t.Fatalf("expected invalid input error, got %v", failure)
			}
			if table.ownedByCalls != 1 || scoped.draftCalls != 0 {
				t.Fatal("expected invalid input to stop before scoped draft creation")
			}
		})
	}
}

func TestServiceBindsOperatorAndCreatesDraftThroughScopedTable(t *testing.T) {
	operatorID := uuid.New()
	accountID := uuid.New()
	recipientID := uuid.New()
	draftID := mailing.Identity(uuid.New())
	_, table, scoped, _, service := newServiceFixture(operatorID)
	scoped.draftID = draftID

	actual, failure := service.CreateDraft(context.Background(), CreateDraftInput{
		Name:            "  draft name  ",
		MessageText:     "  сообщение  ",
		AccountID:       accountID,
		SharedDialogIDs: []uuid.UUID{recipientID},
	})
	if failure != nil {
		t.Fatalf("create draft: %v", failure)
	}
	if actual != draftID {
		t.Fatalf("expected draft %s, got %s", draftID.UUID(), actual.UUID())
	}
	if table.ownedByCalls != 1 || table.ownedByOperatorID != operatorID {
		t.Fatalf("expected OwnedBy(%s) once, got %d calls for %s", operatorID, table.ownedByCalls, table.ownedByOperatorID)
	}
	if scoped.draftCalls != 1 {
		t.Fatalf("expected one scoped draft call, got %d", scoped.draftCalls)
	}
	if scoped.input.Name != "draft name" || scoped.input.MessageText != "сообщение" {
		t.Fatalf("expected trimmed input, got %#v", scoped.input)
	}
}

func TestServiceDashboardUsesConsoleProjection(t *testing.T) {
	operatorID := uuid.New()
	projection, table, _, _, service := newServiceFixture(operatorID)
	projection.dashboardAccounts = []Account{{ID: uuid.New()}}
	projection.dashboardDialogs = []SharedDialog{{ID: uuid.New()}}
	projection.dashboardPrivate = []PrivateDialog{{AccountID: uuid.New()}}
	projection.dashboardMailings = []MailingSummary{{ID: uuid.New()}}

	dashboard, failure := service.Dashboard(context.Background())
	if failure != nil {
		t.Fatalf("load dashboard: %v", failure)
	}
	if table.ownedByCalls != 1 || projection.dashboardCalls != 1 || projection.dashboardOperator != operatorID {
		t.Fatalf("expected one operator-bound projection call, got OwnedBy %d and Dashboard %d for %s", table.ownedByCalls, projection.dashboardCalls, projection.dashboardOperator)
	}
	if len(dashboard.Accounts) != 1 || len(dashboard.SharedDialogs) != 1 || len(dashboard.PrivateDialogs) != 1 || len(dashboard.Mailings) != 1 {
		t.Fatalf("unexpected dashboard data: %#v", dashboard)
	}
}

func TestServicePreservesConsoleSentinels(t *testing.T) {
	operatorID := uuid.New()
	validInput := CreateDraftInput{
		Name: "draft", MessageText: "message", AccountID: uuid.New(), SharedDialogIDs: []uuid.UUID{uuid.New()},
	}

	t.Run("dashboard", func(t *testing.T) {
		projection, _, _, _, service := newServiceFixture(operatorID)
		projection.dashboardFailure = ErrNotFound
		_, failure := service.Dashboard(context.Background())
		if !errors.Is(failure, ErrNotFound) {
			t.Fatalf("expected not found sentinel, got %v", failure)
		}
	})
	t.Run("create draft", func(t *testing.T) {
		_, _, scoped, _, service := newServiceFixture(operatorID)
		scoped.draftFailure = ErrInvalidState
		_, failure := service.CreateDraft(context.Background(), validInput)
		if !errors.Is(failure, ErrInvalidState) {
			t.Fatalf("expected invalid state sentinel, got %v", failure)
		}
	})
	t.Run("queue domain failures", func(t *testing.T) {
		tests := []struct {
			name    string
			domain  error
			console error
		}{
			{name: "not found", domain: mailing.ErrNotFound, console: ErrNotFound},
			{name: "invalid state", domain: mailing.ErrInvalidState, console: ErrInvalidState},
			{name: "no eligible recipients", domain: mailing.ErrNoEligibleRecipients, console: ErrNoEligibleRecipients},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				_, _, scoped, row, service := newServiceFixture(operatorID)
				row.queueFailure = test.domain
				mailingID := uuid.New()
				failure := service.Queue(context.Background(), mailingID)
				if !errors.Is(failure, test.console) {
					t.Fatalf("expected console sentinel %v, got %v", test.console, failure)
				}
				if scoped.mailingCalls != 1 || scoped.mailingID.UUID() != mailingID || row.queueCalls != 1 {
					t.Fatalf("expected requested mailing row to be queued once, got row %s and %d calls", scoped.mailingID.UUID(), row.queueCalls)
				}
			})
		}
	})
}

func TestServiceQueueRejectsZeroMailing(t *testing.T) {
	_, _, scoped, row, service := newServiceFixture(uuid.New())
	if failure := service.Queue(context.Background(), uuid.Nil); !errors.Is(failure, ErrInvalidInput) {
		t.Fatalf("expected invalid input error, got %v", failure)
	}
	if scoped.mailingCalls != 0 || row.queueCalls != 0 {
		t.Fatal("expected zero mailing identity to stop before row selection")
	}
}

func TestServiceVerifiesConfiguredOperator(t *testing.T) {
	operatorID := uuid.New()
	projection, _, _, _, service := newServiceFixture(operatorID)
	projection.operatorExists = true
	if failure := service.VerifyOperator(context.Background()); failure != nil {
		t.Fatalf("verify existing operator: %v", failure)
	}
	if projection.operatorCalls != 1 || projection.operatorID != operatorID {
		t.Fatalf("expected operator verification for %s", operatorID)
	}
	projection.operatorExists = false
	if failure := service.VerifyOperator(context.Background()); !errors.Is(failure, ErrNotFound) {
		t.Fatalf("expected missing operator error, got %v", failure)
	}
	projection.operatorFailure = ErrInvalidState
	if failure := service.VerifyOperator(context.Background()); !errors.Is(failure, ErrInvalidState) {
		t.Fatalf("expected operator projection error, got %v", failure)
	}
}

func TestServiceGuardsContextAndCollaborators(t *testing.T) {
	operatorID := uuid.New()
	projection := &fakeConsole{}
	table := &fakeMailings{scoped: &fakeOperatorMailings{row: &fakeMailing{}}}
	assertPanics(t, func() {
		NewService(uuid.Nil, projection, table)
	})
	assertPanics(t, func() {
		NewService(operatorID, nil, table)
	})
	assertPanics(t, func() {
		NewService(operatorID, projection, nil)
	})

	service := NewService(operatorID, projection, table)
	assertPanics(t, func() {
		_, _ = service.Dashboard(nil)
	})
}

func repeatedRune(value rune, count int) string {
	result := make([]rune, count)
	for index := range result {
		result[index] = value
	}
	return string(result)
}

func assertPanics(t *testing.T, call func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	call()
}
