package mailingconsole

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/mailing"
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

type multiOperatorConsole struct {
	dashboards map[uuid.UUID]Dashboard
}

func (projection *multiOperatorConsole) OperatorExists(_ context.Context, operatorID uuid.UUID) (bool, error) {
	_, exists := projection.dashboards[operatorID]
	return exists, nil
}

func (projection *multiOperatorConsole) Dashboard(_ context.Context, operatorID uuid.UUID) ([]Account, []SharedDialog, []PrivateDialog, []MailingSummary, error) {
	dashboard, exists := projection.dashboards[operatorID]
	if !exists {
		return nil, nil, nil, nil, ErrNotFound
	}
	return dashboard.Accounts, dashboard.SharedDialogs, dashboard.PrivateDialogs, dashboard.Mailings, nil
}

type multiOperatorMailings struct {
	operators map[uuid.UUID]*multiOperatorMailing
}

type multiOperatorMailing struct {
	accountID uuid.UUID
	mailingID uuid.UUID
	row       *fakeMailing
}

func (table *multiOperatorMailings) OwnedBy(operatorID uuid.UUID) OperatorMailings {
	operator := table.operators[operatorID]
	if operator == nil {
		return nil
	}
	return operatorMailingsFixture{operator: operator}
}

type operatorMailingsFixture struct {
	operator *multiOperatorMailing
}

func (table operatorMailingsFixture) CreateDraft(_ context.Context, input CreateDraftInput) (mailing.ID, error) {
	if input.AccountID != table.operator.accountID {
		return mailing.ID{}, ErrNotFound
	}
	return mailing.Identity(table.operator.mailingID), nil
}

func (table operatorMailingsFixture) Mailing(identity mailing.ID) mailing.Mailing {
	if identity.UUID() != table.operator.mailingID {
		return &fakeMailing{queueFailure: mailing.ErrNotFound}
	}
	return table.operator.row
}

func newServiceFixture(operatorID uuid.UUID) (*fakeConsole, *fakeMailings, *fakeOperatorMailings, *fakeMailing, Service) {
	projection := &fakeConsole{}
	row := &fakeMailing{}
	scoped := &fakeOperatorMailings{row: row}
	table := &fakeMailings{scoped: scoped}
	service := NewService(projection, table)
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
			operatorID := uuid.New()
			_, table, scoped, _, service := newServiceFixture(operatorID)
			_, failure := service.CreateDraft(context.Background(), application.Actor{OperatorID: operatorID}, test.input)
			if !errors.Is(failure, ErrInvalidInput) {
				t.Fatalf("expected invalid input error, got %v", failure)
			}
			if table.ownedByCalls != 0 || scoped.draftCalls != 0 {
				t.Fatal("expected invalid input to stop before scoped table selection")
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

	actual, failure := service.CreateDraft(context.Background(), application.Actor{OperatorID: operatorID}, CreateDraftInput{
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
		t.Fatalf("expected OwnedBy(%s) once per draft operation, got %d calls for %s", operatorID, table.ownedByCalls, table.ownedByOperatorID)
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

	dashboard, failure := service.Dashboard(context.Background(), application.Actor{OperatorID: operatorID})
	if failure != nil {
		t.Fatalf("load dashboard: %v", failure)
	}
	if table.ownedByCalls != 0 || projection.dashboardCalls != 1 || projection.dashboardOperator != operatorID {
		t.Fatalf("expected one operator-bound projection call without mailing selection, got OwnedBy %d and Dashboard %d for %s", table.ownedByCalls, projection.dashboardCalls, projection.dashboardOperator)
	}
	if len(dashboard.Accounts) != 1 || len(dashboard.SharedDialogs) != 1 || len(dashboard.PrivateDialogs) != 1 || len(dashboard.Mailings) != 1 {
		t.Fatalf("unexpected dashboard data: %#v", dashboard)
	}
}

func TestServiceIsolatesTwoActorsAcrossConsoleOperations(t *testing.T) {
	operatorA, operatorB := uuid.New(), uuid.New()
	accountA, accountB := uuid.New(), uuid.New()
	draftA, draftB := uuid.New(), uuid.New()
	consoleProjection := &multiOperatorConsole{dashboards: map[uuid.UUID]Dashboard{
		operatorA: {Accounts: []Account{{ID: accountA}}, Mailings: []MailingSummary{{ID: draftA}}},
		operatorB: {Accounts: []Account{{ID: accountB}}, Mailings: []MailingSummary{{ID: draftB}}},
	}}
	mailingTable := &multiOperatorMailings{operators: map[uuid.UUID]*multiOperatorMailing{
		operatorA: {accountID: accountA, mailingID: draftA, row: &fakeMailing{}},
		operatorB: {accountID: accountB, mailingID: draftB, row: &fakeMailing{}},
	}}
	service := NewService(consoleProjection, mailingTable)
	actorA := application.Actor{OperatorID: operatorA}
	actorB := application.Actor{OperatorID: operatorB}

	dashboardA, failure := service.Dashboard(context.Background(), actorA)
	if failure != nil || len(dashboardA.Accounts) != 1 || dashboardA.Accounts[0].ID != accountA {
		t.Fatalf("operator A dashboard: %v %#v", failure, dashboardA)
	}
	dashboardB, failure := service.Dashboard(context.Background(), actorB)
	if failure != nil || len(dashboardB.Accounts) != 1 || dashboardB.Accounts[0].ID != accountB {
		t.Fatalf("operator B dashboard: %v %#v", failure, dashboardB)
	}

	validDraft := CreateDraftInput{Name: "draft", MessageText: "message", AccountID: accountA, SharedDialogIDs: []uuid.UUID{uuid.New()}}
	if _, failure = service.CreateDraft(context.Background(), actorA, validDraft); failure != nil {
		t.Fatalf("operator A create draft: %v", failure)
	}
	validDraft.AccountID = accountB
	if _, failure = service.CreateDraft(context.Background(), actorA, validDraft); !errors.Is(failure, ErrNotFound) {
		t.Fatalf("foreign account create: expected not found, got %v", failure)
	}
	if failure = service.Queue(context.Background(), actorA, draftB); !errors.Is(failure, ErrNotFound) {
		t.Fatalf("foreign queue: expected not found, got %v", failure)
	}
	randomDraft := uuid.New()
	if randomFailure := service.Queue(context.Background(), actorA, randomDraft); !errors.Is(randomFailure, ErrNotFound) {
		t.Fatalf("random queue: expected not found, got %v", randomFailure)
	}
	if failure = service.Queue(context.Background(), actorB, draftB); failure != nil {
		t.Fatalf("operator B queue: %v", failure)
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
		_, failure := service.Dashboard(context.Background(), application.Actor{OperatorID: operatorID})
		if !errors.Is(failure, ErrNotFound) {
			t.Fatalf("expected not found sentinel, got %v", failure)
		}
	})
	t.Run("create draft", func(t *testing.T) {
		_, _, scoped, _, service := newServiceFixture(operatorID)
		scoped.draftFailure = ErrInvalidState
		_, failure := service.CreateDraft(context.Background(), application.Actor{OperatorID: operatorID}, validInput)
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
				failure := service.Queue(context.Background(), application.Actor{OperatorID: operatorID}, mailingID)
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
	if failure := service.Queue(context.Background(), application.Actor{OperatorID: uuid.New()}, uuid.Nil); !errors.Is(failure, ErrInvalidInput) {
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
	if failure := service.VerifyOperator(context.Background(), application.Actor{OperatorID: operatorID}); failure != nil {
		t.Fatalf("verify existing operator: %v", failure)
	}
	if projection.operatorCalls != 1 || projection.operatorID != operatorID {
		t.Fatalf("expected operator verification for %s", operatorID)
	}
	projection.operatorExists = false
	if failure := service.VerifyOperator(context.Background(), application.Actor{OperatorID: operatorID}); !errors.Is(failure, ErrNotFound) {
		t.Fatalf("expected missing operator error, got %v", failure)
	}
	projection.operatorFailure = ErrInvalidState
	if failure := service.VerifyOperator(context.Background(), application.Actor{OperatorID: operatorID}); !errors.Is(failure, ErrInvalidState) {
		t.Fatalf("expected operator projection error, got %v", failure)
	}
}

func TestServiceGuardsContextAndCollaborators(t *testing.T) {
	operatorID := uuid.New()
	projection := &fakeConsole{}
	table := &fakeMailings{scoped: &fakeOperatorMailings{row: &fakeMailing{}}}
	assertPanics(t, func() {
		NewService(projection, nil)
	})

	service := NewService(projection, table)
	assertPanics(t, func() {
		_, _ = service.Dashboard(nil, application.Actor{OperatorID: operatorID})
	})
	if _, failure := service.Dashboard(context.Background(), application.Actor{}); !errors.Is(failure, ErrInvalidInput) {
		t.Fatalf("expected missing actor to be rejected, got %v", failure)
	}
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
