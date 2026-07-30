// Package mock provides an in-memory, deterministic implementation of the
// operator account authentication CQS ports. It deliberately does not talk to
// Telegram or persist sessions.
package mock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	requests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
)

const (
	// MockPhoneCode is the code accepted by the phone verification mock.
	MockPhoneCode = "12345"

	phoneChallengeLifetime = 5 * time.Minute
	qrChallengeLifetime    = 2 * time.Minute
)

var (
	ErrInvalidInput      = errors.New("operator account auth mock invalid input")
	ErrChallengeNotFound = errors.New("operator account auth mock challenge not found")
	ErrChallengeExpired  = errors.New("operator account auth mock challenge expired")
	ErrInvalidCode       = errors.New("operator account auth mock invalid phone code")
)

var mockNamespace = uuid.MustParse("7e0e4f16-5b6c-4fd4-9ee9-11ca1ec8a001")

type phoneChallengeState struct {
	challenge application.PhoneChallenge
	code      string
}

type qrChallengeState struct {
	challenge application.QRChallenge
}

// Store is the thread-safe in-memory state shared by all mock CQS handlers.
type Store struct {
	mu sync.RWMutex

	operators map[uuid.UUID]*operatorState
}

type operatorState struct {
	accounts []application.Account
	phone    *phoneChallengeState
	qr       *qrChallengeState
	sequence uint64
}

// Application groups one mock implementation of every CQS port. Each field
// is a separate command object because the ports intentionally all use an
// Execute method with different arguments.
type Application struct {
	StartPhone  commands.StartPhone
	VerifyPhone commands.VerifyPhone
	StartQR     commands.StartQR
	RefreshQR   commands.RefreshQR
	Status      requests.Status
}

// Mock is an alias for Application for callers that want the implementation
// type to be explicit.
type Mock = Application

// New creates a mock application with deterministic operator account rows.
func New() *Application {
	store := NewStore()
	return &Application{
		StartPhone:  StartPhoneCommand{store: store},
		VerifyPhone: VerifyPhoneCommand{store: store},
		StartQR:     StartQRCommand{store: store},
		RefreshQR:   RefreshQRCommand{store: store},
		Status:      StatusRequest{store: store},
	}
}

// NewMock is an explicit alias for New for callers that prefer the
// implementation's role to be visible at the call site.
func NewMock() *Application {
	return New()
}

// NewStore creates empty challenge state with the fixed mock account rows.
// The store can be shared by independently constructed command handlers.
func NewStore() *Store {
	return &Store{
		operators: make(map[uuid.UUID]*operatorState),
	}
}

// NewStartPhone constructs a phone command. If no store is supplied, a new
// standalone mock store is created.
func NewStartPhone(stores ...*Store) commands.StartPhone {
	return StartPhoneCommand{store: selectStore(stores...)}
}

// NewVerifyPhone constructs a phone verification command. If no store is
// supplied, a new standalone mock store is created.
func NewVerifyPhone(stores ...*Store) commands.VerifyPhone {
	return VerifyPhoneCommand{store: selectStore(stores...)}
}

// NewStartQR constructs a QR command. If no store is supplied, a new
// standalone mock store is created.
func NewStartQR(stores ...*Store) commands.StartQR {
	return StartQRCommand{store: selectStore(stores...)}
}

// NewRefreshQR constructs a QR refresh command. If no store is supplied, a
// new standalone mock store is created.
func NewRefreshQR(stores ...*Store) commands.RefreshQR {
	return RefreshQRCommand{store: selectStore(stores...)}
}

// NewStatus constructs a status request. If no store is supplied, a new
// standalone mock store is created.
func NewStatus(stores ...*Store) requests.Status {
	return StatusRequest{store: selectStore(stores...)}
}

func selectStore(stores ...*Store) *Store {
	if len(stores) > 0 && stores[0] != nil {
		return stores[0]
	}
	return NewStore()
}

// StartPhoneCommand is the mock StartPhone command implementation.
type StartPhoneCommand struct {
	store *Store
}

// Execute creates a short-lived phone-code challenge. Formatting commonly
// used in phone input is normalized to an international +<digits> value.
func (command StartPhoneCommand) Execute(
	ctx context.Context,
	actor applicationroot.Actor,
	phone string,
) (application.PhoneChallenge, error) {
	if failure := validateContext(ctx); failure != nil {
		return application.PhoneChallenge{}, failure
	}
	normalized, failure := normalizePhone(phone)
	if failure != nil {
		return application.PhoneChallenge{}, failure
	}
	if command.store == nil || actor.OperatorID == uuid.Nil {
		return application.PhoneChallenge{}, fmt.Errorf("%w: mock store is required", ErrInvalidInput)
	}

	command.store.mu.Lock()
	defer command.store.mu.Unlock()
	state := command.store.stateLocked(actor)
	sequence := state.nextIDLocked("phone")

	challenge := application.PhoneChallenge{
		RequestID: uuid.NewSHA1(mockNamespace, []byte(actor.OperatorID.String()+":"+sequence)),
		Phone:     normalized,
		Delivery:  "Telegram SMS",
		ExpiresAt: time.Now().Add(phoneChallengeLifetime),
	}
	state.phone = &phoneChallengeState{
		challenge: challenge,
		code:      MockPhoneCode,
	}
	return challenge, nil
}

// VerifyPhoneCommand is the mock VerifyPhone command implementation.
type VerifyPhoneCommand struct {
	store *Store
}

// Execute accepts MockPhoneCode and returns the matching account row. A
// successful verification consumes the pending phone challenge.
func (command VerifyPhoneCommand) Execute(
	ctx context.Context,
	actor applicationroot.Actor,
	requestID uuid.UUID,
	code string,
) (application.Account, error) {
	if failure := validateContext(ctx); failure != nil {
		return application.Account{}, failure
	}
	if requestID == uuid.Nil {
		return application.Account{}, fmt.Errorf("%w: phone request ID is required", ErrInvalidInput)
	}
	if strings.TrimSpace(code) == "" {
		return application.Account{}, fmt.Errorf("%w: phone code is required", ErrInvalidInput)
	}
	if command.store == nil || actor.OperatorID == uuid.Nil {
		return application.Account{}, fmt.Errorf("%w: mock store is required", ErrInvalidInput)
	}

	command.store.mu.Lock()
	defer command.store.mu.Unlock()
	state := command.store.stateLocked(actor)

	if state.phone == nil || state.phone.challenge.RequestID != requestID {
		return application.Account{}, fmt.Errorf("%w: phone request %s", ErrChallengeNotFound, requestID)
	}
	if !time.Now().Before(state.phone.challenge.ExpiresAt) {
		state.phone = nil
		return application.Account{}, fmt.Errorf("%w: phone request %s", ErrChallengeExpired, requestID)
	}
	if strings.TrimSpace(code) != state.phone.code {
		return application.Account{}, fmt.Errorf("%w for phone request %s", ErrInvalidCode, requestID)
	}

	phone := state.phone.challenge.Phone
	state.phone = nil
	for _, account := range state.accounts {
		if account.Phone == phone {
			return account, nil
		}
	}

	account := application.Account{
		ID:                uuid.NewSHA1(mockNamespace, []byte("account:"+actor.OperatorID.String()+":"+phone)),
		Phone:             phone,
		TelegramUsername:  "new_mock_operator",
		TelegramFirstName: "Mock",
		TelegramLastName:  "Verified",
	}
	state.accounts = append(state.accounts, account)
	return account, nil
}

// StartQRCommand is the mock StartQR command implementation.
type StartQRCommand struct {
	store *Store
}

// Execute creates a short-lived mock Telegram deep-link challenge.
func (command StartQRCommand) Execute(ctx context.Context, actor applicationroot.Actor) (application.QRChallenge, error) {
	if failure := validateContext(ctx); failure != nil {
		return application.QRChallenge{}, failure
	}
	if command.store == nil || actor.OperatorID == uuid.Nil {
		return application.QRChallenge{}, fmt.Errorf("%w: mock store is required", ErrInvalidInput)
	}

	command.store.mu.Lock()
	defer command.store.mu.Unlock()
	state := command.store.stateLocked(actor)

	sequence := state.nextIDLocked("qr")
	challenge := application.QRChallenge{
		RequestID: uuid.NewSHA1(mockNamespace, []byte(actor.OperatorID.String()+":"+sequence)),
		URL:       mockQRURL(actor.OperatorID, sequence),
		ExpiresAt: time.Now().Add(qrChallengeLifetime),
	}
	state.qr = &qrChallengeState{challenge: challenge}
	return challenge, nil
}

// RefreshQRCommand is the mock RefreshQR command implementation.
type RefreshQRCommand struct {
	store *Store
}

// Execute replaces the token and expiry for the active QR challenge while
// retaining its request ID.
func (command RefreshQRCommand) Execute(
	ctx context.Context,
	actor applicationroot.Actor,
	requestID uuid.UUID,
) (application.QRChallenge, error) {
	if failure := validateContext(ctx); failure != nil {
		return application.QRChallenge{}, failure
	}
	if requestID == uuid.Nil {
		return application.QRChallenge{}, fmt.Errorf("%w: QR request ID is required", ErrInvalidInput)
	}
	if command.store == nil || actor.OperatorID == uuid.Nil {
		return application.QRChallenge{}, fmt.Errorf("%w: mock store is required", ErrInvalidInput)
	}

	command.store.mu.Lock()
	defer command.store.mu.Unlock()
	state := command.store.stateLocked(actor)

	if state.qr == nil || state.qr.challenge.RequestID != requestID {
		return application.QRChallenge{}, fmt.Errorf("%w: QR request %s", ErrChallengeNotFound, requestID)
	}
	if !time.Now().Before(state.qr.challenge.ExpiresAt) {
		state.qr = nil
		return application.QRChallenge{}, fmt.Errorf("%w: QR request %s", ErrChallengeExpired, requestID)
	}

	sequence := state.nextIDLocked("qr-refresh")
	refreshed := application.QRChallenge{
		RequestID: requestID,
		URL:       mockQRURL(actor.OperatorID, sequence),
		ExpiresAt: time.Now().Add(qrChallengeLifetime),
	}
	state.qr.challenge = refreshed
	return refreshed, nil
}

// StatusRequest is the mock Status request implementation.
type StatusRequest struct {
	store *Store
}

// Execute returns a race-free snapshot of account and challenge state.
func (request StatusRequest) Execute(ctx context.Context, actor applicationroot.Actor) (application.Status, error) {
	if failure := validateContext(ctx); failure != nil {
		return application.Status{}, failure
	}
	if request.store == nil || actor.OperatorID == uuid.Nil {
		return application.Status{}, fmt.Errorf("%w: mock store is required", ErrInvalidInput)
	}

	request.store.mu.Lock()
	defer request.store.mu.Unlock()
	state := request.store.stateLocked(actor)
	request.store.expireChallengesLocked(time.Now())

	status := application.Status{
		Accounts: append([]application.Account(nil), state.accounts...),
	}
	if state.phone != nil {
		challenge := state.phone.challenge
		status.PhoneChallenge = &challenge
	}
	if state.qr != nil {
		challenge := state.qr.challenge
		status.QRChallenge = &challenge
	}
	return status, nil
}

func (store *Store) stateLocked(actor applicationroot.Actor) *operatorState {
	if store.operators == nil {
		store.operators = make(map[uuid.UUID]*operatorState)
	}
	state := store.operators[actor.OperatorID]
	if state == nil {
		state = &operatorState{accounts: defaultAccounts(actor.OperatorID)}
		store.operators[actor.OperatorID] = state
	}
	return state
}

func defaultAccounts(actorID uuid.UUID) []application.Account {
	return []application.Account{
		{
			ID:                fixtureAccountID(actorID, 1),
			Phone:             "+15551234567",
			TelegramUsername:  "mock_operator",
			TelegramFirstName: "Mock",
			TelegramLastName:  "Operator",
		},
		{
			ID:                fixtureAccountID(actorID, 2),
			Phone:             "+15550001111",
			TelegramUsername:  "backup_operator",
			TelegramFirstName: "Backup",
			TelegramLastName:  "Account",
		},
	}
}

func fixtureAccountID(actorID uuid.UUID, slot int) uuid.UUID {
	return uuid.NewSHA1(mockNamespace, []byte("fixture-account:"+actorID.String()+":"+fmt.Sprint(slot)))
}

func (state *operatorState) nextIDLocked(kind string) string {
	state.sequence++
	return fmt.Sprintf("%s:%d", kind, state.sequence)
}

func (store *Store) expireChallengesLocked(now time.Time) {
	for _, state := range store.operators {
		if state.phone != nil && !now.Before(state.phone.challenge.ExpiresAt) {
			state.phone = nil
		}
		if state.qr != nil && !now.Before(state.qr.challenge.ExpiresAt) {
			state.qr = nil
		}
	}
}

func mockQRURL(actorID uuid.UUID, sequence string) string {
	return "tg://login?token=mock-" + actorID.String() + "-" + strings.ReplaceAll(sequence, ":", "-")
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidInput)
	}
	if failure := ctx.Err(); failure != nil {
		return failure
	}
	return nil
}

func normalizePhone(phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", fmt.Errorf("%w: phone is required", ErrInvalidInput)
	}

	var normalized strings.Builder
	for index, character := range phone {
		switch {
		case character == '+' && index == 0:
			normalized.WriteRune(character)
		case unicode.IsDigit(character):
			normalized.WriteRune(character)
		case character == ' ' || character == '-' || character == '(' || character == ')' || character == '.':
			// Ignore common display formatting.
		default:
			return "", fmt.Errorf("%w: phone contains unsupported character %q", ErrInvalidInput, character)
		}
	}

	value := normalized.String()
	if after, ok := strings.CutPrefix(value, "00"); ok {
		value = "+" + after
	}
	if !strings.HasPrefix(value, "+") {
		value = "+" + value
	}
	digits := strings.TrimPrefix(value, "+")
	if len(digits) < 7 || len(digits) > 15 || digits[0] == '0' {
		return "", fmt.Errorf("%w: phone must contain 7 to 15 international digits", ErrInvalidInput)
	}
	return value, nil
}

var (
	_ commands.StartPhone  = StartPhoneCommand{}
	_ commands.VerifyPhone = VerifyPhoneCommand{}
	_ commands.StartQR     = StartQRCommand{}
	_ commands.RefreshQR   = RefreshQRCommand{}
	_ requests.Status      = StatusRequest{}
)
