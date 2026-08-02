package operatoraccountauth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

type phoneProviderProbe struct{}

func (phoneProviderProbe) SendCode(context.Context, AuthTarget, string) (SendCodeResult, error) {
	return SendCodeResult{}, nil
}

func (phoneProviderProbe) SignIn(context.Context, AuthTarget, string, string, PhoneCodeHash) (Profile, error) {
	return Profile{}, nil
}

func (phoneProviderProbe) Password(context.Context, AuthTarget, string) (Profile, error) {
	return Profile{}, nil
}

var _ PhoneProvider = phoneProviderProbe{}

type persistenceProbe struct{}

func (persistenceProbe) BeginOrResume(context.Context, applicationroot.Actor, string, time.Time) (BeginResult, error) {
	return BeginResult{}, nil
}

func (persistenceProbe) Finalize(
	context.Context,
	applicationroot.Actor,
	operatoraccount.ID,
	operatoraccount.Version,
	Profile,
) (Account, error) {
	return Account{}, nil
}

func (persistenceProbe) BeginAbort(
	context.Context,
	applicationroot.Actor,
	operatoraccount.ID,
	operatoraccount.Version,
) (operatoraccount.Version, error) {
	return 0, nil
}

func (persistenceProbe) CompleteAbort(
	context.Context,
	applicationroot.Actor,
	operatoraccount.ID,
	operatoraccount.Version,
) error {
	return nil
}

func (persistenceProbe) ListAccounts(context.Context, applicationroot.Actor) ([]Account, error) {
	return nil, nil
}

func (persistenceProbe) ListOrphanAuthenticationLifecycles(context.Context) ([]AuthTarget, error) {
	return nil, nil
}

type runtimeStopperProbe struct{}

func (runtimeStopperProbe) StopAccount(context.Context, AuthTarget) error {
	return nil
}

var (
	_ AuthenticationPersistence = persistenceProbe{}
	_ RuntimeStopper            = runtimeStopperProbe{}
)

func TestPhoneCodeHashDoesNotLeakInFormattingOrStructuredLogs(t *testing.T) {
	secret := "telegram-phone-code-hash"
	hash := NewPhoneCodeHash(secret)

	for _, format := range []string{"%v", "%+v", "%#v"} {
		formatted := fmt.Sprintf(format, hash)
		if bytes.Contains([]byte(formatted), []byte(secret)) {
			t.Fatalf("format %q leaked phone code hash: %q", format, formatted)
		}
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	logger.Info("auth attempt", "phone_code_hash", hash)
	if bytes.Contains(logs.Bytes(), []byte(secret)) {
		t.Fatalf("slog output leaked phone code hash: %q", logs.String())
	}
}

func TestResultRequiresExactlyOneOutcome(t *testing.T) {
	account := &Account{ID: uuid.New()}
	challenge := &Challenge{RequestID: uuid.New(), Stage: StageCode}

	tests := []struct {
		name   string
		result Result
		want   error
	}{
		{name: "account", result: Result{Account: account}},
		{name: "challenge", result: Result{Challenge: challenge}},
		{name: "neither", result: Result{}, want: ErrInvalidResult},
		{name: "both", result: Result{Account: account, Challenge: challenge}, want: ErrInvalidResult},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if failure := test.result.Validate(); !errors.Is(failure, test.want) {
				t.Fatalf("Result.Validate() error = %v, want %v", failure, test.want)
			}
		})
	}
}

func TestContractMethodSetsStayNarrow(t *testing.T) {
	resultType := reflect.TypeOf(Result{})
	if resultType.NumField() != 2 {
		t.Fatalf("Result has %d fields, want exactly Account and Challenge", resultType.NumField())
	}
	for _, name := range []string{"Account", "Challenge"} {
		if _, found := resultType.FieldByName(name); !found {
			t.Fatalf("Result is missing %s", name)
		}
	}

	providerMethods := reflect.TypeOf((*PhoneProvider)(nil)).Elem()
	if providerMethods.NumMethod() != 3 {
		t.Fatalf("PhoneProvider has %d methods, want exactly 3", providerMethods.NumMethod())
	}
	for _, name := range []string{"SendCode", "SignIn", "Password"} {
		if _, found := providerMethods.MethodByName(name); !found {
			t.Fatalf("PhoneProvider is missing %s", name)
		}
	}

	persistenceMethods := reflect.TypeOf((*AuthenticationPersistence)(nil)).Elem()
	if persistenceMethods.NumMethod() != 6 {
		t.Fatalf("AuthenticationPersistence has %d methods, want 6", persistenceMethods.NumMethod())
	}
	for _, name := range []string{"BeginOrResume", "Finalize", "BeginAbort", "CompleteAbort", "ListAccounts", "ListOrphanAuthenticationLifecycles"} {
		if _, found := persistenceMethods.MethodByName(name); !found {
			t.Fatalf("AuthenticationPersistence is missing %s", name)
		}
	}
}

func TestRetryAfterErrorIsPositiveAndRecoverable(t *testing.T) {
	if _, err := NewRetryAfterError(0); !errors.Is(err, ErrInvalidRetryAfter) {
		t.Fatalf("NewRetryAfterError(0) error = %v, want ErrInvalidRetryAfter", err)
	}

	failure, err := NewRetryAfterError(37 * time.Second)
	if err != nil {
		t.Fatalf("NewRetryAfterError() error = %v", err)
	}

	var retryAfter *RetryAfterError
	if !errors.As(failure, &retryAfter) {
		t.Fatalf("errors.As(%T) did not recover RetryAfterError", failure)
	}
	if got := retryAfter.RetryAfter(); got != 37*time.Second {
		t.Fatalf("RetryAfter() = %s, want 37s", got)
	}
}

func TestBeginOutcomesDistinguishAdmissionDecisions(t *testing.T) {
	expiresAt := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	for _, outcome := range []BeginOutcome{BeginStarted, BeginResumed, BeginInProgress} {
		result := BeginResult{
			Account:       Account{Status: operatoraccount.StatusAuthenticating},
			Outcome:       outcome,
			AuthExpiresAt: expiresAt,
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("BeginResult.Validate(%q) error = %v", outcome, err)
		}
	}

	active := BeginResult{
		Account: Account{Status: operatoraccount.StatusActive},
		Outcome: BeginAlreadyActive,
	}
	if err := active.Validate(); err != nil {
		t.Fatalf("active BeginResult.Validate() error = %v", err)
	}

	for _, result := range []BeginResult{
		{Account: Account{Status: operatoraccount.StatusAuthenticating}, Outcome: BeginStarted},
		{Account: Account{Status: operatoraccount.StatusAuthenticating}, Outcome: BeginResumed},
		{Account: Account{Status: operatoraccount.StatusAuthenticating}, Outcome: BeginInProgress},
		{Account: Account{Status: operatoraccount.StatusActive}, Outcome: BeginAlreadyActive, AuthExpiresAt: expiresAt},
	} {
		if err := result.Validate(); !errors.Is(err, ErrInvalidAuthenticationExpiry) {
			t.Fatalf("BeginResult.Validate(%q) error = %v, want ErrInvalidAuthenticationExpiry", result.Outcome, err)
		}
	}
	if err := (BeginResult{Outcome: BeginOutcome("unknown")}).Validate(); !errors.Is(err, ErrInvalidBeginResult) {
		t.Fatalf("unknown BeginResult.Validate() error = %v, want ErrInvalidBeginResult", err)
	}
}

func TestAuthTargetAliasesCanonicalRuntimeTarget(t *testing.T) {
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	accountID := operatoraccount.Identity(uuid.New())
	target := AuthTarget{
		Actor:     actor,
		AccountID: accountID,
		Status:    operatoraccount.StatusDisconnecting,
		Version:   operatoraccount.Version(9),
	}

	if target.Actor != actor || target.AccountID != accountID || target.Status != operatoraccount.StatusDisconnecting || target.Version != operatoraccount.Version(9) {
		t.Fatalf("auth target = %+v, want actor/account/version fence", target)
	}
	if reflect.TypeOf(target) != reflect.TypeOf(operatoraccounts.RuntimeTarget{}) {
		t.Fatalf("AuthTarget type = %v, want %v", reflect.TypeOf(target), reflect.TypeOf(operatoraccounts.RuntimeTarget{}))
	}
}
