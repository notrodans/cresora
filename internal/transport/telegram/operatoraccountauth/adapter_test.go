package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	gotdtelegram "github.com/gotd/td/telegram"
	gotdauth "github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	applicationroot "github.com/notrodans/cresora/internal/application"
	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

type fakeRuntime struct {
	calls  int
	target application.AuthTarget
	err    error
}

func (runtime *fakeRuntime) Execute(
	ctx context.Context,
	target application.AuthTarget,
	callback accountowner.ClientCallback,
) error {
	runtime.calls++
	runtime.target = target
	if runtime.err != nil {
		return runtime.err
	}
	return callback(ctx, nil)
}

type fakePhoneClient struct {
	sent        sentCode
	sendErr     error
	signInErr   error
	passwordErr error
	selfProfile application.Profile
	selfErr     error

	sendCalls     int
	signInCalls   int
	passwordCalls int
	selfCalls     int
	phone         string
	code          string
	hash          string
	passwordValue string
}

func (client *fakePhoneClient) sendCode(_ context.Context, phone string) (sentCode, error) {
	client.sendCalls++
	client.phone = phone
	return client.sent, client.sendErr
}

func (client *fakePhoneClient) signIn(_ context.Context, phone, code, hash string) error {
	client.signInCalls++
	client.phone = phone
	client.code = code
	client.hash = hash
	return client.signInErr
}

func (client *fakePhoneClient) password(_ context.Context, password string) error {
	client.passwordCalls++
	client.passwordValue = password
	return client.passwordErr
}

func (client *fakePhoneClient) self(context.Context) (application.Profile, error) {
	client.selfCalls++
	return client.selfProfile, client.selfErr
}

func newTestAdapter(runtime Runtime, client phoneClient) Adapter {
	return Adapter{
		runtime: runtime,
		clientFactory: func(*gotdtelegram.Client) phoneClient {
			return client
		},
	}
}

func testTarget() application.AuthTarget {
	return application.AuthTarget{
		Actor:     applicationroot.Actor{OperatorID: uuid.MustParse("11111111-1111-4111-8111-111111111111")},
		AccountID: operatoraccount.Identity(uuid.MustParse("22222222-2222-4222-8222-222222222222")),
		Status:    operatoraccount.StatusAuthenticating,
		Version:   operatoraccount.Version(7),
	}
}

func TestAdapterPhoneMethodsUseOneScopedRuntimeOperation(t *testing.T) {
	target := testTarget()
	profile := application.Profile{UserID: 42, Username: "operator"}

	t.Run("send code", func(t *testing.T) {
		runtime := new(fakeRuntime)
		client := &fakePhoneClient{sent: sentCode{hash: "opaque-hash", delivery: "SMS"}}
		adapter := newTestAdapter(runtime, client)

		result, err := adapter.SendCode(context.Background(), target, "+15551234567")
		if err != nil {
			t.Fatalf("SendCode() error = %v, want nil", err)
		}
		if runtime.calls != 1 {
			t.Fatalf("runtime operations = %d, want 1", runtime.calls)
		}
		if runtime.target != target {
			t.Fatalf("runtime target = %#v, want %#v", runtime.target, target)
		}
		if client.sendCalls != 1 || client.selfCalls != 0 {
			t.Fatalf("client calls = send %d, self %d, want 1, 0", client.sendCalls, client.selfCalls)
		}
		if result.PhoneCodeHash.IsZero() || result.Delivery != "SMS" {
			t.Fatalf("SendCode() result = %#v, want opaque hash and SMS delivery", result)
		}
		if result.PhoneCodeHash.String() == "opaque-hash" {
			t.Fatal("PhoneCodeHash.String() exposed the hash")
		}
	})

	t.Run("sign in then self", func(t *testing.T) {
		runtime := new(fakeRuntime)
		client := &fakePhoneClient{selfProfile: profile}
		adapter := newTestAdapter(runtime, client)

		got, err := adapter.SignIn(context.Background(), target, "+15551234567", "12345", application.NewPhoneCodeHash("opaque-hash"))
		if err != nil {
			t.Fatalf("SignIn() error = %v, want nil", err)
		}
		if runtime.calls != 1 {
			t.Fatalf("runtime operations = %d, want 1", runtime.calls)
		}
		if runtime.target != target {
			t.Fatalf("runtime target = %#v, want %#v", runtime.target, target)
		}
		if client.signInCalls != 1 || client.selfCalls != 1 {
			t.Fatalf("client calls = sign-in %d, self %d, want 1, 1", client.signInCalls, client.selfCalls)
		}
		if got != profile {
			t.Fatalf("profile = %#v, want %#v", got, profile)
		}
	})

	t.Run("password then self", func(t *testing.T) {
		runtime := new(fakeRuntime)
		client := &fakePhoneClient{selfProfile: profile}
		adapter := newTestAdapter(runtime, client)

		got, err := adapter.Password(context.Background(), target, "correct-password")
		if err != nil {
			t.Fatalf("Password() error = %v, want nil", err)
		}
		if runtime.calls != 1 {
			t.Fatalf("runtime operations = %d, want 1", runtime.calls)
		}
		if runtime.target != target {
			t.Fatalf("runtime target = %#v, want %#v", runtime.target, target)
		}
		if client.passwordCalls != 1 || client.selfCalls != 1 {
			t.Fatalf("client calls = password %d, self %d, want 1, 1", client.passwordCalls, client.selfCalls)
		}
		if got != profile {
			t.Fatalf("profile = %#v, want %#v", got, profile)
		}
	})
}

func TestAdapterDoesNotFetchSelfAfterFailedAuthentication(t *testing.T) {
	tests := []struct {
		name string
		call func(Adapter) error
	}{
		{
			name: "sign in",
			call: func(adapter Adapter) error {
				_, err := adapter.SignIn(context.Background(), testTarget(), "+15551234567", "12345", application.NewPhoneCodeHash("hash"))
				return err
			},
		},
		{
			name: "password",
			call: func(adapter Adapter) error {
				_, err := adapter.Password(context.Background(), testTarget(), "password")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := new(fakeRuntime)
			client := &fakePhoneClient{
				signInErr:   errors.New("sign-in failed"),
				passwordErr: errors.New("password failed"),
			}
			adapter := newTestAdapter(runtime, client)

			if err := test.call(adapter); err == nil {
				t.Fatal("operation error = nil, want failure")
			}
			if runtime.calls != 1 {
				t.Fatalf("runtime operations = %d, want 1", runtime.calls)
			}
			if client.selfCalls != 0 {
				t.Fatalf("Self calls = %d, want 0", client.selfCalls)
			}
		})
	}
}

func TestAdapterRejectsNonPositiveSelfProfile(t *testing.T) {
	runtime := new(fakeRuntime)
	client := &fakePhoneClient{selfProfile: application.Profile{UserID: 0}}
	adapter := newTestAdapter(runtime, client)

	_, err := adapter.SignIn(context.Background(), testTarget(), "+15551234567", "12345", application.NewPhoneCodeHash("hash"))
	if !errors.Is(err, application.ErrSessionUnavailable) {
		t.Fatalf("SignIn() error = %v, want ErrSessionUnavailable", err)
	}
	if runtime.calls != 1 || client.selfCalls != 1 {
		t.Fatalf("operations = runtime %d, self %d, want 1, 1", runtime.calls, client.selfCalls)
	}
}

func TestAdapterMapsEmptySendCodeResponseToProtocolDiagnostic(t *testing.T) {
	adapter := newTestAdapter(new(fakeRuntime), &fakePhoneClient{})
	_, err := adapter.SendCode(context.Background(), testTarget(), "+15551234567")
	var diagnostic *application.ProviderFailureError
	if !errors.As(err, &diagnostic) {
		t.Fatalf("SendCode() error = %T %v, want ProviderFailureError", err, err)
	}
	if diagnostic.Kind() != application.ProviderFailureProtocol {
		t.Fatalf("ProviderFailureError.Kind() = %v, want protocol", diagnostic.Kind())
	}
	if !errors.Is(err, application.ErrProviderUnavailable) {
		t.Fatal("empty SendCode response does not retain ErrProviderUnavailable")
	}
	if errors.Is(err, application.ErrProviderTransient) {
		t.Fatal("empty SendCode response unexpectedly retained ErrProviderTransient")
	}
}

func TestMapProviderError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "invalid code", err: tgerr.New(400, "PHONE_CODE_INVALID"), want: application.ErrInvalidCode},
		{name: "password required", err: gotdauth.ErrPasswordAuthNeeded, want: application.ErrPasswordRequired},
		{name: "invalid password", err: gotdauth.ErrPasswordInvalid, want: application.ErrInvalidPassword},
		{name: "unauthorized", err: tgerr.New(401, "UNAUTHORIZED"), want: application.ErrUnauthorized},
		{name: "session revoked", err: tgerr.New(401, "SESSION_REVOKED"), want: application.ErrSessionUnavailable},
		{name: "transient", err: errors.New("temporary network failure"), want: application.ErrProviderTransient},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mapProviderError(test.err); !errors.Is(got, test.want) {
				t.Fatalf("mapProviderError() = %v, want %v", got, test.want)
			}
		})
	}

	t.Run("safe flood wait", func(t *testing.T) {
		failure := mapProviderError(tgerr.New(420, "FLOOD_WAIT_3"))
		var retry *application.RetryAfterError
		if !errors.As(failure, &retry) {
			t.Fatalf("mapProviderError() = %v, want RetryAfterError", failure)
		}
		if got := retry.RetryAfter(); got != 3*time.Second {
			t.Fatalf("RetryAfter() = %s, want %s", got, 3*time.Second)
		}
	})
}

func TestProfileRequiresPositiveSelfID(t *testing.T) {
	for _, id := range []int64{0, -1} {
		_, err := profile(&tg.User{ID: id})
		if !errors.Is(err, application.ErrSessionUnavailable) {
			t.Fatalf("profile(ID=%d) error = %v, want ErrSessionUnavailable", id, err)
		}
	}

	got, err := profile(&tg.User{ID: 9, Username: "name", FirstName: "First", LastName: "Last"})
	if err != nil {
		t.Fatalf("profile() error = %v, want nil", err)
	}
	want := application.Profile{UserID: 9, Username: "name", FirstName: "First", LastName: "Last"}
	if got != want {
		t.Fatalf("profile() = %#v, want %#v", got, want)
	}
}

func TestMapProviderErrorReturnsSafeDiagnostics(t *testing.T) {
	const canary = "vendor-secret-should-not-cross-provider-boundary"
	tests := []struct {
		name string
		err  error
		kind application.ProviderFailureKind
	}{
		{
			name: "configuration rejected",
			err:  fmt.Errorf("unsafe wrapper: %w", tgerr.New(400, "API_ID_INVALID")),
			kind: application.ProviderFailureConfigurationRejected,
		},
		{
			name: "invalid phone",
			err:  tgerr.New(400, "PHONE_NUMBER_INVALID"),
			kind: application.ProviderFailurePhoneRejected,
		},
		{
			name: "banned phone",
			err:  tgerr.New(400, "PHONE_NUMBER_BANNED"),
			kind: application.ProviderFailurePhoneRejected,
		},
		{
			name: "phone flood",
			err:  tgerr.New(400, "PHONE_NUMBER_FLOOD"),
			kind: application.ProviderFailurePhoneRejected,
		},
		{
			name: "runtime capacity",
			err:  fmt.Errorf("unsafe wrapper: %w", accountowner.ErrRuntimeCapacity),
			kind: application.ProviderFailureRuntimeCapacity,
		},
		{
			name: "protocol",
			err:  fmt.Errorf("unsafe wrapper: %w", errInvalidSendCodeResponse),
			kind: application.ProviderFailureProtocol,
		},
		{
			name: "remote rejected",
			err:  fmt.Errorf("unsafe wrapper: %w", tgerr.New(400, canary)),
			kind: application.ProviderFailureRemoteRejected,
		},
		{
			name: "remote failure",
			err:  fmt.Errorf("unsafe wrapper: %w", tgerr.New(503, canary)),
			kind: application.ProviderFailureRemoteFailure,
		},
		{
			name: "transport unknown",
			err:  errors.New(canary),
			kind: application.ProviderFailureTransportUnknown,
		},
		{
			name: "malformed flood wait",
			err:  tgerr.New(420, "FLOOD_WAIT"),
			kind: application.ProviderFailureRemoteRejected,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := mapProviderError(test.err)
			var diagnostic *application.ProviderFailureError
			if !errors.As(failure, &diagnostic) {
				t.Fatalf("mapProviderError() = %T %v, want ProviderFailureError", failure, failure)
			}
			if diagnostic.Kind() != test.kind {
				t.Fatalf("ProviderFailureError.Kind() = %v, want %v", diagnostic.Kind(), test.kind)
			}
			semantic := application.ErrProviderTransient
			if test.kind == application.ProviderFailureProtocol {
				semantic = application.ErrProviderUnavailable
			}
			if !errors.Is(failure, semantic) {
				t.Fatalf("mapProviderError() = %v, want %v identity", failure, semantic)
			}
			if test.kind == application.ProviderFailureProtocol && errors.Is(failure, application.ErrProviderTransient) {
				t.Fatal("protocol diagnostic unexpectedly retained ErrProviderTransient")
			}
			for _, format := range []string{"%s", "%v", "%+v"} {
				message := fmt.Sprintf(format, failure)
				if strings.Contains(message, canary) {
					t.Fatalf("formatted %s error disclosed provider data: %q", format, message)
				}
			}
		})
	}
}

func TestMapProviderErrorPreservesSafeSentinelsAndCancellation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "canceled", err: fmt.Errorf("outer: %w", context.Canceled), want: context.Canceled},
		{name: "deadline", err: fmt.Errorf("outer: %w", context.DeadlineExceeded), want: context.DeadlineExceeded},
		{name: "invalid code", err: fmt.Errorf("outer: %w", tgerr.New(400, "PHONE_CODE_INVALID")), want: application.ErrInvalidCode},
		{name: "password required", err: fmt.Errorf("outer: %w", tgerr.New(400, "SESSION_PASSWORD_NEEDED")), want: application.ErrPasswordRequired},
		{name: "invalid password", err: fmt.Errorf("outer: %w", tgerr.New(400, "PASSWORD_HASH_INVALID")), want: application.ErrInvalidPassword},
		{name: "session", err: fmt.Errorf("outer: %w", tgerr.New(401, "SESSION_REVOKED")), want: application.ErrSessionUnavailable},
		{name: "unauthorized", err: fmt.Errorf("outer: %w", tgerr.New(401, "UNAUTHORIZED")), want: application.ErrUnauthorized},
		{name: "application transient", err: fmt.Errorf("outer: %w", application.ErrProviderTransient), want: application.ErrProviderTransient},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mapProviderError(test.err); got != test.want {
				t.Fatalf("mapProviderError() = %v (%T), want %v (%T)", got, got, test.want, test.want)
			}
		})
	}
}

func TestMapProviderErrorFindsWrappedRPCInJoinedFailure(t *testing.T) {
	const canary = "joined-rpc-provider-secret"
	failure := mapProviderError(errors.Join(
		errors.New(canary),
		fmt.Errorf("outer: %w", tgerr.New(502, canary)),
	))
	var diagnostic *application.ProviderFailureError
	if !errors.As(failure, &diagnostic) {
		t.Fatalf("mapProviderError() = %T %v, want ProviderFailureError", failure, failure)
	}
	if diagnostic.Kind() != application.ProviderFailureRemoteFailure {
		t.Fatalf("ProviderFailureError.Kind() = %v, want remote failure", diagnostic.Kind())
	}
	for _, format := range []string{"%s", "%v", "%+v"} {
		if message := fmt.Sprintf(format, failure); strings.Contains(message, canary) {
			t.Fatalf("formatted %s error disclosed provider data: %q", format, message)
		}
	}
}
