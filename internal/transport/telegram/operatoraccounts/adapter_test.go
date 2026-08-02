package operatoraccounts

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/gotd/td/tgerr"

	application "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

type revokeRuntimeFake struct {
	err         error
	teardownErr error
	calls       atomic.Int32
}

func (runtime *revokeRuntimeFake) RevokeAndStop(
	ctx context.Context,
	target application.RuntimeTarget,
	callback accountowner.ClientCallback,
) error {
	runtime.calls.Add(1)
	if runtime.err != nil {
		return runtime.err
	}
	callbackFailure := callback(ctx, nil)
	if runtime.teardownErr != nil {
		return runtime.teardownErr
	}
	return callbackFailure
}

type logoutClientFake struct {
	err   error
	calls atomic.Int32
}

func (client *logoutClientFake) logOut(context.Context) error {
	client.calls.Add(1)
	return client.err
}

func newTestAdapter(runtime Runtime, client *logoutClientFake) Adapter {
	return Adapter{
		runtime: runtime,
		clientFactory: func(*gotdtelegram.Client) logoutClient {
			return client
		},
	}
}

func TestAdapterRevokeAndStopMapsLogoutOutcomesWithoutRetry(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		converged  bool
		kind       application.RemoteLogoutFailureKind
		retryAfter time.Duration
	}{
		{name: "success", converged: true},
		{name: "unauthorized", err: tgerr.New(401, "AUTH_KEY_UNREGISTERED"), converged: true},
		{
			name:       "flood wait",
			err:        tgerr.New(420, "FLOOD_WAIT_3"),
			kind:       application.RemoteLogoutFailureFloodWait,
			retryAfter: 3 * time.Second,
		},
		{name: "transient", err: tgerr.New(503, "REMOTE_DETAIL"), kind: application.RemoteLogoutFailureTransient},
		{name: "permanent", err: tgerr.New(400, "REMOTE_DETAIL"), kind: application.RemoteLogoutFailurePermanent},
		{name: "ambiguous cancellation", err: context.Canceled, kind: application.RemoteLogoutFailureAmbiguous},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &logoutClientFake{err: test.err}
			adapter := newTestAdapter(new(revokeRuntimeFake), client)
			outcome := adapter.RevokeAndStop(context.Background(), application.RuntimeTarget{})
			if err := outcome.Validate(); err != nil {
				t.Fatalf("outcome.Validate() error = %v", err)
			}
			if outcome.Converged() != test.converged {
				t.Fatalf("outcome.Converged() = %t, want %t", outcome.Converged(), test.converged)
			}
			if test.converged {
				if _, failed := outcome.Failure(); failed {
					t.Fatal("converged outcome contained a failure")
				}
			} else {
				failure, ok := outcome.Failure()
				if !ok {
					t.Fatal("failed outcome did not contain a failure")
				}
				if got := failure.Kind(); got != test.kind {
					t.Fatalf("failure.Kind() = %q, want %q", got, test.kind)
				}
				if got := failure.RetryAfter(); got != test.retryAfter {
					t.Fatalf("failure.RetryAfter() = %s, want %s", got, test.retryAfter)
				}
			}
			if got := client.calls.Load(); got != 1 {
				t.Fatalf("AuthLogOut callback calls = %d, want 1", got)
			}
		})
	}
}

func TestAdapterRevokeAndStopMapsRuntimeFailureToClosedOutcome(t *testing.T) {
	runtime := &revokeRuntimeFake{err: accountowner.ErrRegistryStopped}
	client := new(logoutClientFake)
	outcome := newTestAdapter(runtime, client).RevokeAndStop(context.Background(), application.RuntimeTarget{})

	if err := outcome.Validate(); err != nil {
		t.Fatalf("outcome.Validate() error = %v", err)
	}
	failure, ok := outcome.Failure()
	if !ok || failure.Kind() != application.RemoteLogoutFailureUnavailable {
		t.Fatalf("runtime failure outcome = %#v, want unavailable failure", outcome)
	}
	if got := client.calls.Load(); got != 0 {
		t.Fatalf("AuthLogOut callback calls = %d, want 0", got)
	}
}

func TestAdapterRevokeAndStopSanitizesRawLogoutFailure(t *testing.T) {
	client := &logoutClientFake{err: errors.New("provider secret response")}
	outcome := newTestAdapter(new(revokeRuntimeFake), client).RevokeAndStop(context.Background(), application.RuntimeTarget{})
	failure, ok := outcome.Failure()
	if !ok {
		t.Fatal("raw failure did not produce a closed failure outcome")
	}
	if strings.Contains(failure.Error(), "provider secret response") {
		t.Fatalf("sanitized failure leaked raw error: %q", failure.Error())
	}
}

func TestAdapterRevokeAndStopDoesNotConverge401WhenTeardownFails(t *testing.T) {
	runtime := &revokeRuntimeFake{teardownErr: context.DeadlineExceeded}
	client := &logoutClientFake{err: tgerr.New(401, "AUTH_KEY_UNREGISTERED")}
	outcome := newTestAdapter(runtime, client).RevokeAndStop(context.Background(), application.RuntimeTarget{})

	if err := outcome.Validate(); err != nil {
		t.Fatalf("outcome.Validate() error = %v", err)
	}
	if outcome.Converged() {
		t.Fatal("401 was incorrectly reported as converged after teardown failure")
	}
	failure, ok := outcome.Failure()
	if !ok || failure.Kind() != application.RemoteLogoutFailureUnavailable {
		t.Fatalf("teardown failure outcome = %#v, want unavailable failure", outcome)
	}
	if got := client.calls.Load(); got != 1 {
		t.Fatalf("AuthLogOut callback calls = %d, want 1", got)
	}
}

func TestAdapterRevokeAndStopRequiresTeardownForSuccess(t *testing.T) {
	runtime := &revokeRuntimeFake{teardownErr: errors.New("owner teardown failed")}
	client := new(logoutClientFake)
	outcome := newTestAdapter(runtime, client).RevokeAndStop(context.Background(), application.RuntimeTarget{})

	if outcome.Converged() {
		t.Fatal("successful logout callback was reported converged without local teardown")
	}
	failure, ok := outcome.Failure()
	if !ok || failure.Kind() != application.RemoteLogoutFailureUnavailable {
		t.Fatalf("teardown failure outcome = %#v, want unavailable failure", outcome)
	}
}
