package operatoraccount

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAccountLifecycleFollowsApprovedTransitionGraph(t *testing.T) {
	account := New(Identity(uuid.MustParse("018f0f12-0f12-7f12-8f12-0f120f120f12")))
	authExpiry := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)

	transitions := []struct {
		name          string
		move          func() error
		status        Status
		version       Version
		failureCode   FailureCode
		telegramID    int64
		authExpiresAt time.Time
		remoteIntent  bool
	}{
		{
			name:          "disconnected to authenticating",
			move:          func() error { return account.BeginAuthentication(authExpiry) },
			status:        StatusAuthenticating,
			version:       2,
			authExpiresAt: authExpiry,
		},
		{
			name:       "authenticating to active",
			move:       func() error { return account.Activate(42) },
			status:     StatusActive,
			version:    3,
			telegramID: 42,
		},
		{
			name:        "active to reauthentication required",
			move:        func() error { return account.RequireReauthentication(FailureCodeSessionInvalid) },
			status:      StatusReauthRequired,
			version:     4,
			failureCode: FailureCodeSessionInvalid,
			telegramID:  42,
		},
		{
			name:          "reauthentication required to authenticating",
			move:          func() error { return account.BeginAuthentication(authExpiry) },
			status:        StatusAuthenticating,
			version:       5,
			telegramID:    42,
			authExpiresAt: authExpiry,
		},
		{
			name:       "authenticating to active after reauthentication",
			move:       func() error { return account.Activate(42) },
			status:     StatusActive,
			version:    6,
			telegramID: 42,
		},
		{
			name:         "active to disconnecting",
			move:         account.BeginDisconnect,
			status:       StatusDisconnecting,
			version:      7,
			telegramID:   42,
			remoteIntent: true,
		},
		{
			name:       "disconnecting to disconnected",
			move:       account.MarkDisconnected,
			status:     StatusDisconnected,
			version:    8,
			telegramID: 42,
		},
	}

	for _, transition := range transitions {
		t.Run(transition.name, func(t *testing.T) {
			if failure := transition.move(); failure != nil {
				t.Fatalf("account lifecycle transition: %v", failure)
			}
			if actual := account.Status(); actual != transition.status {
				t.Fatalf("account status = %q, want %q", actual, transition.status)
			}
			if actual := account.Version(); actual != transition.version {
				t.Fatalf("account version = %d, want %d", actual, transition.version)
			}
			if actual := account.FailureCode(); actual != transition.failureCode {
				t.Fatalf("account failure code = %q, want %q", actual, transition.failureCode)
			}
			if actual := account.TelegramUserID(); actual != transition.telegramID {
				t.Fatalf("account Telegram user ID = %d, want %d", actual, transition.telegramID)
			}
			if actual := account.AuthExpiresAt(); !actual.Equal(transition.authExpiresAt) {
				t.Fatalf("account auth expiry = %s, want %s", actual, transition.authExpiresAt)
			}
			if actual := account.RemoteLogoutRequired(); actual != transition.remoteIntent {
				t.Fatalf("account remote logout requirement = %t, want %t", actual, transition.remoteIntent)
			}
		})
	}
}

func TestAccountLifecycleAllowsAuthenticatingAndReauthenticationDisconnect(t *testing.T) {
	authExpiry := time.Date(2026, time.August, 1, 13, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		move         func(*Account) error
		remoteIntent bool
	}{
		{
			name: "authenticating",
			move: func(account *Account) error {
				if failure := account.BeginAuthentication(authExpiry); failure != nil {
					return failure
				}
				return account.BeginDisconnect()
			},
		},
		{
			name:         "reauthentication required",
			remoteIntent: true,
			move: func(account *Account) error {
				if failure := account.BeginAuthentication(authExpiry); failure != nil {
					return failure
				}
				if failure := account.Activate(42); failure != nil {
					return failure
				}
				if failure := account.RequireReauthentication(FailureCodeAuthExpired); failure != nil {
					return failure
				}
				return account.BeginDisconnect()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := New(Identity(uuid.New()))
			if failure := test.move(&account); failure != nil {
				t.Fatalf("begin disconnect: %v", failure)
			}
			if account.Status() != StatusDisconnecting {
				t.Fatalf("account status = %q, want %q", account.Status(), StatusDisconnecting)
			}
			if actual := account.RemoteLogoutRequired(); actual != test.remoteIntent {
				t.Fatalf("account remote logout requirement = %t, want %t", actual, test.remoteIntent)
			}
			if failure := account.MarkDisconnected(); failure != nil {
				t.Fatalf("mark disconnected: %v", failure)
			}
			if account.Status() != StatusDisconnected || !account.AuthExpiresAt().IsZero() || account.FailureCode() != NoFailure || account.RemoteLogoutRequired() {
				t.Fatalf("disconnected account retained transient state: status=%q expiry=%s failure=%q remote logout requirement=%t", account.Status(), account.AuthExpiresAt(), account.FailureCode(), account.RemoteLogoutRequired())
			}
		})
	}
}

func TestAccountRejectsInvalidTransitionWithoutChangingState(t *testing.T) {
	account := New(Identity(uuid.New()))
	before := account

	if failure := account.Activate(42); !errors.Is(failure, ErrInvalidTransition) {
		t.Fatalf("activate from disconnected error = %v, want ErrInvalidTransition", failure)
	}
	if account != before {
		t.Fatalf("invalid transition changed account from %#v to %#v", before, account)
	}

	authenticating, failure := Restore(
		Identity(uuid.New()),
		StatusAuthenticating,
		InitialVersion,
		NoFailure,
		42,
		time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC),
	)
	if failure != nil {
		t.Fatalf("restore authenticating account: %v", failure)
	}
	authenticatingBefore := authenticating
	if failure := authenticating.RequireReauthentication(FailureCodeSessionInvalid); !errors.Is(failure, ErrInvalidTransition) {
		t.Fatalf("require reauthentication from authenticating error = %v, want ErrInvalidTransition", failure)
	}
	if authenticating != authenticatingBefore {
		t.Fatalf("invalid reauthentication transition changed account from %#v to %#v", authenticatingBefore, authenticating)
	}
}

func TestAccountRequiresIdentityExpiryAndBoundedFailureCodes(t *testing.T) {
	account := New(Identity(uuid.New()))
	if failure := account.BeginAuthentication(time.Time{}); !errors.Is(failure, ErrInvalidAuthenticationExpiry) {
		t.Fatalf("authentication without expiry error = %v, want ErrInvalidAuthenticationExpiry", failure)
	}
	if failure := account.BeginAuthentication(time.Date(2026, time.August, 1, 14, 0, 0, 0, time.UTC)); failure != nil {
		t.Fatalf("begin authentication: %v", failure)
	}
	if failure := account.Activate(0); !errors.Is(failure, ErrInvalidTelegramIdentity) {
		t.Fatalf("activation without identity error = %v, want ErrInvalidTelegramIdentity", failure)
	}
	if failure := account.Activate(-1); !errors.Is(failure, ErrInvalidTelegramIdentity) {
		t.Fatalf("activation with negative identity error = %v, want ErrInvalidTelegramIdentity", failure)
	}
	if failure := account.Activate(42); failure != nil {
		t.Fatalf("activate account: %v", failure)
	}
	if failure := account.RequireReauthentication(FailureCode("transport detail leaked")); !errors.Is(failure, ErrInvalidFailureCode) {
		t.Fatalf("unbounded failure code error = %v, want ErrInvalidFailureCode", failure)
	}
	if account.Status() != StatusActive || account.Version() != 3 {
		t.Fatalf("invalid failure code changed account: status=%q version=%d", account.Status(), account.Version())
	}
}

func TestRestoreRejectsInconsistentCurrentState(t *testing.T) {
	id := Identity(uuid.New())
	expiry := time.Date(2026, time.August, 1, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		id            ID
		status        Status
		version       Version
		failureCode   FailureCode
		telegramID    int64
		authExpiresAt time.Time
		remoteIntent  bool
	}{
		{name: "zero id", id: ID{}, status: StatusDisconnected, version: InitialVersion},
		{name: "unknown status", id: id, status: Status("unknown"), version: InitialVersion},
		{name: "zero version", id: id, status: StatusDisconnected},
		{name: "active without identity", id: id, status: StatusActive, version: InitialVersion},
		{name: "reauthentication required without identity", id: id, status: StatusReauthRequired, version: InitialVersion, failureCode: FailureCodeSessionInvalid},
		{name: "authenticating without expiry", id: id, status: StatusAuthenticating, version: InitialVersion},
		{name: "active with expiry", id: id, status: StatusActive, version: InitialVersion, telegramID: 42, authExpiresAt: expiry},
		{name: "reauthentication required without code", id: id, status: StatusReauthRequired, version: InitialVersion, telegramID: 42},
		{name: "reauthentication required with unsupported code", id: id, status: StatusReauthRequired, version: InitialVersion, failureCode: FailureCode("unsupported"), telegramID: 42},
		{name: "disconnected with code", id: id, status: StatusDisconnected, version: InitialVersion, failureCode: FailureCodeSessionInvalid},
		{name: "active with remote disconnect intent", id: id, status: StatusActive, version: InitialVersion, telegramID: 42, remoteIntent: true},
		{name: "disconnected with remote disconnect intent", id: id, status: StatusDisconnected, version: InitialVersion, remoteIntent: true},
		{name: "authenticating with remote disconnect intent", id: id, status: StatusAuthenticating, version: InitialVersion, telegramID: 42, authExpiresAt: expiry, remoteIntent: true},
		{name: "reauthentication required with remote disconnect intent", id: id, status: StatusReauthRequired, version: InitialVersion, failureCode: FailureCodeSessionInvalid, telegramID: 42, remoteIntent: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, failure := Restore(test.id, test.status, test.version, test.failureCode, test.telegramID, test.authExpiresAt, test.remoteIntent); !errors.Is(failure, ErrInvalidState) {
				t.Fatalf("restore error = %v, want ErrInvalidState", failure)
			}
		})
	}
}

func TestRestoreAllowsRemoteLogoutRequirementOnlyWhileDisconnecting(t *testing.T) {
	account, failure := Restore(
		Identity(uuid.New()),
		StatusDisconnecting,
		InitialVersion,
		NoFailure,
		42,
		time.Time{},
		true,
	)
	if failure != nil {
		t.Fatalf("restore disconnecting account: %v", failure)
	}
	if !account.RemoteLogoutRequired() {
		t.Fatal("restored disconnecting account remote logout requirement = false, want true")
	}
}

func TestAccountDoesNotWrapLifecycleVersion(t *testing.T) {
	account, failure := Restore(Identity(uuid.New()), StatusDisconnected, ^Version(0), NoFailure, 0, time.Time{})
	if failure != nil {
		t.Fatalf("restore max-version account: %v", failure)
	}
	before := account

	if failure := account.BeginAuthentication(time.Date(2026, time.August, 1, 16, 0, 0, 0, time.UTC)); !errors.Is(failure, ErrVersionExhausted) {
		t.Fatalf("begin authentication at max version error = %v, want ErrVersionExhausted", failure)
	}
	if account != before {
		t.Fatalf("version-exhausted transition changed account from %#v to %#v", before, account)
	}
}
