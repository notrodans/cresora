package operatoraccounts

import (
	"errors"
	"testing"
	"time"
)

func TestNewRemoteLogoutFailureValidatesBoundedValues(t *testing.T) {
	tests := []struct {
		name       string
		kind       RemoteLogoutFailureKind
		retryAfter time.Duration
		wantErr    bool
	}{
		{name: "unknown kind", kind: RemoteLogoutFailureUnknown, wantErr: true},
		{name: "flood wait requires duration", kind: RemoteLogoutFailureFloodWait, wantErr: true},
		{name: "flood wait rejects negative duration", kind: RemoteLogoutFailureFloodWait, retryAfter: -time.Second, wantErr: true},
		{name: "transient rejects duration", kind: RemoteLogoutFailureTransient, retryAfter: time.Second, wantErr: true},
		{name: "ambiguous", kind: RemoteLogoutFailureAmbiguous},
		{name: "permanent", kind: RemoteLogoutFailurePermanent},
		{name: "unavailable", kind: RemoteLogoutFailureUnavailable},
		{name: "flood wait", kind: RemoteLogoutFailureFloodWait, retryAfter: 3 * time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure, err := NewRemoteLogoutFailure(test.kind, test.retryAfter)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidRemoteLogoutFailure) {
					t.Fatalf("NewRemoteLogoutFailure() error = %v, want ErrInvalidRemoteLogoutFailure", err)
				}
				if failure != nil {
					t.Fatalf("NewRemoteLogoutFailure() value = %#v, want nil", failure)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRemoteLogoutFailure() error = %v", err)
			}
			if failure.Kind() != test.kind || failure.RetryAfter() != test.retryAfter {
				t.Fatalf("failure = kind %q retry-after %s, want kind %q retry-after %s", failure.Kind(), failure.RetryAfter(), test.kind, test.retryAfter)
			}
			if failure.Error() != "telegram account remote logout failure" {
				t.Fatalf("failure error = %q, want fixed safe message", failure.Error())
			}
		})
	}
}

func TestRemoteLogoutFailurePreservesSafeClassWhenNonConverged(t *testing.T) {
	tests := []struct {
		name       string
		kind       RemoteLogoutFailureKind
		retryAfter time.Duration
		wantIs     error
	}{
		{name: "flood wait", kind: RemoteLogoutFailureFloodWait, retryAfter: time.Second, wantIs: ErrRemoteLogoutFloodWait},
		{name: "transient", kind: RemoteLogoutFailureTransient, wantIs: ErrRemoteLogoutTransient},
		{name: "ambiguous", kind: RemoteLogoutFailureAmbiguous, wantIs: ErrRemoteLogoutAmbiguous},
		{name: "permanent", kind: RemoteLogoutFailurePermanent, wantIs: ErrRemoteLogoutPermanent},
		{name: "unavailable", kind: RemoteLogoutFailureUnavailable, wantIs: ErrRuntimeUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure, err := NewRemoteLogoutFailure(test.kind, test.retryAfter)
			if err != nil {
				t.Fatalf("NewRemoteLogoutFailure() error = %v", err)
			}
			wrapped := nonConvergedRemoteFailure(failure)
			if !errors.Is(wrapped, ErrRemoteLogoutNotConverged) {
				t.Fatalf("wrapped error = %v, want ErrRemoteLogoutNotConverged", wrapped)
			}
			if !errors.Is(wrapped, test.wantIs) {
				t.Fatalf("wrapped error = %v, want class %v", wrapped, test.wantIs)
			}
			var actual *RemoteLogoutFailure
			if !errors.As(wrapped, &actual) {
				t.Fatalf("errors.As() did not recover bounded failure from %v", wrapped)
			}
			if actual.Kind() != test.kind {
				t.Fatalf("errors.As() kind = %q, want %q", actual.Kind(), test.kind)
			}
		})
	}
}
