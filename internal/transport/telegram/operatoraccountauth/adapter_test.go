package operatoraccountauth

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
)

func TestAdapterIsInertUntilLiveAuthenticationIsExplicitlyApproved(t *testing.T) {
	adapter := New(nil, 1, "not-used")
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	checks := []func() error{
		func() error { _, err := adapter.StartPhone(context.Background(), actor, "+15551234567"); return err },
		func() error {
			_, err := adapter.VerifyPhone(context.Background(), actor, uuid.New(), "12345")
			return err
		},
		func() error { _, err := adapter.StartQR(context.Background(), actor); return err },
		func() error { _, err := adapter.RefreshQR(context.Background(), actor, uuid.New()); return err },
		func() error { _, err := adapter.Status(context.Background(), actor); return err },
	}
	for index, check := range checks {
		if failure := check(); !errors.Is(failure, ErrLiveAuthenticationDisabled) {
			t.Fatalf("adapter operation %d error = %v", index, failure)
		}
	}
}
