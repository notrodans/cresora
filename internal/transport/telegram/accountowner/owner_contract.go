package accountowner

import (
	"context"

	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

// ownerRuntime is the narrow lifecycle and callback surface the registry
// consumes. Keeping the seam here lets registry tests use deterministic owner
// fakes without exposing a gotd dependency to the registry contract.
type ownerRuntime interface {
	Run(context.Context) error
	Stop()
	WaitReady(context.Context) error
	Wait(context.Context) error
	Execute(context.Context, ClientCallback) error
}

// ownerBuilder is a user defined factory for ownerRuntime instances.
type ownerBuilder func(
	gotdclient.Factory,
	transporttelegram.SessionScope,
	int,
	string,
) (ownerRuntime, error)
