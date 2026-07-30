// Package fake provides deterministic provider ports for development and
// tests. It has no network, gotd, session, or persistence dependency and must
// only be composed explicitly in a development/testing route.
package fake

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/notrodans/nebula-go/internal/application/operatoraccountauth/challenges"
)

// DefaultCode is deliberately a fake-only value. It is not used by the
// production composition and is never accepted by a live Telegram adapter.
const DefaultCode = "12345"

// Provider is a deterministic, thread-safe phone and QR provider. The
// coordinator only receives provider handles through its opaque port type.
type Provider struct {
	mu sync.Mutex

	code     string
	sequence uint64
}

// New constructs a fake provider. An empty code selects DefaultCode.
func New(code string) *Provider {
	if code == "" {
		code = DefaultCode
	}
	return &Provider{code: code}
}

func (provider *Provider) StartPhone(ctx context.Context, request challenges.PhoneStart) (challenges.PhoneStarted, error) {
	if err := contextError(ctx); err != nil {
		return challenges.PhoneStarted{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.sequence++
	handle := fmt.Sprintf("fake-phone-%d", provider.sequence)
	return challenges.PhoneStarted{Handle: challenges.NewProviderHandle(handle), Delivery: "Telegram test code"}, nil
}

func (provider *Provider) VerifyPhone(ctx context.Context, handle challenges.ProviderHandle, code string) (challenges.PhoneVerified, error) {
	if err := contextError(ctx); err != nil {
		return challenges.PhoneVerified{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	// The fake intentionally cannot inspect ProviderHandle.value; code behavior
	// is deterministic and does not expose a provider token.
	if code != provider.code {
		return challenges.PhoneVerified{}, errors.New("fake code rejected")
	}
	return challenges.PhoneVerified{}, nil
}

func (provider *Provider) CancelPhone(ctx context.Context, handle challenges.ProviderHandle) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return nil
}

func (provider *Provider) StartQR(ctx context.Context, _ challenges.QRStart) (challenges.QRStarted, error) {
	if err := contextError(ctx); err != nil {
		return challenges.QRStarted{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.sequence++
	handle := fmt.Sprintf("fake-qr-%d", provider.sequence)
	return challenges.QRStarted{
		Handle: challenges.NewProviderHandle(handle),
		URL:    "tg://login?token=" + handle,
	}, nil
}

func (provider *Provider) RefreshQR(ctx context.Context, handle challenges.ProviderHandle) (challenges.QRStarted, error) {
	if err := contextError(ctx); err != nil {
		return challenges.QRStarted{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.sequence++
	newHandle := fmt.Sprintf("fake-qr-%d", provider.sequence)
	return challenges.QRStarted{
		Handle: challenges.NewProviderHandle(newHandle),
		URL:    "tg://login?token=" + newHandle,
	}, nil
}

func (provider *Provider) CancelQR(ctx context.Context, handle challenges.ProviderHandle) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	return ctx.Err()
}

var _ challenges.PhoneProvider = (*Provider)(nil)
var _ challenges.QRProvider = (*Provider)(nil)
