package telegram_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/gotd/td/tg"

	"github.com/notrodans/cresora/internal/domain/recipient"
	transport "github.com/notrodans/cresora/internal/transport/telegram"
	targets "github.com/notrodans/cresora/internal/transport/telegram/targets"
)

type fakeLookup func(context.Context, transport.PeerLookupRequest) (transport.PeerProjection, error)

func (lookup fakeLookup) Lookup(
	context context.Context,
	request transport.PeerLookupRequest,
) (transport.PeerProjection, error) {
	return lookup(context, request)
}

func TestResolverMapsPeers(t *testing.T) {
	accountID := uuid.New()
	recipientID := uuid.New()
	ctx := context.Background()

	tests := []struct {
		name       string
		projection transport.PeerProjection
		assertPeer func(*testing.T, tg.InputPeerClass)
	}{
		{
			name: "user",
			projection: transport.PeerProjection{
				Type:       transport.PeerTypeUser,
				ID:         101,
				AccessHash: int64Pointer(201),
			},
			assertPeer: func(t *testing.T, peer tg.InputPeerClass) {
				actual, ok := peer.(*tg.InputPeerUser)
				if !ok {
					t.Fatalf("expected user peer, got %T", peer)
				}
				if actual.UserID != 101 || actual.AccessHash != 201 {
					t.Fatalf("unexpected user peer: %+v", actual)
				}
			},
		},
		{
			name: "chat",
			projection: transport.PeerProjection{
				Type: transport.PeerTypeChat,
				ID:   301,
			},
			assertPeer: func(t *testing.T, peer tg.InputPeerClass) {
				actual, ok := peer.(*tg.InputPeerChat)
				if !ok {
					t.Fatalf("expected chat peer, got %T", peer)
				}
				if actual.ChatID != 301 {
					t.Fatalf("unexpected chat peer: %+v", actual)
				}
			},
		},
		{
			name: "channel",
			projection: transport.PeerProjection{
				Type:       transport.PeerTypeChannel,
				ID:         401,
				AccessHash: int64Pointer(501),
			},
			assertPeer: func(t *testing.T, peer tg.InputPeerClass) {
				actual, ok := peer.(*tg.InputPeerChannel)
				if !ok {
					t.Fatalf("expected channel peer, got %T", peer)
				}
				if actual.ChannelID != 401 || actual.AccessHash != 501 {
					t.Fatalf("unexpected channel peer: %+v", actual)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actualRequest transport.PeerLookupRequest
			resolver := targets.NewResolver(accountID, fakeLookup(func(
				_ context.Context,
				request transport.PeerLookupRequest,
			) (transport.PeerProjection, error) {
				actualRequest = request
				return test.projection, nil
			}))

			target, failure := resolver.Target(ctx, recipient.Identity(recipientID))
			if failure != nil {
				t.Fatalf("resolve target: %v", failure)
			}
			peer, failure := target.Peer()
			if failure != nil {
				t.Fatalf("build peer: %v", failure)
			}
			test.assertPeer(t, peer)

			if actualRequest.AccountID != accountID {
				t.Fatalf("expected account %s, got %s", accountID, actualRequest.AccountID)
			}
			if actualRequest.RecipientID != recipientID {
				t.Fatalf("expected recipient %s, got %s", recipientID, actualRequest.RecipientID)
			}
		})
	}
}

func TestResolverPreservesLookupErrors(t *testing.T) {
	expected := errors.New("lookup failed")
	resolver := targets.NewResolver(uuid.New(), fakeLookup(func(
		context.Context,
		transport.PeerLookupRequest,
	) (transport.PeerProjection, error) {
		return transport.PeerProjection{}, expected
	}))

	_, failure := resolver.Target(context.Background(), recipient.Identity(uuid.New()))

	if !errors.Is(failure, expected) {
		t.Fatalf("expected lookup error to be preserved, got %v", failure)
	}
}

func TestResolverRejectsZeroRecipientWithoutLookup(t *testing.T) {
	lookupCalled := false
	resolver := targets.NewResolver(uuid.New(), fakeLookup(func(
		context.Context,
		transport.PeerLookupRequest,
	) (transport.PeerProjection, error) {
		lookupCalled = true
		return transport.PeerProjection{}, nil
	}))

	assertPanics(t, func() {
		_, _ = resolver.Target(context.Background(), recipient.Identity(uuid.Nil))
	})

	if lookupCalled {
		t.Fatal("expected zero recipient identity to be rejected before lookup")
	}
}

func TestNewResolverGuards(t *testing.T) {
	lookup := fakeLookup(func(
		context.Context,
		transport.PeerLookupRequest,
	) (transport.PeerProjection, error) {
		return transport.PeerProjection{}, nil
	})

	assertPanics(t, func() {
		_ = targets.NewResolver(uuid.Nil, lookup)
	})
}

func TestResolverRejectsInvalidPeers(t *testing.T) {
	tests := []struct {
		name       string
		projection transport.PeerProjection
	}{
		{
			name: "zero ID",
			projection: transport.PeerProjection{
				Type:       transport.PeerTypeUser,
				AccessHash: int64Pointer(1),
			},
		},
		{
			name: "unknown type",
			projection: transport.PeerProjection{
				Type: transport.PeerType("unknown"),
				ID:   1,
			},
		},
		{
			name: "user without hash",
			projection: transport.PeerProjection{
				Type: transport.PeerTypeUser,
				ID:   1,
			},
		},
		{
			name: "channel with zero hash",
			projection: transport.PeerProjection{
				Type:       transport.PeerTypeChannel,
				ID:         1,
				AccessHash: int64Pointer(0),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := targets.NewResolver(uuid.New(), fakeLookup(func(
				context.Context,
				transport.PeerLookupRequest,
			) (transport.PeerProjection, error) {
				return test.projection, nil
			}))

			_, failure := resolver.Target(context.Background(), recipient.Identity(uuid.New()))

			if !errors.Is(failure, transport.ErrInvalidPeer) {
				t.Fatalf("expected invalid peer error, got %v", failure)
			}
		})
	}
}

func int64Pointer(value int64) *int64 {
	return &value
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
