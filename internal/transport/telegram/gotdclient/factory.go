// Package gotdclient contains the narrow boundary between the Telegram
// transport session store and gotd's client session storage.
package gotdclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/gotd/td/session"
	gotdtelegram "github.com/gotd/td/telegram"

	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
)

var (
	errNilSessionStore = errors.New("telegram session store is required")
	errInvalidScope    = errors.New("telegram session scope is invalid")
	errInvalidAppID    = errors.New("telegram app id must be positive")
	errBlankAppHash    = errors.New("telegram app hash is required")
)

// Factory creates unstarted gotd clients scoped to an owned Telegram account.
// It retains only the transport-neutral session store; clients and account
// scopes are created per NewClient call.
type Factory struct {
	store transporttelegram.SessionStore
}

// New constructs a stateless gotd client factory around store.
func New(store transporttelegram.SessionStore) Factory {
	return Factory{store: store}
}

// NewFactory is an explicit constructor name for Factory.
func NewFactory(store transporttelegram.SessionStore) Factory {
	return New(store)
}

// newClient is kept as a private seam so package tests can verify construction
// without invoking gotd client behavior. Its signature intentionally matches
// the pinned gotd constructor exactly.
var newClient = gotdtelegram.NewClient

// NewClient creates one new, unstarted gotd client for scope.
func (factory Factory) NewClient(
	scope transporttelegram.SessionScope,
	appID int,
	appHash string,
) (*gotdtelegram.Client, error) {
	return factory.newClient(scope, appID, appHash, nil)
}

// NewClientWithConnectionState creates one new, unstarted gotd client for
// scope and installs the narrow connection-state observer used by lifecycle
// owners. It deliberately accepts only this dedicated callback rather than
// exposing gotd Options, so session storage remains bound to this scope.
func (factory Factory) NewClientWithConnectionState(
	scope transporttelegram.SessionScope,
	appID int,
	appHash string,
	onConnectionState func(gotdtelegram.ConnectionState),
) (*gotdtelegram.Client, error) {
	return factory.newClient(scope, appID, appHash, onConnectionState)
}

func (factory Factory) newClient(
	scope transporttelegram.SessionScope,
	appID int,
	appHash string,
	onConnectionState func(gotdtelegram.ConnectionState),
) (*gotdtelegram.Client, error) {
	if sessionStoreIsNil(factory.store) {
		return nil, errNilSessionStore
	}
	if scope.OperatorID == uuid.Nil || scope.AccountID == uuid.Nil {
		return nil, errInvalidScope
	}
	if appID <= 0 {
		return nil, errInvalidAppID
	}
	if strings.TrimSpace(appHash) == "" {
		return nil, errBlankAppHash
	}

	storage := &scopedSessionStorage{
		store: factory.store,
		scope: scope,
	}
	return newClient(appID, appHash, gotdtelegram.Options{
		SessionStorage:    storage,
		OnConnectionState: onConnectionState,
	}), nil
}

// sessionStoreIsNil also rejects an interface containing a typed nil store.
// Calling a method through such an interface would otherwise defer the
// failure until gotd starts and loads the session.
func sessionStoreIsNil(store transporttelegram.SessionStore) bool {
	if store == nil {
		return true
	}

	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// scopedSessionStorage adapts one complete operator/account scope to gotd's
// unscoped session storage interface. The scope is copied into each adapter.
type scopedSessionStorage struct {
	store transporttelegram.SessionStore
	scope transporttelegram.SessionScope
}

var _ gotdtelegram.SessionStorage = (*scopedSessionStorage)(nil)

func (storage scopedSessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	loaded, err := storage.store.Load(ctx, storage.scope)
	if err != nil {
		return nil, fmt.Errorf("load Telegram session: %w", err)
	}
	if !loaded.Present {
		return nil, session.ErrNotFound
	}
	return loaded.Bytes, nil
}

func (storage scopedSessionStorage) StoreSession(ctx context.Context, data []byte) error {
	if err := storage.store.Store(ctx, storage.scope, data); err != nil {
		return fmt.Errorf("store Telegram session: %w", err)
	}
	return nil
}
