package gotdclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/gotd/td/session"
	gotdtelegram "github.com/gotd/td/telegram"

	transporttelegram "github.com/notrodans/cresora/internal/transport/telegram"
)

type sessionStoreStub struct {
	loadSession transporttelegram.Session
	loadErr     error
	storeErr    error

	loadContext context.Context
	loadScope   transporttelegram.SessionScope

	storeContext context.Context
	storeScope   transporttelegram.SessionScope
	storeData    []byte
}

func (stub *sessionStoreStub) Load(
	ctx context.Context,
	scope transporttelegram.SessionScope,
) (transporttelegram.Session, error) {
	stub.loadContext = ctx
	stub.loadScope = scope
	return stub.loadSession, stub.loadErr
}

func (stub *sessionStoreStub) Store(
	ctx context.Context,
	scope transporttelegram.SessionScope,
	data []byte,
) error {
	stub.storeContext = ctx
	stub.storeScope = scope
	stub.storeData = data
	return stub.storeErr
}

type panicSessionStore struct{}

func (panicSessionStore) Load(context.Context, transporttelegram.SessionScope) (transporttelegram.Session, error) {
	panic("session store was loaded during client construction")
}

func (panicSessionStore) Store(context.Context, transporttelegram.SessionScope, []byte) error {
	panic("session store was written during client construction")
}

func validScope() transporttelegram.SessionScope {
	return transporttelegram.SessionScope{
		OperatorID: uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		AccountID:  uuid.MustParse("22222222-2222-4222-8222-222222222222"),
	}
}

func TestNewClientRejectsNilStore(t *testing.T) {
	var store *sessionStoreStub

	called := false
	previous := newClient
	newClient = func(int, string, gotdtelegram.Options) *gotdtelegram.Client {
		called = true
		return nil
	}
	t.Cleanup(func() { newClient = previous })

	client, err := New(store).NewClient(validScope(), 123, "app-hash")
	if client != nil {
		t.Fatal("client is non-nil for a nil session store")
	}
	if !errors.Is(err, errNilSessionStore) {
		t.Fatalf("error = %v, want nil-store validation error", err)
	}
	if called {
		t.Fatal("gotd constructor was called for a nil session store")
	}
}

func TestNewClientValidatesBeforeConstructionWithoutLeakingHash(t *testing.T) {
	secret := "super-secret-app-hash-7f6a"
	previous := newClient
	called := false
	newClient = func(int, string, gotdtelegram.Options) *gotdtelegram.Client {
		called = true
		return nil
	}
	t.Cleanup(func() { newClient = previous })

	tests := []struct {
		name    string
		store   transporttelegram.SessionStore
		scope   transporttelegram.SessionScope
		appID   int
		appHash string
		want    error
	}{
		{
			name:    "nil operator UUID",
			store:   &sessionStoreStub{},
			scope:   transporttelegram.SessionScope{AccountID: validScope().AccountID},
			appID:   123,
			appHash: secret,
			want:    errInvalidScope,
		},
		{
			name:    "nil account UUID",
			store:   &sessionStoreStub{},
			scope:   transporttelegram.SessionScope{OperatorID: validScope().OperatorID},
			appID:   123,
			appHash: secret,
			want:    errInvalidScope,
		},
		{
			name:    "nonpositive app ID",
			store:   &sessionStoreStub{},
			scope:   validScope(),
			appID:   0,
			appHash: secret,
			want:    errInvalidAppID,
		},
		{
			name:    "blank app hash",
			store:   &sessionStoreStub{},
			scope:   validScope(),
			appID:   123,
			appHash: " \t\n ",
			want:    errBlankAppHash,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called = false
			client, err := New(test.store).NewClient(test.scope, test.appID, test.appHash)
			if client != nil {
				t.Fatal("client is non-nil for invalid input")
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if called {
				t.Fatal("gotd constructor was called for invalid input")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("validation error leaked app hash: %q", err)
			}
		})
	}
}

func TestNewClientConstructionIsInert(t *testing.T) {
	client, err := New(panicSessionStore{}).NewClient(validScope(), 123, "app-hash")
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if client == nil {
		t.Fatal("NewClient() returned a nil gotd client")
	}
}

func TestNewClientCapturesCredentialsAndOnlySessionStorageOption(t *testing.T) {
	store := &sessionStoreStub{}
	scope := validScope()
	const (
		appID   = 456
		appHash = "hash-is-captured-by-the-constructor-only"
	)

	var capturedID int
	var capturedHash string
	var capturedOptions gotdtelegram.Options
	previous := newClient
	newClient = func(id int, hash string, options gotdtelegram.Options) *gotdtelegram.Client {
		capturedID = id
		capturedHash = hash
		capturedOptions = options
		return nil
	}
	t.Cleanup(func() { newClient = previous })

	client, err := New(store).NewClient(scope, appID, appHash)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if client != nil {
		t.Fatal("test constructor returned an unexpected client")
	}
	if capturedID != appID || capturedHash != appHash {
		t.Fatalf("constructor credentials = (%d, %q), want (%d, %q)", capturedID, capturedHash, appID, appHash)
	}

	storage, ok := capturedOptions.SessionStorage.(*scopedSessionStorage)
	if !ok || storage == nil {
		t.Fatalf("SessionStorage = %T, want non-nil *scopedSessionStorage", capturedOptions.SessionStorage)
	}
	if storage.store != store || storage.scope != scope {
		t.Fatalf("captured storage = %+v, want store and scope captured by value", storage)
	}
	if !reflect.DeepEqual(capturedOptions, gotdtelegram.Options{SessionStorage: storage}) {
		t.Fatalf("options contain fields other than SessionStorage: %+v", capturedOptions)
	}
}

func TestNewClientWithConnectionStateCapturesOnlyDedicatedObserver(t *testing.T) {
	store := &sessionStoreStub{}
	scope := validScope()
	var capturedOptions gotdtelegram.Options
	previous := newClient
	newClient = func(_ int, _ string, options gotdtelegram.Options) *gotdtelegram.Client {
		capturedOptions = options
		return nil
	}
	t.Cleanup(func() { newClient = previous })

	var states []gotdtelegram.ConnectionState
	observer := func(state gotdtelegram.ConnectionState) {
		states = append(states, state)
	}
	client, err := New(store).NewClientWithConnectionState(scope, 456, "app-hash", observer)
	if err != nil {
		t.Fatalf("NewClientWithConnectionState() error = %v", err)
	}
	if client != nil {
		t.Fatal("test constructor returned an unexpected client")
	}
	storage, ok := capturedOptions.SessionStorage.(*scopedSessionStorage)
	if !ok || storage == nil {
		t.Fatalf("SessionStorage = %T, want non-nil *scopedSessionStorage", capturedOptions.SessionStorage)
	}
	if capturedOptions.OnConnectionState == nil {
		t.Fatal("OnConnectionState was not captured")
	}
	optionsWithoutObserver := capturedOptions
	optionsWithoutObserver.OnConnectionState = nil
	if !reflect.DeepEqual(optionsWithoutObserver, gotdtelegram.Options{SessionStorage: storage}) {
		t.Fatalf("options contain fields other than session storage and observer: %+v", capturedOptions)
	}

	capturedOptions.OnConnectionState(gotdtelegram.ConnectionStateReady)
	if !reflect.DeepEqual(states, []gotdtelegram.ConnectionState{gotdtelegram.ConnectionStateReady}) {
		t.Fatalf("observed states = %v, want ready callback", states)
	}
}

func TestScopedSessionStorageLoad(t *testing.T) {
	data := []byte("opaque gotd session")
	tests := []struct {
		name      string
		loaded    transporttelegram.Session
		loadErr   error
		wantData  []byte
		wantError error
	}{
		{
			name:      "owned account without session",
			loaded:    transporttelegram.Session{Bytes: []byte("ignored"), Present: false},
			wantError: session.ErrNotFound,
		},
		{
			name:     "present session",
			loaded:   transporttelegram.Session{Bytes: data, Present: true},
			wantData: data,
		},
		{
			name:      "unauthorized",
			loadErr:   transporttelegram.ErrSessionUnauthorized,
			wantError: transporttelegram.ErrSessionUnauthorized,
		},
		{
			name:      "corrupt",
			loadErr:   transporttelegram.ErrSessionCorrupt,
			wantError: transporttelegram.ErrSessionCorrupt,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &sessionStoreStub{loadSession: test.loaded, loadErr: test.loadErr}
			storage := &scopedSessionStorage{store: store, scope: validScope()}
			gotData, err := storage.LoadSession(context.Background())
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("LoadSession() error = %v, want %v", err, test.wantError)
				}
				if gotData != nil {
					t.Fatalf("LoadSession() data = %q on error, want nil", gotData)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadSession() error = %v", err)
			}
			if !reflect.DeepEqual(gotData, test.wantData) {
				t.Fatalf("LoadSession() data = %q, want %q", gotData, test.wantData)
			}
			if len(gotData) != 0 && &gotData[0] != &test.wantData[0] {
				t.Fatal("present session bytes were copied")
			}
		})
	}
}

func TestScopedSessionStorageStoreDelegatesExactly(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "request")
	scope := validScope()
	data := []byte("opaque session bytes")
	underlying := errors.New("database unavailable")
	store := &sessionStoreStub{storeErr: underlying}
	storage := &scopedSessionStorage{store: store, scope: scope}

	err := storage.StoreSession(ctx, data)
	if !errors.Is(err, underlying) {
		t.Fatalf("StoreSession() error = %v, want wrapped database error", err)
	}
	if store.storeContext != ctx {
		t.Fatal("StoreSession() did not pass the original context")
	}
	if store.storeScope != scope {
		t.Fatalf("StoreSession() scope = %+v, want %+v", store.storeScope, scope)
	}
	if len(store.storeData) != len(data) || &store.storeData[0] != &data[0] {
		t.Fatal("StoreSession() did not pass the original byte slice")
	}

	store.storeErr = nil
	if err := storage.StoreSession(ctx, data); err != nil {
		t.Fatalf("StoreSession() successful call error = %v", err)
	}
}

func TestNewClientConcurrentConstructionKeepsScopesIsolated(t *testing.T) {
	store := &sessionStoreStub{}
	factory := New(store)
	scopes := []transporttelegram.SessionScope{
		validScope(),
		{OperatorID: uuid.MustParse("33333333-3333-4333-8333-333333333333"), AccountID: uuid.MustParse("44444444-4444-4444-8444-444444444444")},
		{OperatorID: uuid.MustParse("55555555-5555-4555-8555-555555555555"), AccountID: uuid.MustParse("66666666-6666-4666-8666-666666666666")},
		{OperatorID: uuid.MustParse("77777777-7777-4777-8777-777777777777"), AccountID: uuid.MustParse("88888888-8888-4888-8888-888888888888")},
	}

	var (
		mu       sync.Mutex
		storages = make([]*scopedSessionStorage, 0, len(scopes))
	)
	previous := newClient
	newClient = func(_ int, _ string, options gotdtelegram.Options) *gotdtelegram.Client {
		storage, ok := options.SessionStorage.(*scopedSessionStorage)
		if !ok {
			panic(fmt.Sprintf("unexpected storage type %T", options.SessionStorage))
		}
		mu.Lock()
		storages = append(storages, storage)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { newClient = previous })

	var wait sync.WaitGroup
	wait.Add(len(scopes))
	for index, scope := range scopes {
		go func(index int, scope transporttelegram.SessionScope) {
			defer wait.Done()
			if _, err := factory.NewClient(scope, 100+index, fmt.Sprintf("hash-%d", index)); err != nil {
				t.Errorf("NewClient() error = %v", err)
			}
		}(index, scope)
	}
	wait.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(storages) != len(scopes) {
		t.Fatalf("captured %d adapters, want %d", len(storages), len(scopes))
	}
	seen := make(map[*scopedSessionStorage]struct{}, len(storages))
	for _, storage := range storages {
		if _, exists := seen[storage]; exists {
			t.Fatal("concurrent client constructions shared a session adapter")
		}
		seen[storage] = struct{}{}
	}
	for _, storage := range storages {
		matched := false
		for _, scope := range scopes {
			if storage.scope == scope {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("adapter captured unexpected scope %+v", storage.scope)
		}
	}
}
