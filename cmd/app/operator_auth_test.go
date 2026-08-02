package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	slogger "log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/notrodans/cresora/config"
	application "github.com/notrodans/cresora/internal/application"
	applicationoperatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

func TestComposeOperatorAuthDisabledDoesNotConstructRuntimeOrAdapter(t *testing.T) {
	cfg := validOperatorAuthConfig(t)
	cfg.TelegramAuthEnabled = false

	lifecycle, failure := composeOperatorAuth(
		context.Background(),
		cfg,
		nil,
		chi.NewRouter(),
		stubPrincipalProvider(),
		cfg.PublicOrigin.String(),
		nil,
	)
	if failure != nil {
		t.Fatalf("compose disabled operator authentication: %v", failure)
	}
	if lifecycle != nil {
		t.Fatal("disabled operator authentication constructed a lifecycle")
	}
	router := chi.NewRouter()
	registerDisabledOperatorAuth(router, stubPrincipalProvider(), cfg)
	request := httptest.NewRequest(http.MethodGet, "/operator-accounts/authenticate", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled operator authentication status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestComposeOperatorAuthRequiresSharedRuntime(t *testing.T) {
	cfg := validOperatorAuthConfig(t)
	cfg.WebOnly = false

	lifecycle, failure := composeOperatorAuth(
		context.Background(),
		cfg,
		nil,
		chi.NewRouter(),
		stubPrincipalProvider(),
		cfg.PublicOrigin.String(),
		nil,
	)
	if lifecycle != nil {
		t.Fatal("authentication without a shared runtime returned a live lifecycle")
	}
	if failure == nil || !containsText(failure.Error(), "shared telegram account runtime") {
		t.Fatalf("missing shared runtime failure = %v", failure)
	}
}

func TestComposeOperatorAccountsBuildsOneDisconnectServiceForTheSharedRuntime(t *testing.T) {
	runtime := &fakeOperatorAuthRuntime{events: new([]string)}
	composition, failure := composeOperatorAccounts(nil, runtime)
	if failure != nil {
		t.Fatalf("compose operator account services: %v", failure)
	}
	if composition == nil || composition.store == nil || composition.disconnect == nil {
		t.Fatalf("incomplete operator account composition: %#v", composition)
	}
	if composition.runtime != runtime {
		t.Fatal("operator account composition did not retain the shared runtime")
	}
	if _, ok := any(runtime).(transportRuntimeRevoker); !ok {
		t.Fatal("shared runtime does not satisfy the revocation boundary")
	}
}

func TestRecoverOperatorAccountDisconnectPendingIsNonfatalAndSafe(t *testing.T) {
	var logs bytes.Buffer
	logger := slogger.New(slogger.NewTextHandler(&logs, nil))
	service := &fakeOperatorAccountRecovery{result: applicationoperatoraccounts.RecoveryResult{
		Attempted: 1,
		Pending:   1,
		PendingByKind: map[applicationoperatoraccounts.RemoteLogoutFailureKind]int{
			applicationoperatoraccounts.RemoteLogoutFailureFloodWait: 1,
		},
	}}
	if failure := recoverOperatorAccountDisconnect(context.Background(), service, logger); failure != nil {
		t.Fatalf("pending disconnect recovery: %v", failure)
	}
	output := logs.String()
	for _, field := range []string{"pending=1", "pending_flood_wait=1"} {
		if !strings.Contains(output, field) {
			t.Fatalf("pending recovery log omitted %q: %q", field, output)
		}
	}
	if strings.Contains(output, "provider") || strings.Contains(output, "secret") {
		t.Fatalf("pending recovery log contains unsafe details: %q", output)
	}
}

func TestRecoverOperatorAccountDisconnectFatalFailureStopsStartup(t *testing.T) {
	expected := errors.New("durable state failure")
	service := &fakeOperatorAccountRecovery{failure: expected}
	if failure := recoverOperatorAccountDisconnect(context.Background(), service, slogger.New(slogger.NewTextHandler(&bytes.Buffer{}, nil))); failure == nil || !errors.Is(failure, expected) {
		t.Fatalf("fatal disconnect recovery = %v, want %v", failure, expected)
	}
}

func TestOrchestrateOperatorAuthRecoversBeforeRegisteringLiveRoute(t *testing.T) {
	events := make([]string, 0, 3)
	service := &fakeOperatorAuthService{events: &events}

	lifecycle, failure := orchestrateOperatorAuth(
		context.Background(),
		service,
		func() { events = append(events, "route") },
	)
	if failure != nil {
		t.Fatalf("compose live operator authentication: %v", failure)
	}
	if lifecycle == nil {
		t.Fatal("live operator authentication returned no lifecycle")
	}
	want := []string{"recover", "route"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("composition order = %v, want %v", events, want)
	}
}

func TestComposeOperatorAuthRecoveryFailureDoesNotRegisterLiveRoute(t *testing.T) {
	events := make([]string, 0, 8)
	expected := errors.New("recovery failed")
	service := &fakeOperatorAuthService{events: &events, recoverErr: expected}
	lifecycle, failure := orchestrateOperatorAuth(
		context.Background(),
		service,
		func() { events = append(events, "route") },
	)
	if lifecycle != nil {
		t.Fatal("recovery failure returned a live lifecycle")
	}
	if !errors.Is(failure, expected) {
		t.Fatalf("recovery failure = %v, want %v", failure, expected)
	}
	if got := events[len(events)-1:]; !reflect.DeepEqual(got, []string{"shutdown"}) {
		t.Fatalf("recovery cleanup order = %v, want [shutdown]", got)
	}
	for _, event := range events {
		if event == "route" {
			t.Fatalf("live route registered after recovery failure: %v", events)
		}
	}
}

func TestShutdownApplicationResourcesStopsHTTPThenServiceRuntimeAndDatabase(t *testing.T) {
	events := make([]string, 0, 4)
	server := &orderedServerController{events: &events}
	if failure := shutdownServer(server); failure != nil {
		t.Fatalf("stop HTTP admission: %v", failure)
	}

	service := &fakeOperatorAuthService{events: &events}
	runtime := &fakeOperatorAuthRuntime{events: &events}
	database := fakeOperatorAuthDatabase{events: &events}
	lifecycle := &operatorAuthLifecycle{service: service}
	if failure := shutdownApplicationResources(lifecycle, runtime, database); failure != nil {
		t.Fatalf("shutdown application resources: %v", failure)
	}

	want := []string{"http", "shutdown", "stop", "database"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("shutdown order = %v, want %v", events, want)
	}
}

func TestStopOperatorAuthServiceUsesShutdownTimeout(t *testing.T) {
	originalTimeout := operatorAuthShutdownTimeout
	operatorAuthShutdownTimeout = time.Millisecond
	defer func() { operatorAuthShutdownTimeout = originalTimeout }()

	events := make([]string, 0, 2)
	service := &fakeOperatorAuthService{
		events: &events,
		shutdown: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	failure := stopOperatorAuthService(context.Background(), service)
	if failure == nil || !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("service shutdown failure = %v, want deadline exceeded", failure)
	}
	if !reflect.DeepEqual(events, []string{"shutdown"}) {
		t.Fatalf("shutdown events = %v, want [shutdown]", events)
	}
}

type fakeOperatorAuthService struct {
	events     *[]string
	recoverErr error
	shutdown   func(context.Context) error
}

func (service *fakeOperatorAuthService) Recover(context.Context) error {
	*service.events = append(*service.events, "recover")
	return service.recoverErr
}

func (service *fakeOperatorAuthService) Shutdown(ctx context.Context) error {
	*service.events = append(*service.events, "shutdown")
	if service.shutdown != nil {
		return service.shutdown(ctx)
	}
	return nil
}

type fakeOperatorAuthRuntime struct {
	events *[]string
	stop   func(context.Context) error
}

type transportRuntimeRevoker interface {
	RevokeAndStop(context.Context, applicationoperatoraccounts.RuntimeTarget, accountowner.ClientCallback) error
}

func (runtime *fakeOperatorAuthRuntime) Execute(context.Context, applicationoperatoraccountauth.AuthTarget, accountowner.ClientCallback) error {
	return nil
}

func (runtime *fakeOperatorAuthRuntime) StopAccount(context.Context, applicationoperatoraccountauth.AuthTarget) error {
	return nil
}

func (runtime *fakeOperatorAuthRuntime) Stop(ctx context.Context) error {
	*runtime.events = append(*runtime.events, "stop")
	if runtime.stop != nil {
		return runtime.stop(ctx)
	}
	return nil
}

func (runtime *fakeOperatorAuthRuntime) RevokeAndStop(context.Context, applicationoperatoraccounts.RuntimeTarget, accountowner.ClientCallback) error {
	return nil
}

type fakeOperatorAccountRecovery struct {
	result  applicationoperatoraccounts.RecoveryResult
	failure error
}

func (service *fakeOperatorAccountRecovery) Recover(context.Context) (applicationoperatoraccounts.RecoveryResult, error) {
	return service.result, service.failure
}

type fakeOperatorAuthDatabase struct {
	events *[]string
}

func (database fakeOperatorAuthDatabase) Close() {
	*database.events = append(*database.events, "database")
}

type orderedServerController struct {
	events *[]string
}

func (server *orderedServerController) Shutdown(context.Context) error {
	*server.events = append(*server.events, "http")
	return nil
}

func (server *orderedServerController) Close() error { return nil }

func validOperatorAuthConfig(t *testing.T) *config.Config {
	t.Helper()
	var hash config.SecretString
	if failure := hash.UnmarshalText([]byte("telegram-api-hash")); failure != nil {
		t.Fatalf("configure Telegram API hash: %v", failure)
	}
	var encryptionKey config.SessionEncryptionKey
	encodedKey := base64.StdEncoding.EncodeToString([]byte("01234567890123456789012345678901"))
	if failure := encryptionKey.UnmarshalText([]byte(encodedKey)); failure != nil {
		t.Fatalf("configure Telegram session encryption key: %v", failure)
	}
	return &config.Config{
		Env:                          config.Development,
		PublicOrigin:                 url.URL{Scheme: "http", Host: "localhost"},
		WebOnly:                      true,
		TelegramAuthEnabled:          true,
		TelegramAPIID:                12345,
		TelegramAPIHash:              hash,
		TelegramSessionKeyID:         "current",
		TelegramSessionEncryptionKey: encryptionKey,
	}
}

func stubPrincipalProvider() principal.Provider {
	return principal.ProviderFunc(func(*http.Request) (application.Actor, error) {
		return application.Actor{OperatorID: uuid.New()}, nil
	})
}
