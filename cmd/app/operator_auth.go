package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/config"
	operatoraccountcommands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	applicationoperatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	operatoraccountrequests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
	"github.com/notrodans/cresora/internal/entrypoint/http/operatoraccounts"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	pgoperatoraccounts "github.com/notrodans/cresora/internal/infrastracture/storage/pg/operatoraccounts"
	transportoperatoraccountauth "github.com/notrodans/cresora/internal/transport/telegram/operatoraccountauth"
)

type operatorAuthService interface {
	Recover(context.Context) error
	Shutdown(context.Context) error
}

// operatorAuthRuntime is the one process-local owner used by both the
// transport adapter and the application lifecycle port. Keeping those two
// capabilities on one value prevents a second owner from being composed.
type operatorAuthRuntime interface {
	transportoperatoraccountauth.Runtime
	applicationoperatoraccountauth.RuntimeStopper
}

type sharedRuntimeStopper interface {
	Stop(context.Context) error
}

type operatorAuthPorts struct {
	start    operatoraccountcommands.Start
	code     operatoraccountcommands.Code
	password operatoraccountcommands.Password
	cancel   operatoraccountcommands.Cancel
	status   operatoraccountrequests.Status
}

type operatorAuthDatabaseCloser interface {
	Close()
}

type operatorAuthLifecycle struct {
	service operatorAuthService
}

func composeOperatorAuth(
	rootContext context.Context,
	cfg *config.Config,
	database *pgxpool.Pool,
	router chi.Router,
	principalProvider principal.Provider,
	publicOrigin string,
	runtime operatorAuthRuntime,
) (*operatorAuthLifecycle, error) {
	if cfg == nil {
		return nil, errors.New("telegram authentication configuration is required")
	}
	if !cfg.TelegramAuthEnabled {
		return nil, nil
	}
	if runtime == nil {
		return nil, errors.New("telegram authentication requires the shared telegram account runtime")
	}

	provider := transportoperatoraccountauth.New(runtime)
	persistence := pgoperatoraccounts.New(database)
	service := applicationoperatoraccountauth.NewService(persistence, provider, runtime)
	commands := operatoraccountcommands.NewApplication(service)
	ports := operatorAuthPorts{
		start:    commands.Start,
		code:     commands.Code,
		password: commands.Password,
		cancel:   commands.Cancel,
		status:   operatoraccountrequests.NewStatus(service),
	}

	return orchestrateOperatorAuth(rootContext, service, func() {
		registerLiveOperatorAuth(
			router,
			ports,
			principalProvider,
			publicOrigin,
			operatorAuthRouteOptions(cfg),
		)
	})
}

// orchestrateOperatorAuth is deliberately limited to lifecycle ordering. All
// production dependencies are constructed explicitly by root composition;
// this seam only lets ordering tests use already-created values.
func orchestrateOperatorAuth(
	rootContext context.Context,
	service operatorAuthService,
	register func(),
) (*operatorAuthLifecycle, error) {
	if failure := service.Recover(rootContext); failure != nil {
		cleanupFailure := stopOperatorAuthService(context.Background(), service)
		if cleanupFailure != nil {
			return nil, errors.Join(
				fmt.Errorf("recover operator account authentication: %w", failure),
				cleanupFailure,
			)
		}
		return nil, fmt.Errorf("recover operator account authentication: %w", failure)
	}

	register()
	return &operatorAuthLifecycle{service: service}, nil
}

func validateTelegramRuntimeConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("telegram runtime configuration is required")
	}
	if cfg.TelegramAPIID <= 0 {
		return errors.New("telegram configuration TELEGRAM_API_ID must be positive when the shared runtime is enabled")
	}
	if !cfg.TelegramAPIHash.Configured() {
		return errors.New("telegram configuration TELEGRAM_API_HASH is required when the shared runtime is enabled")
	}
	if cfg.TelegramSessionKeyID == "" {
		return errors.New("telegram configuration TELEGRAM_SESSION_KEY_ID is required when the shared runtime is enabled")
	}
	if !cfg.TelegramSessionEncryptionKey.Configured() {
		return errors.New("telegram configuration TELEGRAM_SESSION_ENCRYPTION_KEY is required when the shared runtime is enabled")
	}
	return nil
}

// validateOperatorAuthConfig remains as a compatibility seam for direct
// package tests. Runtime validation is owned by root composition and is
// intentionally independent of the HTTP route flag.
func validateOperatorAuthConfig(cfg *config.Config) error {
	return validateTelegramRuntimeConfig(cfg)
}

func operatorAuthRouteOptions(cfg *config.Config) operatoraccounts.RouteOptions {
	return operatoraccounts.RouteOptions{
		Mode:        operatoraccounts.RouteLive,
		Environment: operatoraccounts.DeploymentEnvironment(cfg.Env),
		Cookie:      operatoraccounts.NewCookieConfig(cfg.SessionCookieSecure(), cfg.SessionCookieAllowsInsecureLocal()),
	}
}

func registerLiveOperatorAuth(
	router chi.Router,
	ports operatorAuthPorts,
	principalProvider principal.Provider,
	publicOrigin string,
	options operatoraccounts.RouteOptions,
) {
	authenticationRouter := operatoraccounts.NewWithPhoneAuth(
		ports.start,
		ports.code,
		ports.password,
		ports.cancel,
		ports.status,
		principalProvider,
		publicOrigin,
		options,
	)
	router.Mount("/", authenticationRouter)
}

func registerDisabledOperatorAuth(router chi.Router, principalProvider principal.Provider, cfg *config.Config) {
	operatoraccounts.RegisterWithOptions(
		router,
		nil,
		nil,
		nil,
		nil,
		nil,
		principalProvider,
		cfg.PublicOrigin.String(),
		operatoraccounts.RouteOptions{
			Mode:        operatoraccounts.RouteDisabled,
			Environment: operatoraccounts.DeploymentEnvironment(cfg.Env),
			Cookie:      operatoraccounts.NewCookieConfig(cfg.SessionCookieSecure(), cfg.SessionCookieAllowsInsecureLocal()),
		},
	)
}

var operatorAuthShutdownTimeout = 10 * time.Second

func stopOperatorAuthService(
	ctx context.Context,
	service operatorAuthService,
) error {
	if service == nil {
		return nil
	}
	serviceContext, cancel := context.WithTimeout(ctx, operatorAuthShutdownTimeout)
	failure := service.Shutdown(serviceContext)
	cancel()
	if failure != nil {
		return fmt.Errorf("shut down operator account authentication service: %w", failure)
	}
	return nil
}

func stopSharedRuntime(ctx context.Context, runtime sharedRuntimeStopper) error {
	if runtime == nil {
		return nil
	}
	runtimeContext, cancel := context.WithTimeout(ctx, operatorAuthShutdownTimeout)
	failure := runtime.Stop(runtimeContext)
	cancel()
	if failure != nil {
		return fmt.Errorf("stop telegram account runtime: %w", failure)
	}
	return nil
}

func shutdownApplicationResources(
	lifecycle *operatorAuthLifecycle,
	runtime sharedRuntimeStopper,
	database operatorAuthDatabaseCloser,
) error {
	var service operatorAuthService
	if lifecycle != nil {
		service = lifecycle.service
	}
	failure := stopOperatorAuthService(context.Background(), service)
	failure = errors.Join(failure, stopSharedRuntime(context.Background(), runtime))
	if database != nil {
		database.Close()
	}
	return failure
}
