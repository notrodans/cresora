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
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg"
	pgoperatoraccounts "github.com/notrodans/cresora/internal/infrastracture/storage/pg/operatoraccounts"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
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
	runtime operatorAuthRuntime
}

func composeOperatorAuth(
	rootContext context.Context,
	cfg *config.Config,
	database *pgxpool.Pool,
	router chi.Router,
	principalProvider principal.Provider,
	publicOrigin string,
) (*operatorAuthLifecycle, error) {
	if cfg == nil {
		return nil, errors.New("telegram authentication configuration is required")
	}
	if !cfg.TelegramAuthEnabled {
		return nil, nil
	}
	if !cfg.WebOnly {
		return nil, errors.New("WEB_ONLY=false requires configured Telegram account adapters")
	}
	if failure := validateOperatorAuthConfig(cfg); failure != nil {
		return nil, failure
	}

	sessionStore, failure := pg.NewTelegramSessionStore(
		database,
		cfg.TelegramSessionKeyID,
		cfg.TelegramSessionEncryptionKey.Bytes(),
	)
	if failure != nil {
		return nil, fmt.Errorf("construct encrypted telegram session store: %w", failure)
	}
	factory := gotdclient.New(sessionStore)
	runtime, failure := accountowner.NewRegistry(accountowner.RegistryConfig{
		Factory: factory,
		AppID:   cfg.TelegramAPIID,
		AppHash: cfg.TelegramAPIHash.Value(),
	})
	if failure != nil {
		return nil, fmt.Errorf("construct telegram account runtime: %w", failure)
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

	return orchestrateOperatorAuth(rootContext, service, runtime, func() {
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
// production dependencies are constructed explicitly in composeOperatorAuth;
// this seam only lets ordering tests use already-created values.
func orchestrateOperatorAuth(
	rootContext context.Context,
	service operatorAuthService,
	runtime operatorAuthRuntime,
	register func(),
) (*operatorAuthLifecycle, error) {
	if failure := service.Recover(rootContext); failure != nil {
		cleanupFailure := stopOperatorAuthRuntime(context.Background(), service, runtime)
		if cleanupFailure != nil {
			return nil, errors.Join(
				fmt.Errorf("recover operator account authentication: %w", failure),
				cleanupFailure,
			)
		}
		return nil, fmt.Errorf("recover operator account authentication: %w", failure)
	}

	register()
	return &operatorAuthLifecycle{service: service, runtime: runtime}, nil
}

func validateOperatorAuthConfig(cfg *config.Config) error {
	if cfg.TelegramAPIID <= 0 {
		return errors.New("telegram configuration TELEGRAM_API_ID must be positive when authentication is enabled")
	}
	if !cfg.TelegramAPIHash.Configured() {
		return errors.New("telegram configuration TELEGRAM_API_HASH is required when authentication is enabled")
	}
	if cfg.TelegramSessionKeyID == "" {
		return errors.New("telegram configuration TELEGRAM_SESSION_KEY_ID is required when authentication is enabled")
	}
	if !cfg.TelegramSessionEncryptionKey.Configured() {
		return errors.New("telegram configuration TELEGRAM_SESSION_ENCRYPTION_KEY is required when authentication is enabled")
	}
	return nil
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

func stopOperatorAuthRuntime(
	ctx context.Context,
	service operatorAuthService,
	runtime operatorAuthRuntime,
) error {
	var failures []error
	if service != nil {
		serviceContext, cancel := context.WithTimeout(ctx, operatorAuthShutdownTimeout)
		failure := service.Shutdown(serviceContext)
		cancel()
		if failure != nil {
			failures = append(failures, fmt.Errorf("shut down operator account authentication service: %w", failure))
		}
	}
	if runtime != nil {
		runtimeContext, cancel := context.WithTimeout(context.Background(), operatorAuthShutdownTimeout)
		failure := runtime.Stop(runtimeContext)
		cancel()
		if failure != nil {
			failures = append(failures, fmt.Errorf("stop Telegram account runtime: %w", failure))
		}
	}
	return errors.Join(failures...)
}

func shutdownApplicationResources(lifecycle *operatorAuthLifecycle, database operatorAuthDatabaseCloser) error {
	var service operatorAuthService
	var runtime operatorAuthRuntime
	if lifecycle != nil {
		service = lifecycle.service
		runtime = lifecycle.runtime
	}
	failure := stopOperatorAuthRuntime(context.Background(), service, runtime)
	if database != nil {
		database.Close()
	}
	return failure
}
