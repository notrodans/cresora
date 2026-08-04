package main

import (
	"context"
	"errors"
	"fmt"
	slogger "log/slog"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/config"
	application "github.com/notrodans/cresora/internal/application"
	operatoraccountcommands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	applicationoperatoraccountauth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	operatoraccountrequests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/entrypoint/http/operatoraccounts"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	pgoperatoraccounts "github.com/notrodans/cresora/internal/infrastracture/storage/pg/operatoraccounts"
	transportoperatoraccountauth "github.com/notrodans/cresora/internal/transport/telegram/operatoraccountauth"
	transportoperatoraccounts "github.com/notrodans/cresora/internal/transport/telegram/operatoraccounts"
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

type operatorAccountComposition struct {
	runtime    operatorAuthRuntime
	store      *pgoperatoraccounts.Store
	disconnect *applicationoperatoraccounts.Service
}

type operatorAccountDisconnectCommand struct {
	service *applicationoperatoraccounts.Service
}

func (command operatorAccountDisconnectCommand) Execute(
	ctx context.Context,
	actor application.Actor,
	accountID operatoraccount.ID,
) (applicationoperatoraccounts.DisconnectResult, error) {
	return command.service.Disconnect(ctx, actor, accountID)
}

type authenticationPersistenceWithoutRemoteIntents struct {
	applicationoperatoraccountauth.AuthenticationPersistence
	remoteIntents applicationoperatoraccounts.RemoteLogoutIntentLister
}

func (persistence authenticationPersistenceWithoutRemoteIntents) ListOrphanAuthenticationLifecycles(
	ctx context.Context,
) ([]applicationoperatoraccountauth.AuthTarget, error) {
	targets, err := persistence.AuthenticationPersistence.ListOrphanAuthenticationLifecycles(ctx)
	if err != nil {
		return nil, err
	}
	remoteTargets, err := persistence.remoteIntents.ListRemoteLogoutIntents(ctx)
	if err != nil {
		return nil, fmt.Errorf("list remote logout intents while recovering authentication: %w", err)
	}
	remote := make(map[operatorAccountTargetKey]struct{}, len(remoteTargets))
	for _, target := range remoteTargets {
		remote[operatorAccountTargetKey{
			operatorID: target.Actor.OperatorID,
			accountID:  target.AccountID.UUID(),
			version:    target.Version,
		}] = struct{}{}
	}
	filtered := make([]applicationoperatoraccountauth.AuthTarget, 0, len(targets))
	for _, target := range targets {
		if _, excluded := remote[operatorAccountTargetKey{
			operatorID: target.Actor.OperatorID,
			accountID:  target.AccountID.UUID(),
			version:    target.Version,
		}]; excluded {
			continue
		}
		filtered = append(filtered, target)
	}
	return filtered, nil
}

type operatorAccountTargetKey struct {
	operatorID uuid.UUID
	accountID  uuid.UUID
	version    operatoraccount.Version
}

type operatorAuthDatabaseCloser interface {
	Close()
}

type operatorAuthLifecycle struct {
	service    operatorAuthService
	disconnect *applicationoperatoraccounts.Service
}

func composeOperatorAccounts(
	database *pgxpool.Pool,
	runtime operatorAuthRuntime,
) (*operatorAccountComposition, error) {
	if runtime == nil {
		return nil, errors.New("telegram account disconnect requires the shared telegram account runtime")
	}
	revokerRuntime, ok := any(runtime).(transportoperatoraccounts.Runtime)
	if !ok || revokerRuntime == nil {
		return nil, errors.New("telegram account disconnect requires the shared runtime revocation capability")
	}
	store := pgoperatoraccounts.New(database)
	revoker := transportoperatoraccounts.New(revokerRuntime)
	return &operatorAccountComposition{
		runtime:    runtime,
		store:      store,
		disconnect: applicationoperatoraccounts.NewService(store, revoker),
	}, nil
}

func composeOperatorAuthWithComposition(
	rootContext context.Context,
	cfg *config.Config,
	router chi.Router,
	principalProvider principal.Provider,
	publicOrigin string,
	composition *operatorAccountComposition,
) (*operatorAuthLifecycle, error) {
	if cfg == nil {
		return nil, errors.New("telegram authentication configuration is required")
	}
	if !cfg.TelegramAuthEnabled {
		return nil, nil
	}
	if composition == nil || composition.runtime == nil || composition.store == nil || composition.disconnect == nil {
		return nil, errors.New("telegram authentication requires the composed operator account services")
	}

	provider := transportoperatoraccountauth.New(composition.runtime)
	authPersistence := authenticationPersistenceWithoutRemoteIntents{
		AuthenticationPersistence: composition.store,
		remoteIntents:             composition.store,
	}
	applicationOperatorAccountAuthService := applicationoperatoraccountauth.NewService(authPersistence, provider, composition.runtime)
	commands := operatoraccountcommands.NewApplication(applicationOperatorAccountAuthService)
	ports := operatorAuthPorts{
		start:    commands.Start,
		code:     commands.Code,
		password: commands.Password,
		cancel:   commands.Cancel,
		status:   operatoraccountrequests.NewStatus(applicationOperatorAccountAuthService),
	}

	lifecycle, failure := orchestrateOperatorAuth(rootContext, applicationOperatorAccountAuthService, func() {
		registerLiveOperatorAuthWithDisconnect(
			router,
			ports,
			principalProvider,
			publicOrigin,
			operatorAuthRouteOptions(cfg),
			operatorAccountDisconnectCommand{service: composition.disconnect},
		)
	})
	if lifecycle != nil {
		lifecycle.disconnect = composition.disconnect
	}
	return lifecycle, failure
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
	registerLiveOperatorAuthWithDisconnect(router, ports, principalProvider, publicOrigin, options, nil)
}

func registerLiveOperatorAuthWithDisconnect(
	router chi.Router,
	ports operatorAuthPorts,
	principalProvider principal.Provider,
	publicOrigin string,
	options operatoraccounts.RouteOptions,
	disconnectCommand operatoraccountsDisconnectCommand,
) {
	authenticationRouter := operatoraccounts.NewWithPhoneAuthAndDisconnect(
		ports.start,
		ports.code,
		ports.password,
		ports.cancel,
		ports.status,
		disconnectCommand,
		principalProvider,
		publicOrigin,
		options,
	)
	router.Mount("/", authenticationRouter)
}

// operatoraccountsDisconnectCommand is intentionally structural. The concrete
// command is owned by this composition root while the HTTP package consumes
// only its narrow application port.
type operatoraccountsDisconnectCommand interface {
	Execute(context.Context, application.Actor, operatoraccount.ID) (applicationoperatoraccounts.DisconnectResult, error)
}

type operatorAccountRecovery interface {
	Recover(context.Context) (applicationoperatoraccounts.RecoveryResult, error)
}

func recoverOperatorAccountDisconnect(
	ctx context.Context,
	service operatorAccountRecovery,
	log *slogger.Logger,
) error {
	result, failure := service.Recover(ctx)
	if failure != nil {
		return fmt.Errorf("recover telegram account disconnect intents: %w", failure)
	}
	if result.Pending == 0 {
		return nil
	}
	if log == nil {
		log = slogger.Default()
	}
	log.LogAttrs(
		ctx,
		slogger.LevelWarn,
		"telegram account disconnect recovery remains pending",
		slogger.Int("attempted", result.Attempted),
		slogger.Int("completed", result.Completed),
		slogger.Int("pending", result.Pending),
		slogger.Int("skipped", result.Skipped),
		slogger.Int("pending_flood_wait", result.PendingByKind[applicationoperatoraccounts.RemoteLogoutFailureFloodWait]),
		slogger.Int("pending_transient", result.PendingByKind[applicationoperatoraccounts.RemoteLogoutFailureTransient]),
		slogger.Int("pending_ambiguous", result.PendingByKind[applicationoperatoraccounts.RemoteLogoutFailureAmbiguous]),
		slogger.Int("pending_permanent", result.PendingByKind[applicationoperatoraccounts.RemoteLogoutFailurePermanent]),
		slogger.Int("pending_unavailable", result.PendingByKind[applicationoperatoraccounts.RemoteLogoutFailureUnavailable]),
	)
	return nil
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
