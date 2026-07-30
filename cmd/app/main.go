package main

import (
	"context"
	"errors"
	"fmt"
	slogger "log/slog"
	"net/http"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/config"
	mailingconsolecommands "github.com/notrodans/cresora/internal/application/commands/mailing-console"
	operatorsessions "github.com/notrodans/cresora/internal/application/operatorsessions"
	mailingconsolerequests "github.com/notrodans/cresora/internal/application/requests/mailing-console"
	mailingconsole "github.com/notrodans/cresora/internal/application/services/mailingconsole"
	backgroundjobs "github.com/notrodans/cresora/internal/entrypoint/background"
	deliverybackground "github.com/notrodans/cresora/internal/entrypoint/background/delivery"
	"github.com/notrodans/cresora/internal/entrypoint/background/delivery/actor"
	deliveryreaper "github.com/notrodans/cresora/internal/entrypoint/background/deliveryreaper"
	deliveryreconciler "github.com/notrodans/cresora/internal/entrypoint/background/deliveryreconciler"
	"github.com/notrodans/cresora/internal/entrypoint/http/authentication"
	"github.com/notrodans/cresora/internal/entrypoint/http/console"
	"github.com/notrodans/cresora/internal/entrypoint/http/operatoraccounts"
	"github.com/notrodans/cresora/internal/infrastracture/logger/slog"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg"
	claims "github.com/notrodans/cresora/internal/infrastracture/storage/pg/claims"
	deliveries "github.com/notrodans/cresora/internal/infrastracture/storage/pg/deliveries"
	mailings "github.com/notrodans/cresora/internal/infrastracture/storage/pg/mailings"
	pgreaper "github.com/notrodans/cresora/internal/infrastracture/storage/pg/reaper"
	pgreconciler "github.com/notrodans/cresora/internal/infrastracture/storage/pg/reconciler"
	telegramaccount "github.com/notrodans/cresora/internal/transport/telegram/account"
)

const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	maxHeaderBytes    = 1 << 20
)

func main() {
	// signal.NotifyContext изменяет поведение контекста таким образом,
	// что при получении системных сигналов SIGINT или SIGTERM автоматически закрывается ctx.Done().
	root, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	// cancel() освобождает ресурсы и восстанавливает стандартное поведение операционной системы.
	// Под стандартным поведением подразумевается немедленное принудительное завершение процесса.
	defer cancel()
	if failure := runApplication(root, cancel); failure != nil {
		panic(failure)
	}
}

func logging(base *slogger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.NewString()

		logger := base.With(
			slogger.String("request_id", requestID),
			slogger.String("http_method", r.Method),
			slogger.String("http_path", r.URL.Path),
		)

		logger.Info("handling request")

		ctx := context.WithValue(r.Context(), slog.LoggerKey{}, logger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func runApplication(rootContext context.Context, cancel context.CancelFunc) error {
	rootDir, err := config.ProjectRoot()
	if err != nil {
		panic(err)
	}

	// Загружаем конфигурацию. В случае ошибки функция вызывает панику.
	// По соглашению префикс Must означает, что функция может вызвать панику.
	cfg := config.MustLoad(rootDir)

	log := slog.SetupLogger(cfg)

	migrationsPath := filepath.Join(rootDir, "migrations")
	pg.ExecuteMigrations(rootContext, cfg, log, migrationsPath)

	// Создаём пул соединений с PostgreSQL для повторного использования подключений.
	database, failure := pgxpool.New(rootContext, cfg.DbUrl)
	if failure != nil {
		return fmt.Errorf("open PostgreSQL database: %w", failure)
	}
	defer database.Close()

	// Lease recovery is transport-free and therefore runs in every mode,
	// including WEB_ONLY. The PostgreSQL adapter retains its own batch, grace,
	// and retry defaults; only the polling interval is application config.
	deliveryRecovery := pgreaper.New(database, pgreaper.Config{})
	reaperLoop := deliveryreaper.New(deliveryRecovery, deliveryreaper.Config{
		Interval: cfg.DeliveryReaperInterval,
	})
	runReconciler := pgreconciler.New(database, pgreconciler.Config{})
	reconcilerLoop := deliveryreconciler.New(runReconciler, deliveryreconciler.Config{
		Interval: cfg.DeliveryReconcilerInterval,
	})

	// Создаём сервис для работы с таблицами рассылок.
	service := mailingconsole.NewService(mailings.NewMailingConsole(database), mailings.NewMailings(database))
	credentialStore := pg.NewOperatorCredentialStore(database)
	sessionStore := pg.NewOperatorWebSessionStore(database)
	authenticationService := operatorsessions.NewService(credentialStore, sessionStore)
	cookieConfig := authentication.CookieConfig{
		Name:               cfg.SessionCookieName(),
		Secure:             cfg.SessionCookieSecure(),
		AllowInsecureLocal: cfg.SessionCookieAllowsInsecureLocal(),
	}
	sessionProvider := authentication.NewSessionProvider(authenticationService, cookieConfig)
	// Команда для создания драфта рассылки
	createDraft := mailingconsolecommands.NewCreateDraft(&service)
	// Команда для помещения рассылки в очередь
	queueMailing := mailingconsolecommands.NewQueue(&service)
	// Запрос борды
	dashboard := mailingconsolerequests.NewDashboard(&service)

	loggerMiddleware := func(next http.Handler) http.Handler {
		return logging(log, next)
	}

	router := chi.NewRouter()
	router.Use(loggerMiddleware)
	router.Use(middleware.Recoverer)
	authentication.Register(router, authenticationService, sessionProvider, cfg.PublicOrigin.String(), cookieConfig)
	// Telegram account sign-in remains unavailable until live Telegram
	// adapters are composed. Never wire the deterministic in-memory mock here:
	// this disabled route preserves the endpoint surface and returns a generic
	// 503 after principal authentication in every deployment environment.
	operatoraccounts.RegisterWithOptions(
		router,
		nil,
		nil,
		nil,
		nil,
		nil,
		sessionProvider,
		cfg.PublicOrigin.String(),
		operatoraccounts.RouteOptions{
			Mode:        operatoraccounts.RouteDisabled,
			Environment: operatoraccounts.DeploymentEnvironment(cfg.Env),
			Cookie:      operatoraccounts.NewCookieConfig(cfg.SessionCookieSecure(), cfg.SessionCookieAllowsInsecureLocal()),
		},
	)
	console.Register(router, createDraft, queueMailing, dashboard, sessionProvider, cfg.PublicOrigin.String(), log)

	// Инициализируем HTTP сервер
	server := &http.Server{
		Addr:              cfg.WebAddr.Host,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}

	// Канал ошибок фоновых обработчиков.
	var workerErrors <-chan error
	if !cfg.WebOnly {
		// Эти адаптеры остаются внешними до подключения жизненного цикла Telegram-аккаунтов.
		// Нельзя считать, что обработчики доставки запущены, пока адаптеры не настроены.
		var apis APIs
		targets := telegramaccount.NewTargets(pg.NewTelegramPeerLookup(database))
		if apis == nil || targets == nil {
			return errors.New("WEB_ONLY=false requires configured Telegram account adapters")
		}
		worker := make(chan error, 1)
		workerErrors = worker
		go func() {
			worker <- run(rootContext, database, apis, targets)
		}()
	}

	backgroundErrors := make(chan error, 1)
	backgroundSupervisor := backgroundjobs.NewRunner(
		[]backgroundjobs.Job{
			namedBackgroundJob("delivery reaper", reaperLoop.Run),
			namedBackgroundJob("delivery reconciler", reconcilerLoop.Run),
		},
		lifecycleWaitTimeout,
	)
	go func() {
		backgroundErrors <- backgroundSupervisor.Run(rootContext)
	}()

	return monitorRuntime(rootContext, cancel, server, server.ListenAndServe, workerErrors, backgroundErrors)
}

// APIs и Targets — это адаптеры уровня приложения вокруг существующего
// жизненного цикла аккаунтов gotd и проекций Telegram в PostgreSQL.
type APIs = telegramaccount.APIs
type Targets = telegramaccount.Targets

func run(context context.Context, database *pgxpool.Pool, apis APIs, targets Targets) error {
	deliveries := deliveries.NewDeliveries(database)
	commands := telegramaccount.NewCommands(apis, targets, deliveries)
	factory := actor.NewFactory(commands, 4, 32)
	supervisor := actor.NewSupervisor(context, factory)
	claims := claims.NewClaims(database, 5*time.Minute)
	pump := deliverybackground.New(claims, supervisor, 4, 250*time.Millisecond)

	failure := pump.Run(context)
	actorFailure := supervisor.Wait()
	if actorFailure != nil {
		return fmt.Errorf("run mailing delivery actors: %w", actorFailure)
	}
	if failure != nil && !errors.Is(failure, context.Err()) {
		return fmt.Errorf("run mailing delivery background: %w", failure)
	}
	return nil
}

type serverController interface {
	Shutdown(context.Context) error
	Close() error
}

type serveFunction func() error

func namedBackgroundJob(name string, job backgroundjobs.Job) backgroundjobs.Job {
	return func(context context.Context) error {
		if failure := job(context); failure != nil {
			return fmt.Errorf("%s: %w", name, failure)
		}
		return nil
	}
}

var lifecycleWaitTimeout = 10 * time.Second

var errReaperErrorsClosed = errors.New("delivery reaper supervision channel closed unexpectedly")

func monitorRuntime(
	root context.Context,
	cancel context.CancelFunc,
	server serverController,
	serve serveFunction,
	workerErrors <-chan error,
	backgroundErrors ...<-chan error,
) error {
	var reaperErrors <-chan error
	if len(backgroundErrors) > 0 {
		reaperErrors = backgroundErrors[0]
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- serve()
	}()

	for {
		select {
		case failure := <-serverErrors:
			failure = normalizeServerFailure(failure)
			cancel()
			shutdownFailure := shutdownServer(server)
			workerFailure := waitWorker(workerErrors)
			reaperFailure := waitReaper(reaperErrors)
			if failure != nil {
				return failure
			}
			if shutdownFailure != nil {
				return shutdownFailure
			}
			if workerFailure != nil {
				return fmt.Errorf("delivery worker stopped: %w", workerFailure)
			}
			if reaperFailure != nil && !errors.Is(reaperFailure, root.Err()) {
				return fmt.Errorf("delivery reaper stopped: %w", reaperFailure)
			}
			return errors.New("mailing console server stopped unexpectedly")
		case failure := <-workerErrors:
			shutdownRequested := root.Err() != nil
			cancel()
			shutdownFailure := shutdownServer(server)
			serverFailure := waitServer(serverErrors)
			reaperFailure := waitReaper(reaperErrors)
			if shutdownRequested && (failure == nil || errors.Is(failure, root.Err())) {
				if shutdownFailure != nil {
					return shutdownFailure
				}
				if reaperFailure != nil && !errors.Is(reaperFailure, root.Err()) {
					return fmt.Errorf("delivery reaper stopped: %w", reaperFailure)
				}
				return serverFailure
			}
			if failure == nil {
				return errors.New("delivery worker stopped unexpectedly")
			}
			if shutdownFailure != nil {
				return shutdownFailure
			}
			if serverFailure != nil {
				return serverFailure
			}
			return fmt.Errorf("delivery worker stopped: %w", failure)
		case failure, open := <-reaperErrors:
			if !open {
				failure = errReaperErrorsClosed
			}
			shutdownRequested := root.Err() != nil
			cancel()
			shutdownFailure := shutdownServer(server)
			serverFailure := waitServer(serverErrors)
			workerFailure := waitWorker(workerErrors)
			if shutdownFailure != nil {
				return shutdownFailure
			}
			if serverFailure != nil {
				return serverFailure
			}
			if workerFailure != nil && !errors.Is(workerFailure, root.Err()) {
				return fmt.Errorf("delivery worker stopped: %w", workerFailure)
			}
			if failure == nil {
				if shutdownRequested {
					return nil
				}
				return errors.New("delivery reaper stopped unexpectedly")
			}
			if shutdownRequested && errors.Is(failure, root.Err()) {
				return nil
			}
			return fmt.Errorf("delivery reaper stopped: %w", failure)
		case <-root.Done():
			shutdownFailure := shutdownServer(server)
			serverFailure := waitServer(serverErrors)
			workerFailure := waitWorker(workerErrors)
			reaperFailure := waitReaper(reaperErrors)
			if shutdownFailure != nil {
				return shutdownFailure
			}
			if serverFailure != nil {
				return serverFailure
			}
			if workerFailure != nil && !errors.Is(workerFailure, root.Err()) {
				return fmt.Errorf("delivery worker stopped: %w", workerFailure)
			}
			if reaperFailure != nil && !errors.Is(reaperFailure, root.Err()) {
				return fmt.Errorf("delivery reaper stopped: %w", reaperFailure)
			}
			return nil
		}
	}
}

func normalizeServerFailure(failure error) error {
	if failure == nil || errors.Is(failure, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve mailing console: %w", failure)
}

func shutdownServer(server serverController) error {
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failure := server.Shutdown(shutdownContext)
	if failure != nil {
		_ = server.Close()
		return fmt.Errorf("shut down mailing console server: %w", failure)
	}
	return nil
}

func waitWorker(workerErrors <-chan error) error {
	if workerErrors == nil {
		return nil
	}
	select {
	case failure := <-workerErrors:
		return failure
	case <-time.After(lifecycleWaitTimeout):
		return errors.New("delivery worker shutdown timed out")
	}
}

func waitReaper(reaperErrors <-chan error) error {
	if reaperErrors == nil {
		return nil
	}
	select {
	case failure, open := <-reaperErrors:
		if !open {
			return errReaperErrorsClosed
		}
		return failure
	case <-time.After(lifecycleWaitTimeout):
		return errors.New("delivery reaper shutdown timed out")
	}
}

func waitServer(serverErrors <-chan error) error {
	select {
	case failure := <-serverErrors:
		return normalizeServerFailure(failure)
	case <-time.After(lifecycleWaitTimeout):
		return errors.New("mailing console server shutdown timed out")
	}
}
