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

	"github.com/notrodans/nebula-go/config"
	mailingconsolecommands "github.com/notrodans/nebula-go/internal/application/commands/mailing-console"
	operatoraccountauthmock "github.com/notrodans/nebula-go/internal/application/operatoraccountauth/mock"
	mailingconsolerequests "github.com/notrodans/nebula-go/internal/application/requests/mailing-console"
	mailingconsole "github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
	background "github.com/notrodans/nebula-go/internal/entrypoint/background/delivery"
	"github.com/notrodans/nebula-go/internal/entrypoint/background/delivery/actor"
	"github.com/notrodans/nebula-go/internal/entrypoint/http/console"
	"github.com/notrodans/nebula-go/internal/entrypoint/http/operatoraccounts"
	"github.com/notrodans/nebula-go/internal/infrastracture/logger/slog"
	"github.com/notrodans/nebula-go/internal/infrastracture/storage/pg"
	claims "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/claims"
	deliveries "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/deliveries"
	mailings "github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/mailings"
	telegramaccount "github.com/notrodans/nebula-go/internal/transport/telegram/account"
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
	if rootContext == nil {
		panic("run application without context")
	}
	if cancel == nil {
		panic("run application without cancel function")
	}

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

	// Создаём сервис для работы с таблицами рассылок.
	service := mailingconsole.NewService(cfg.OperatorID, mailings.NewMailingConsole(database), mailings.NewMailings(database))
	if failure = service.VerifyOperator(rootContext); failure != nil {
		return fmt.Errorf("verify configured operator: %w", failure)
	}
	// Команда для создания драфта рассылки
	createDraft := mailingconsolecommands.NewCreateDraft(&service)
	// Команда для помещения рассылки в очередь
	queueMailing := mailingconsolecommands.NewQueue(&service)
	// Запрос борды
	dashboard := mailingconsolerequests.NewDashboard(&service)
	accountAuthentication := operatoraccountauthmock.New()

	loggerMiddleware := func(next http.Handler) http.Handler {
		return logging(log, next)
	}

	router := chi.NewRouter()
	router.Use(loggerMiddleware)
	router.Use(middleware.Recoverer)
	operatoraccounts.Register(
		router,
		accountAuthentication.StartPhone,
		accountAuthentication.VerifyPhone,
		accountAuthentication.StartQR,
		accountAuthentication.RefreshQR,
		accountAuthentication.Status,
	)
	console.Register(router, createDraft, queueMailing, dashboard, cfg.PublicOrigin.String(), log)

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

	return monitorRuntime(rootContext, cancel, server, server.ListenAndServe, workerErrors)
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
	pump := background.New(claims, supervisor, 4, 250*time.Millisecond)

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

var lifecycleWaitTimeout = 10 * time.Second

func monitorRuntime(
	root context.Context,
	cancel context.CancelFunc,
	server serverController,
	serve serveFunction,
	workerErrors <-chan error,
) error {
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
			if failure != nil {
				return failure
			}
			if shutdownFailure != nil {
				return shutdownFailure
			}
			if workerFailure != nil {
				return fmt.Errorf("delivery worker stopped: %w", workerFailure)
			}
			return errors.New("mailing console server stopped unexpectedly")
		case failure := <-workerErrors:
			shutdownRequested := root.Err() != nil
			cancel()
			shutdownFailure := shutdownServer(server)
			serverFailure := waitServer(serverErrors)
			if shutdownRequested && (failure == nil || errors.Is(failure, root.Err())) {
				if shutdownFailure != nil {
					return shutdownFailure
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
		case <-root.Done():
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

func waitServer(serverErrors <-chan error) error {
	select {
	case failure := <-serverErrors:
		return normalizeServerFailure(failure)
	case <-time.After(lifecycleWaitTimeout):
		return errors.New("mailing console server shutdown timed out")
	}
}
