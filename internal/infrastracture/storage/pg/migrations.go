package pg

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/notrodans/cresora/config"
	"github.com/pressly/goose/v3"
)

func ExecuteMigrations(ctx context.Context, cfg *config.Config, logger *slog.Logger, migrationsPath string) {
	db, err := sql.Open("pgx", cfg.DbUrl)
	if err != nil {
		logger.Error(err.Error())
	}
	defer db.Close()

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		panic(err)
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		os.DirFS(migrationsPath),
		goose.WithAllowOutofOrder(true),
	)
	if err != nil {
		panic(err)
	}

	if _, err := provider.Up(ctx); err != nil {
		logger.Error(err.Error())
	}

	logger.Info("Migrations applied successfully")
}
