package pg

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/notrodans/cresora/config"
	"github.com/pressly/goose/v3"
)

func ExecuteMigrations(ctx context.Context, cfg *config.Config, logger *slog.Logger, migrationsPath string) error {
	if cfg.DbUrl == "" {
		return fmt.Errorf("execute migrations: database URL is empty")
	}
	if migrationsPath == "" {
		return fmt.Errorf("execute migrations: migrations path is empty")
	}

	db, err := sql.Open("pgx", cfg.DbUrl)
	if err != nil {
		return fmt.Errorf("execute migrations: open database: %w", err)
	}
	defer db.Close()

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return fmt.Errorf("execute migrations: set dialect: %w", err)
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		os.DirFS(migrationsPath),
	)
	if err != nil {
		return fmt.Errorf("execute migrations: create provider: %w", err)
	}
	defer provider.Close()

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("execute migrations: apply migrations: %w", err)
	}

	logger.Info("Migrations applied successfully")
	return nil
}
