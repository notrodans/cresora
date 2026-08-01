package pg

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/notrodans/cresora/config"
)

func TestMigrationsContainOneCurrentBaseline(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration test")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	entries, failure := os.ReadDir(migrationsPath)
	if failure != nil {
		t.Fatalf("read migrations: %v", failure)
	}
	if len(entries) != 1 {
		t.Fatalf("migration file count = %d, want 1", len(entries))
	}
	if entries[0].Name() != "20260801000000_current_schema.sql" {
		t.Fatalf("migration file = %q, want current baseline", entries[0].Name())
	}

	contents, failure := os.ReadFile(filepath.Join(migrationsPath, entries[0].Name()))
	if failure != nil {
		t.Fatalf("read current migration: %v", failure)
	}
	text := string(contents)
	for _, required := range []string{
		"status operator_account_status_type NOT NULL",
		"status_version bigint NOT NULL DEFAULT 1",
		"telegram_user_id bigint",
		"auth_expires_at timestamptz",
		"failure_code varchar(32)",
		"ck_operator_accounts_timestamp_order",
		"ix_operator_accounts_operator_status",
		"CREATE TABLE sessions",
		"ciphertext bytea NOT NULL",
		"CREATE TYPE mailing_delivery_status_type",
		"'unknown'",
		"execution_generation bigint NOT NULL DEFAULT 1",
		"lease_execution_generation bigint",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("current migration is missing %q", required)
		}
	}
}

func TestExecuteMigrationsRejectsInvalidConfiguration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name string
		cfg  *config.Config
		path string
		want string
	}{
		{name: "empty database URL", cfg: &config.Config{}, path: t.TempDir(), want: "database URL is empty"},
		{name: "empty migrations path", cfg: &config.Config{DbUrl: "postgres://localhost/db"}, want: "migrations path is empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := ExecuteMigrations(context.Background(), test.cfg, logger, test.path)
			if failure == nil || !strings.Contains(failure.Error(), test.want) {
				t.Fatalf("execute migrations error = %v, want %q", failure, test.want)
			}
		})
	}
}

func TestExecuteMigrationsReturnsProviderFailure(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration test")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	context, cancel := context.WithCancel(context.Background())
	cancel()

	failure := ExecuteMigrations(
		context,
		&config.Config{DbUrl: "postgres://localhost:1/cresora"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		migrationsPath,
	)
	if failure == nil {
		t.Fatal("execute migrations returned nil after canceled context")
	}
	if errors.Is(failure, context.Err()) == false {
		t.Fatalf("execute migrations error = %v, want canceled context", failure)
	}
}
