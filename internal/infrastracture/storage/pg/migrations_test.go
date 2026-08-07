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

func TestMigrationsContainCurrentBaselineAndRepair(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration test")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	entries, failure := os.ReadDir(migrationsPath)
	if failure != nil {
		t.Fatalf("read migrations: %v", failure)
	}
	expected := []struct {
		name     string
		contents []string
	}{
		{
			name: "20260801000000_current_schema.sql",
			contents: []string{
				"status operator_account_status_type NOT NULL",
				"status_version bigint NOT NULL DEFAULT 1",
				"telegram_user_id bigint",
				"auth_expires_at timestamptz",
				"failure_code varchar(32)",
				"ck_operator_accounts_timestamp_order",
				"ix_operator_accounts_operator_status",
				"CREATE UNIQUE INDEX uq_operator_accounts_operator_phone",
				"ON operator_accounts (operator_id, phone)",
				"WHERE phone IS NOT NULL",
				"CREATE TABLE sessions",
				"ciphertext bytea NOT NULL",
				"CREATE TYPE mailing_delivery_status_type",
				"'unknown'",
				"execution_generation bigint NOT NULL DEFAULT 1",
				"lease_execution_generation bigint",
			},
		},
		{
			name: "20260801200000_repair_operator_account_phone_index.sql",
			contents: []string{
				"-- +goose Up",
				"CREATE UNIQUE INDEX IF NOT EXISTS uq_operator_accounts_operator_phone",
				"ON operator_accounts (operator_id, phone)",
				"WHERE phone IS NOT NULL",
				"-- +goose Down",
				"The current schema migration owns this index, so rollback is intentionally a no-op.",
			},
		},
		{
			name: "20260802000000_add_operator_account_remote_logout_required.sql",
			contents: []string{
				"-- +goose Up",
				"ADD COLUMN remote_logout_required boolean NOT NULL DEFAULT FALSE",
				"ADD CONSTRAINT ck_operator_accounts_remote_logout_required CHECK",
				"remote_logout_required = FALSE OR status = 'disconnecting'",
				"-- +goose Down",
				"DROP CONSTRAINT IF EXISTS ck_operator_accounts_remote_logout_required",
				"DROP COLUMN IF EXISTS remote_logout_required",
			},
		},
		{
			name: "20260803000000_add_account_dialog_syncs.sql",
			contents: []string{
				"-- +goose Up",
				"CREATE TYPE account_dialog_sync_status_type AS ENUM",
				"CREATE TABLE account_dialog_syncs",
				"lease_token uuid",
				"lease_generation bigint",
				"-- +goose Down",
				"DROP TABLE account_dialog_syncs",
			},
		},
	}
	if len(entries) != len(expected) {
		t.Fatalf("migration file count = %d, want %d", len(entries), len(expected))
	}
	for index, migration := range expected {
		if entries[index].Name() != migration.name {
			t.Fatalf("migration file %d = %q, want %q", index, entries[index].Name(), migration.name)
		}
		contents, readFailure := os.ReadFile(filepath.Join(migrationsPath, migration.name))
		if readFailure != nil {
			t.Fatalf("read migration %q: %v", migration.name, readFailure)
		}
		text := string(contents)
		for _, required := range migration.contents {
			if !strings.Contains(text, required) {
				t.Fatalf("migration %q is missing %q", migration.name, required)
			}
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
