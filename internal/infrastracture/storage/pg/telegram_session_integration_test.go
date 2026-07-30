package pg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

func TestTelegramSessionStorePostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := applyIntegrationMigrations(ctx, databaseURL); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	database, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	fixture, err := createTelegramSessionFixture(ctx, database)
	if err != nil {
		t.Fatalf("create session fixture: %v", err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := cleanupTelegramSessionFixture(cleanupContext, database, fixture); err != nil {
			t.Errorf("cleanup session fixture: %v", err)
		}
	}()

	const keyID = "current"
	key := []byte("01234567890123456789012345678901")
	store, err := NewTelegramSessionStore(database, keyID, key)
	if err != nil {
		t.Fatalf("create Telegram session store: %v", err)
	}
	ownedScope := telegram.SessionScope{OperatorID: fixture.operatorA, AccountID: fixture.accountA}
	foreignScope := telegram.SessionScope{OperatorID: fixture.operatorA, AccountID: fixture.accountB}

	loaded, err := store.Load(ctx, ownedScope)
	if err != nil {
		t.Fatalf("load missing session: %v", err)
	}
	if loaded.Present {
		t.Fatal("missing session was reported as present")
	}

	firstPlaintext := []byte("opaque session value that must not be stored in plaintext")
	if err := store.Store(ctx, ownedScope, firstPlaintext); err != nil {
		t.Fatalf("store first session: %v", err)
	}
	var firstNonce, firstCiphertext []byte
	if err := database.QueryRow(ctx, `SELECT nonce, ciphertext FROM sessions WHERE account_id = $1`, fixture.accountA).Scan(&firstNonce, &firstCiphertext); err != nil {
		t.Fatalf("read first session envelope: %v", err)
	}
	if bytes.Contains(firstCiphertext, firstPlaintext) {
		t.Fatal("session ciphertext contains plaintext session bytes")
	}

	loaded, err = store.Load(ctx, ownedScope)
	if err != nil {
		t.Fatalf("load stored session: %v", err)
	}
	if !loaded.Present || !bytes.Equal(loaded.Bytes, firstPlaintext) {
		t.Fatalf("loaded session = %#v, want present plaintext %q", loaded, firstPlaintext)
	}

	secondPlaintext := []byte("replacement opaque session value")
	if err := store.Store(ctx, ownedScope, secondPlaintext); err != nil {
		t.Fatalf("upsert second session: %v", err)
	}
	var secondNonce []byte
	if err := database.QueryRow(ctx, `SELECT nonce FROM sessions WHERE account_id = $1`, fixture.accountA).Scan(&secondNonce); err != nil {
		t.Fatalf("read second session nonce: %v", err)
	}
	if bytes.Equal(firstNonce, secondNonce) {
		t.Fatal("upsert reused the AES-GCM nonce")
	}
	loaded, err = store.Load(ctx, ownedScope)
	if err != nil {
		t.Fatalf("load replaced session: %v", err)
	}
	if !loaded.Present || !bytes.Equal(loaded.Bytes, secondPlaintext) {
		t.Fatalf("replaced loaded session = %#v, want %q", loaded, secondPlaintext)
	}

	if err := store.Store(ctx, foreignScope, []byte("foreign session")); !errors.Is(err, telegram.ErrSessionUnauthorized) {
		t.Fatalf("store foreign session: error = %v, want %v", err, telegram.ErrSessionUnauthorized)
	}
	foreignOwnerScope := telegram.SessionScope{OperatorID: fixture.operatorB, AccountID: fixture.accountB}
	if err := store.Store(ctx, foreignOwnerScope, []byte("tenant B session")); err != nil {
		t.Fatalf("store foreign owner session: %v", err)
	}
	if _, err := store.Load(ctx, foreignScope); !errors.Is(err, telegram.ErrSessionUnauthorized) {
		t.Fatalf("load cross-tenant session: error = %v, want %v", err, telegram.ErrSessionUnauthorized)
	}
	var foreignCount int
	if err := database.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE account_id = $1`, fixture.accountB).Scan(&foreignCount); err != nil {
		t.Fatalf("count foreign sessions: %v", err)
	}
	if foreignCount != 1 {
		t.Fatalf("foreign account session count = %d, want 1 owner-created session", foreignCount)
	}

	unknownScope := telegram.SessionScope{OperatorID: fixture.operatorA, AccountID: uuid.New()}
	for name, operation := range map[string]func() error{
		"load unknown account": func() error {
			_, err := store.Load(ctx, unknownScope)
			return err
		},
		"store unknown account": func() error {
			return store.Store(ctx, unknownScope, []byte("unknown session"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, telegram.ErrSessionUnauthorized) {
				t.Fatalf("unknown account operation: error = %v, want %v", err, telegram.ErrSessionUnauthorized)
			}
		})
	}

	if _, err := database.Exec(ctx, `UPDATE sessions SET ciphertext = set_byte(ciphertext, 0, get_byte(ciphertext, 0) # 255) WHERE account_id = $1`, fixture.accountA); err != nil {
		t.Fatalf("corrupt session envelope: %v", err)
	}
	_, err = store.Load(ctx, ownedScope)
	if !errors.Is(err, telegram.ErrSessionCorrupt) {
		t.Fatalf("load corrupted session: error = %v, want %v", err, telegram.ErrSessionCorrupt)
	}
	if strings.Contains(err.Error(), string(secondPlaintext)) {
		t.Fatalf("corruption error contains plaintext session: %v", err)
	}
}

func TestTelegramSessionMigrationRejectsLegacyPlaintextRows(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	schema := "telegram_session_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := database.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create migration test schema: %v", err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := database.Exec(cleanupContext, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("drop migration test schema: %v", err)
		}
	}()

	transaction, err := database.Begin(ctx)
	if err != nil {
		t.Fatalf("begin migration test transaction: %v", err)
	}
	defer transaction.Rollback(ctx)
	if _, err := transaction.Exec(ctx, `SET LOCAL search_path TO `+schema); err != nil {
		t.Fatalf("set migration test search path: %v", err)
	}
	if _, err := transaction.Exec(ctx, `CREATE TABLE sessions (
		account_id uuid PRIMARY KEY,
		session varchar(255) NOT NULL,
		created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create legacy sessions table: %v", err)
	}
	if _, err := transaction.Exec(ctx, `INSERT INTO sessions (account_id, session) VALUES ($1, $2)`, uuid.New(), "legacy plaintext session"); err != nil {
		t.Fatalf("insert legacy session: %v", err)
	}

	migration, err := readTelegramSessionMigrationUp()
	if err != nil {
		t.Fatalf("read session migration: %v", err)
	}
	_, err = transaction.Exec(ctx, migration)
	if err == nil {
		t.Fatal("legacy plaintext migration succeeded")
	}
	if !strings.Contains(err.Error(), "legacy plaintext session rows exist") {
		t.Fatalf("legacy migration error = %v, want explicit plaintext failure", err)
	}
}

type telegramSessionFixture struct {
	operatorA uuid.UUID
	operatorB uuid.UUID
	accountA  uuid.UUID
	accountB  uuid.UUID
}

func createTelegramSessionFixture(context context.Context, database *pgxpool.Pool) (telegramSessionFixture, error) {
	fixture := telegramSessionFixture{
		operatorA: uuid.New(),
		operatorB: uuid.New(),
		accountA:  uuid.New(),
		accountB:  uuid.New(),
	}
	transaction, err := database.Begin(context)
	if err != nil {
		return fixture, err
	}
	defer transaction.Rollback(context)
	if _, err := transaction.Exec(context, `INSERT INTO operators (id, username) VALUES ($1, $2), ($3, $4)`, fixture.operatorA, "session-fixture-"+fixture.operatorA.String()[:8], fixture.operatorB, "session-fixture-"+fixture.operatorB.String()[:8]); err != nil {
		return fixture, fmt.Errorf("insert session fixture operators: %w", err)
	}
	if _, err := transaction.Exec(context, `INSERT INTO operator_accounts (id, operator_id, phone, telegram_username, telegram_first_name, api_id) VALUES ($1, $2, '+12025550201', $3, 'Session A', 1), ($4, $5, '+12025550202', $6, 'Session B', 2)`, fixture.accountA, fixture.operatorA, "session-a-"+fixture.accountA.String()[:8], fixture.accountB, fixture.operatorB, "session-b-"+fixture.accountB.String()[:8]); err != nil {
		return fixture, fmt.Errorf("insert session fixture accounts: %w", err)
	}
	if err := transaction.Commit(context); err != nil {
		return fixture, fmt.Errorf("commit session fixture: %w", err)
	}
	return fixture, nil
}

func cleanupTelegramSessionFixture(context context.Context, database *pgxpool.Pool, fixture telegramSessionFixture) error {
	if _, err := database.Exec(context, `DELETE FROM operators WHERE id IN ($1, $2)`, fixture.operatorA, fixture.operatorB); err != nil {
		return fmt.Errorf("delete session fixture operators: %w", err)
	}
	return nil
}

func readTelegramSessionMigrationUp() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("locate session integration test source")
	}
	path := filepath.Join(filepath.Dir(filename), "../../../../migrations/20260728000000_encrypt_telegram_sessions.sql")
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	up := strings.TrimPrefix(string(contents), "-- +goose Up")
	if down := strings.Index(up, "-- +goose Down"); down >= 0 {
		up = up[:down]
	}
	return strings.TrimSpace(up), nil
}
