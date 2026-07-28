package pg

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"

	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

func TestTelegramPeerLookupPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if failure := applyIntegrationMigrations(ctx, databaseURL); failure != nil {
		t.Fatalf("apply migrations: %v", failure)
	}
	database, failure := pgxpool.New(ctx, databaseURL)
	if failure != nil {
		t.Fatalf("open PostgreSQL pool: %v", failure)
	}
	defer database.Close()
	if failure = database.Ping(ctx); failure != nil {
		t.Fatalf("ping PostgreSQL: %v", failure)
	}

	fixture, failure := createTelegramPeerLookupFixture(ctx, database)
	if failure != nil {
		t.Fatalf("create fixture: %v", failure)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if cleanupFailure := cleanupTelegramPeerLookupFixture(cleanupContext, database, fixture); cleanupFailure != nil {
			t.Errorf("cleanup fixture: %v", cleanupFailure)
		}
	}()

	lookup := NewTelegramPeerLookup(database)
	tests := []struct {
		name       string
		request    telegram.PeerLookupRequest
		peerType   telegram.PeerType
		peerID     int64
		accessHash *int64
		wantError  error
	}{
		{
			name: "shared account A hash",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountA,
				RecipientID: fixture.sharedRecipientA,
			},
			peerType:   telegram.PeerTypeChannel,
			peerID:     fixture.sharedPeerID,
			accessHash: int64Pointer(501),
		},
		{
			name: "shared account B hash",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountB,
				RecipientID: fixture.sharedRecipientB,
			},
			peerType:   telegram.PeerTypeChannel,
			peerID:     fixture.sharedPeerID,
			accessHash: int64Pointer(502),
		},
		{
			name: "shared account without association",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountC,
				RecipientID: fixture.sharedRecipientMissingAssociation,
			},
			wantError: telegram.ErrTargetNotFound,
		},
		{
			name: "private user hash",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountA,
				RecipientID: fixture.privateUserRecipient,
			},
			peerType:   telegram.PeerTypeUser,
			peerID:     fixture.privateUserPeerID,
			accessHash: int64Pointer(601),
		},
		{
			name: "private chat nullable hash",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountA,
				RecipientID: fixture.privateChatRecipient,
			},
			peerType: telegram.PeerTypeChat,
			peerID:   fixture.privateChatPeerID,
		},
		{
			name: "private channel hash",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountA,
				RecipientID: fixture.privateChannelRecipient,
			},
			peerType:   telegram.PeerTypeChannel,
			peerID:     fixture.privateChannelPeerID,
			accessHash: int64Pointer(603),
		},
		{
			name: "private account mismatch",
			request: telegram.PeerLookupRequest{
				AccountID:   fixture.accountB,
				RecipientID: fixture.privateMismatchRecipient,
			},
			wantError: telegram.ErrTargetNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection, lookupFailure := lookup.Lookup(ctx, test.request)
			if test.wantError != nil {
				if !errors.Is(lookupFailure, test.wantError) {
					t.Fatalf("expected error %v, got %v", test.wantError, lookupFailure)
				}
				return
			}
			if lookupFailure != nil {
				t.Fatalf("lookup peer: %v", lookupFailure)
			}
			if projection.Type != test.peerType || projection.ID != test.peerID {
				t.Fatalf("expected %s/%d, got %s/%d", test.peerType, test.peerID, projection.Type, projection.ID)
			}
			if !equalInt64Pointers(projection.AccessHash, test.accessHash) {
				t.Fatalf("expected access hash %v, got %v", test.accessHash, projection.AccessHash)
			}
		})
	}

	duplicateContext, duplicateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer duplicateCancel()
	transaction, failure := database.Begin(duplicateContext)
	if failure != nil {
		t.Fatalf("begin duplicate recipient transaction: %v", failure)
	}
	_, failure = transaction.Exec(
		duplicateContext,
		`INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, 1)`,
		fixture.duplicateMailingID,
		fixture.sharedRecipientA,
	)
	if failure == nil {
		_ = transaction.Rollback(duplicateContext)
		t.Fatal("expected duplicate recipient UUID to be rejected")
	}
	_ = transaction.Rollback(duplicateContext)
	var postgresFailure *pgconn.PgError
	if !errors.As(failure, &postgresFailure) || postgresFailure.Code != "23505" || postgresFailure.ConstraintName != "uq_mailing_recipients_id" {
		t.Fatalf("expected global recipient UUID unique violation, got %v", failure)
	}
}

type telegramPeerLookupFixture struct {
	operatorID                        uuid.UUID
	accountA                          uuid.UUID
	accountB                          uuid.UUID
	accountC                          uuid.UUID
	sharedRecipientA                  uuid.UUID
	sharedRecipientB                  uuid.UUID
	sharedRecipientMissingAssociation uuid.UUID
	privateUserRecipient              uuid.UUID
	privateChatRecipient              uuid.UUID
	privateChannelRecipient           uuid.UUID
	privateMismatchRecipient          uuid.UUID
	duplicateMailingID                uuid.UUID
	sharedPeerID                      int64
	privateUserPeerID                 int64
	privateChatPeerID                 int64
	privateChannelPeerID              int64
}

func createTelegramPeerLookupFixture(
	context context.Context,
	database *pgxpool.Pool,
) (telegramPeerLookupFixture, error) {
	fixture := telegramPeerLookupFixture{
		operatorID:                        uuid.New(),
		accountA:                          uuid.New(),
		accountB:                          uuid.New(),
		accountC:                          uuid.New(),
		sharedRecipientA:                  uuid.New(),
		sharedRecipientB:                  uuid.New(),
		sharedRecipientMissingAssociation: uuid.New(),
		privateUserRecipient:              uuid.New(),
		privateChatRecipient:              uuid.New(),
		privateChannelRecipient:           uuid.New(),
		privateMismatchRecipient:          uuid.New(),
		duplicateMailingID:                uuid.New(),
	}
	fixture.sharedPeerID = peerID(fixture.sharedRecipientA)
	fixture.privateUserPeerID = peerID(fixture.privateUserRecipient)
	fixture.privateChatPeerID = peerID(fixture.privateChatRecipient)
	fixture.privateChannelPeerID = peerID(fixture.privateChannelRecipient)

	transaction, failure := database.Begin(context)
	if failure != nil {
		return fixture, failure
	}
	defer transaction.Rollback(context)

	exec := func(query string, arguments ...any) error {
		if _, failure := transaction.Exec(context, query, arguments...); failure != nil {
			return failure
		}
		return nil
	}
	if failure = exec(
		`INSERT INTO operators (id, username, password) VALUES ($1, $2, 'fixture-password')`,
		fixture.operatorID,
		"fixture-"+fixture.operatorID.String()[:8],
	); failure != nil {
		return fixture, fmt.Errorf("insert operator: %w", failure)
	}
	if failure = exec(
		`INSERT INTO operator_accounts (id, operator_id, phone, telegram_username, telegram_first_name, api_id)
		 VALUES ($1, $2, '+12025550101', $3, 'Fixture A', 1),
		        ($4, $2, '+12025550102', $5, 'Fixture B', 2),
		        ($6, $2, '+12025550103', $7, 'Fixture C', 3)`,
		fixture.accountA,
		fixture.operatorID,
		"account-a-"+fixture.accountA.String()[:8],
		fixture.accountB,
		"account-b-"+fixture.accountB.String()[:8],
		fixture.accountC,
		"account-c-"+fixture.accountC.String()[:8],
	); failure != nil {
		return fixture, fmt.Errorf("insert operator accounts: %w", failure)
	}
	if failure = exec(
		`INSERT INTO telegram_shared_dialogs (id, telegram_peer_id, dialog_kind, title, metadata_synced_at)
		 VALUES ($1, $2, 'supergroup', 'Fixture shared dialog', CURRENT_TIMESTAMP)`,
		uuid.New(),
		fixture.sharedPeerID,
	); failure != nil {
		return fixture, fmt.Errorf("insert shared dialog: %w", failure)
	}
	var sharedDialogID uuid.UUID
	if failure = transaction.QueryRow(
		context,
		`SELECT id FROM telegram_shared_dialogs WHERE telegram_peer_id = $1`,
		fixture.sharedPeerID,
	).Scan(&sharedDialogID); failure != nil {
		return fixture, fmt.Errorf("load shared dialog fixture: %w", failure)
	}
	if failure = exec(
		`INSERT INTO operator_accounts_shared_dialogs (account_id, shared_dialog_id, access_hash)
		 VALUES ($1, $3, $2), ($4, $3, $5)`,
		fixture.accountA,
		int64(501),
		sharedDialogID,
		fixture.accountB,
		int64(502),
	); failure != nil {
		return fixture, fmt.Errorf("insert shared account projections: %w", failure)
	}
	privateTargets := []struct {
		accountID  uuid.UUID
		peerType   string
		peerID     int64
		accessHash *int64
	}{
		{fixture.accountA, "user", fixture.privateUserPeerID, int64Pointer(601)},
		{fixture.accountA, "chat", fixture.privateChatPeerID, nil},
		{fixture.accountA, "channel", fixture.privateChannelPeerID, int64Pointer(603)},
	}
	for _, target := range privateTargets {
		if failure = exec(
			`INSERT INTO operator_accounts_private_dialogs (account_id, peer_type, telegram_peer_id, title, access_hash)
			 VALUES ($1, $2::telegram_peer_type, $3, $4, $5)`,
			target.accountID,
			target.peerType,
			target.peerID,
			"Fixture "+target.peerType,
			target.accessHash,
		); failure != nil {
			return fixture, fmt.Errorf("insert private %s projection: %w", target.peerType, failure)
		}
	}

	insertMailing := func(mailingID, accountID, recipientID uuid.UUID) error {
		if failure := exec(
			`INSERT INTO mailings (id, operator_id, name, message_text) VALUES ($1, $2, $3, 'Fixture message')`,
			mailingID,
			fixture.operatorID,
			"fixture-mailing-"+mailingID.String()[:8],
		); failure != nil {
			return fmt.Errorf("insert mailing: %w", failure)
		}
		if failure := exec(
			`INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $2)`,
			mailingID,
			accountID,
		); failure != nil {
			return fmt.Errorf("insert mailing route: %w", failure)
		}
		if failure := exec(
			`INSERT INTO mailing_recipients (mailing_id, id, position) VALUES ($1, $2, 0)`,
			mailingID,
			recipientID,
		); failure != nil {
			return fmt.Errorf("insert mailing recipient: %w", failure)
		}
		return nil
	}
	insertSharedRecipient := func(mailingID, accountID, recipientID uuid.UUID) error {
		if failure := insertMailing(mailingID, accountID, recipientID); failure != nil {
			return failure
		}
		return exec(
			`INSERT INTO telegram_mailing_recipients (mailing_id, recipient_id, shared_dialog_id) VALUES ($1, $2, $3)`,
			mailingID,
			recipientID,
			sharedDialogID,
		)
	}
	insertPrivateRecipient := func(mailingID, accountID, recipientID, privateAccountID uuid.UUID, peerType string, privatePeerID int64) error {
		if failure := insertMailing(mailingID, accountID, recipientID); failure != nil {
			return failure
		}
		return exec(
			`INSERT INTO telegram_mailing_recipients (mailing_id, recipient_id, private_account_id, private_peer_type, private_peer_id)
			 VALUES ($1, $2, $3, $4::telegram_peer_type, $5)`,
			mailingID,
			recipientID,
			privateAccountID,
			peerType,
			privatePeerID,
		)
	}
	fixtures := []func() error{
		func() error { return insertSharedRecipient(uuid.New(), fixture.accountA, fixture.sharedRecipientA) },
		func() error { return insertSharedRecipient(uuid.New(), fixture.accountB, fixture.sharedRecipientB) },
		func() error {
			return insertSharedRecipient(uuid.New(), fixture.accountC, fixture.sharedRecipientMissingAssociation)
		},
		func() error {
			return insertPrivateRecipient(uuid.New(), fixture.accountA, fixture.privateUserRecipient, fixture.accountA, "user", fixture.privateUserPeerID)
		},
		func() error {
			return insertPrivateRecipient(uuid.New(), fixture.accountA, fixture.privateChatRecipient, fixture.accountA, "chat", fixture.privateChatPeerID)
		},
		func() error {
			return insertPrivateRecipient(uuid.New(), fixture.accountA, fixture.privateChannelRecipient, fixture.accountA, "channel", fixture.privateChannelPeerID)
		},
		func() error {
			return insertPrivateRecipient(uuid.New(), fixture.accountB, fixture.privateMismatchRecipient, fixture.accountA, "user", fixture.privateUserPeerID)
		},
		func() error { return insertMailing(fixture.duplicateMailingID, fixture.accountA, uuid.New()) },
	}
	for _, create := range fixtures {
		if failure = create(); failure != nil {
			return fixture, failure
		}
	}
	if failure = transaction.Commit(context); failure != nil {
		return fixture, fmt.Errorf("commit fixture: %w", failure)
	}
	return fixture, nil
}

func cleanupTelegramPeerLookupFixture(
	context context.Context,
	database *pgxpool.Pool,
	fixture telegramPeerLookupFixture,
) error {
	transaction, failure := database.Begin(context)
	if failure != nil {
		return failure
	}
	defer transaction.Rollback(context)
	if _, failure = transaction.Exec(
		context,
		`DELETE FROM telegram_mailing_routes
		 WHERE mailing_id IN (
			SELECT id FROM mailings WHERE operator_id = $1
		 )`,
		fixture.operatorID,
	); failure != nil {
		return fmt.Errorf("delete fixture mailing routes: %w", failure)
	}
	if _, failure = transaction.Exec(context, `DELETE FROM mailings WHERE operator_id = $1`, fixture.operatorID); failure != nil {
		return fmt.Errorf("delete fixture mailings: %w", failure)
	}
	if _, failure = transaction.Exec(context, `DELETE FROM operators WHERE id = $1`, fixture.operatorID); failure != nil {
		return fmt.Errorf("delete fixture operator: %w", failure)
	}
	if _, failure = transaction.Exec(
		context,
		`DELETE FROM telegram_shared_dialogs WHERE telegram_peer_id = $1`,
		fixture.sharedPeerID,
	); failure != nil {
		return fmt.Errorf("delete fixture shared dialog: %w", failure)
	}
	return transaction.Commit(context)
}

func applyIntegrationMigrations(context context.Context, databaseURL string) error {
	database, failure := sql.Open("pgx", databaseURL)
	if failure != nil {
		return failure
	}
	defer database.Close()
	if failure = database.PingContext(context); failure != nil {
		return failure
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("locate integration test source")
	}
	migrationsPath := filepath.Join(filepath.Dir(filename), "../../../../migrations")
	provider, failure := goose.NewProvider(
		goose.DialectPostgres,
		database,
		os.DirFS(migrationsPath),
		goose.WithAllowOutofOrder(true),
	)
	if failure != nil {
		return failure
	}
	_, failure = provider.Up(context)
	return failure
}

func peerID(value uuid.UUID) int64 {
	result := int64(binary.BigEndian.Uint64(value[:8]) & math.MaxInt64)
	if result == 0 {
		return 1
	}
	return result
}
