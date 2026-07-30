package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/services/mailingconsole"
	"github.com/notrodans/cresora/internal/domain/mailing"
)

func TestMailingConsolePostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, _, failure := newIsolatedMailingConsoleDatabase(ctx, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		t.Fatalf("ping PostgreSQL: %v", failure)
	}

	fixture, failure := createMailingConsoleFixture(ctx, database)
	if failure != nil {
		t.Fatalf("create fixture: %v", failure)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if cleanupFailure := cleanupMailingConsoleFixture(cleanupContext, database, fixture); cleanupFailure != nil {
			t.Errorf("cleanup fixture: %v", cleanupFailure)
		}
	}()

	projection := NewMailingConsole(database)
	mailings := NewMailings(database)
	service := mailingconsole.NewService(projection, mailings)
	actorA := application.Actor{OperatorID: fixture.operatorA}
	actorB := application.Actor{OperatorID: fixture.operatorB}
	operatorAMailings := mailings.OwnedBy(fixture.operatorA)
	operatorBMailings := mailings.OwnedBy(fixture.operatorB)
	operatorBDraft, failure := service.CreateDraft(ctx, actorB, mailingconsole.CreateDraftInput{
		Name:            "operator B draft",
		MessageText:     "operator B message",
		AccountID:       fixture.accountB,
		SharedDialogIDs: []uuid.UUID{fixture.sharedOperatorB},
	})
	if failure != nil {
		t.Fatalf("create operator B draft: %v", failure)
	}
	if operatorBDraft.UUID() == uuid.Nil {
		t.Fatal("expected operator B draft identity")
	}
	emptyDraftID, failure := createMailingConsoleEmptyDraft(ctx, database, fixture)
	if failure != nil {
		t.Fatalf("create empty draft: %v", failure)
	}

	t.Run("cross operator queue is rejected without changes", func(t *testing.T) {
		statusBefore, runsBefore, deliveriesBefore := mailingConsoleQueueState(t, ctx, database, operatorBDraft.UUID())
		foreignFailure := service.Queue(ctx, actorA, operatorBDraft.UUID())
		assertMailingConsoleError(t, foreignFailure, mailing.ErrNotFound)
		randomFailure := service.Queue(ctx, actorA, uuid.New())
		assertMailingConsoleError(t, randomFailure, mailing.ErrNotFound)
		statusAfter, runsAfter, deliveriesAfter := mailingConsoleQueueState(t, ctx, database, operatorBDraft.UUID())
		if statusBefore != statusAfter || runsBefore != runsAfter || deliveriesBefore != deliveriesAfter {
			t.Fatalf("cross-operator queue changed state: %q/%d/%d -> %q/%d/%d", statusBefore, runsBefore, deliveriesBefore, statusAfter, runsAfter, deliveriesAfter)
		}
	})
	t.Run("queue without eligible recipients is rejected atomically", func(t *testing.T) {
		statusBefore, runsBefore, deliveriesBefore := mailingConsoleQueueState(t, ctx, database, emptyDraftID)
		failure := operatorAMailings.Mailing(mailing.Identity(emptyDraftID)).Queue(ctx)
		assertMailingConsoleError(t, failure, mailing.ErrNoEligibleRecipients)
		statusAfter, runsAfter, deliveriesAfter := mailingConsoleQueueState(t, ctx, database, emptyDraftID)
		if statusBefore != "draft" || statusAfter != statusBefore || runsBefore != 0 || runsAfter != runsBefore || deliveriesBefore != 0 || deliveriesAfter != deliveriesBefore {
			t.Fatalf("no-recipient queue changed state: %q/%d/%d -> %q/%d/%d", statusBefore, runsBefore, deliveriesBefore, statusAfter, runsAfter, deliveriesAfter)
		}
	})
	t.Run("owner can queue its real draft", func(t *testing.T) {
		failure := operatorBMailings.Mailing(operatorBDraft).Queue(ctx)
		if failure != nil {
			t.Fatalf("queue operator B draft: %v", failure)
		}
		assertMailingConsoleQueuedGraph(t, ctx, database, operatorBDraft.UUID(), 1)
	})
	t.Run("dashboard is operator scoped and excludes unusable targets", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			actor   application.Actor
			own     uuid.UUID
			foreign uuid.UUID
		}{
			{name: "operator A", actor: actorA, own: fixture.accountA, foreign: fixture.accountB},
			{name: "operator B", actor: actorB, own: fixture.accountB, foreign: fixture.accountA},
		} {
			dashboard, dashboardFailure := service.Dashboard(ctx, test.actor)
			if dashboardFailure != nil {
				t.Fatalf("load %s application dashboard: %v", test.name, dashboardFailure)
			}
			for _, account := range dashboard.Accounts {
				if account.ID == test.foreign {
					t.Fatalf("%s application dashboard leaked foreign account", test.name)
				}
			}
			found := false
			for _, account := range dashboard.Accounts {
				found = found || account.ID == test.own
			}
			if !found {
				t.Fatalf("%s application dashboard omitted owned account", test.name)
			}
		}
		accounts, sharedDialogs, privateDialogs, dashboardMailings, dashboardFailure := projection.Dashboard(ctx, fixture.operatorA)
		if dashboardFailure != nil {
			t.Fatalf("load operator A dashboard: %v", dashboardFailure)
		}
		accountIDs := make(map[uuid.UUID]bool, len(accounts))
		for _, account := range accounts {
			accountIDs[account.ID] = true
		}
		if !accountIDs[fixture.accountA] || !accountIDs[fixture.accountA2] || accountIDs[fixture.accountB] {
			t.Fatalf("unexpected operator A accounts: %#v", accounts)
		}
		sharedIDs := make(map[uuid.UUID]bool, len(sharedDialogs))
		for _, dialog := range sharedDialogs {
			sharedIDs[dialog.ID] = true
			if dialog.AccountID != fixture.accountA && dialog.AccountID != fixture.accountA2 {
				t.Fatalf("dashboard leaked shared dialog account: %#v", dialog)
			}
		}
		if !sharedIDs[fixture.sharedUsable] || !sharedIDs[fixture.sharedWrongAccount] || sharedIDs[fixture.sharedLeft] || sharedIDs[fixture.sharedNoSend] || sharedIDs[fixture.sharedNoHash] || sharedIDs[fixture.sharedZeroHash] || sharedIDs[fixture.sharedOperatorB] {
			t.Fatalf("unexpected operator A shared dialogs: %#v", sharedDialogs)
		}
		privateKeys := make(map[string]bool, len(privateDialogs))
		for _, dialog := range privateDialogs {
			privateKeys[consolePrivateKey(dialog.PeerType, dialog.PeerID)] = true
			if dialog.AccountID != fixture.accountA {
				t.Fatalf("dashboard leaked private dialog account: %#v", dialog)
			}
		}
		if !privateKeys[consolePrivateKey(mailingconsole.PeerTypeUser, fixture.privateUserPeer)] || !privateKeys[consolePrivateKey(mailingconsole.PeerTypeChat, fixture.privateChatPeer)] || privateKeys[consolePrivateKey(mailingconsole.PeerTypeUser, fixture.privateOperatorBPeer)] {
			t.Fatalf("unexpected operator A private dialogs: %#v", privateDialogs)
		}
		for _, summary := range dashboardMailings {
			if summary.AccountID == fixture.accountB {
				t.Fatalf("dashboard leaked operator B mailing: %#v", summary)
			}
		}
	})

	createFailure := func(input mailingconsole.CreateDraftInput) error {
		_, failure := service.CreateDraft(ctx, actorA, input)
		return failure
	}
	t.Run("wrong account is rejected", func(t *testing.T) {
		failure := createFailure(mailingconsole.CreateDraftInput{
			Name: "wrong account", MessageText: "message", AccountID: fixture.accountB, SharedDialogIDs: []uuid.UUID{fixture.sharedOperatorB},
		})
		assertMailingConsoleError(t, failure, mailingconsole.ErrNotFound)
	})
	t.Run("wrong selected-account shared dialog is rejected", func(t *testing.T) {
		failure := createFailure(mailingconsole.CreateDraftInput{
			Name: "wrong dialog account", MessageText: "message", AccountID: fixture.accountA, SharedDialogIDs: []uuid.UUID{fixture.sharedWrongAccount},
		})
		assertMailingConsoleError(t, failure, mailingconsole.ErrNotFound)
	})
	t.Run("unusable shared dialogs are rejected", func(t *testing.T) {
		for _, dialogID := range []uuid.UUID{fixture.sharedLeft, fixture.sharedNoSend, fixture.sharedNoHash, fixture.sharedZeroHash} {
			failure := createFailure(mailingconsole.CreateDraftInput{
				Name: "unusable dialog", MessageText: "message", AccountID: fixture.accountA, SharedDialogIDs: []uuid.UUID{dialogID},
			})
			if !errors.Is(failure, mailingconsole.ErrNotFound) {
				t.Fatalf("dialog %s: expected not found, got %v", dialogID, failure)
			}
		}
	})
	t.Run("private dialog is scoped to selected account", func(t *testing.T) {
		failure := createFailure(mailingconsole.CreateDraftInput{
			Name: "wrong private account", MessageText: "message", AccountID: fixture.accountA,
			PrivateTargets: []mailingconsole.PrivateTarget{{PeerType: mailingconsole.PeerTypeUser, PeerID: fixture.privateOperatorBPeer}},
		})
		assertMailingConsoleError(t, failure, mailingconsole.ErrNotFound)
	})
	t.Run("mixed valid and invalid target rolls back", func(t *testing.T) {
		before := countMailingConsoleMailings(t, ctx, database, fixture.operatorA)
		failure := createFailure(mailingconsole.CreateDraftInput{
			Name: "mixed invalid", MessageText: "message", AccountID: fixture.accountA,
			SharedDialogIDs: []uuid.UUID{fixture.sharedUsable, fixture.sharedNoSend},
		})
		assertMailingConsoleError(t, failure, mailingconsole.ErrNotFound)
		after := countMailingConsoleMailings(t, ctx, database, fixture.operatorA)
		if before != after {
			t.Fatalf("expected rollback to preserve mailing count, before %d after %d", before, after)
		}
	})
	t.Run("complete success graph", func(t *testing.T) {
		draftID, failure := operatorAMailings.CreateDraft(ctx, mailingconsole.CreateDraftInput{
			Name:            "complete draft",
			MessageText:     "complete message",
			AccountID:       fixture.accountA,
			SharedDialogIDs: []uuid.UUID{fixture.sharedUsable},
			PrivateTargets: []mailingconsole.PrivateTarget{
				{PeerType: mailingconsole.PeerTypeUser, PeerID: fixture.privateUserPeer},
				{PeerType: mailingconsole.PeerTypeChat, PeerID: fixture.privateChatPeer},
			},
		})
		if failure != nil {
			t.Fatalf("create complete draft: %v", failure)
		}
		assertMailingConsoleDraftGraph(t, ctx, database, draftID.UUID(), fixture)
		if failure = operatorAMailings.Mailing(draftID).Queue(ctx); failure != nil {
			t.Fatalf("queue complete draft: %v", failure)
		}
		assertMailingConsoleQueuedGraph(t, ctx, database, draftID.UUID(), 3)
		statusBefore, runsBefore, deliveriesBefore := mailingConsoleQueueState(t, ctx, database, draftID.UUID())
		failure = operatorAMailings.Mailing(draftID).Queue(ctx)
		assertMailingConsoleError(t, failure, mailing.ErrInvalidState)
		statusAfter, runsAfter, deliveriesAfter := mailingConsoleQueueState(t, ctx, database, draftID.UUID())
		if statusBefore != statusAfter || runsBefore != runsAfter || deliveriesBefore != deliveriesAfter {
			t.Fatalf("invalid-state queue changed state: %q/%d/%d -> %q/%d/%d", statusBefore, runsBefore, deliveriesBefore, statusAfter, runsAfter, deliveriesAfter)
		}

		legacyDraft, legacyFailure := operatorAMailings.CreateDraft(ctx, mailingconsole.CreateDraftInput{
			Name: "legacy queue draft", MessageText: "legacy message", AccountID: fixture.accountA, SharedDialogIDs: []uuid.UUID{fixture.sharedUsable},
		})
		if legacyFailure != nil {
			t.Fatalf("create legacy queue draft: %v", legacyFailure)
		}
		if legacyFailure = mailings.Mailing(legacyDraft).Queue(ctx); legacyFailure != nil {
			t.Fatalf("queue legacy draft: %v", legacyFailure)
		}
		assertMailingConsoleQueuedGraph(t, ctx, database, legacyDraft.UUID(), 1)
	})
}

type mailingConsoleFixture struct {
	operatorA            uuid.UUID
	operatorB            uuid.UUID
	accountA             uuid.UUID
	accountA2            uuid.UUID
	accountB             uuid.UUID
	sharedUsable         uuid.UUID
	sharedWrongAccount   uuid.UUID
	sharedLeft           uuid.UUID
	sharedNoSend         uuid.UUID
	sharedNoHash         uuid.UUID
	sharedZeroHash       uuid.UUID
	sharedOperatorB      uuid.UUID
	privateUserPeer      int64
	privateChatPeer      int64
	privateOperatorBPeer int64
}

func createMailingConsoleFixture(context context.Context, database *pgxpool.Pool) (mailingConsoleFixture, error) {
	fixture := mailingConsoleFixture{
		operatorA:          uuid.New(),
		operatorB:          uuid.New(),
		accountA:           uuid.New(),
		accountA2:          uuid.New(),
		accountB:           uuid.New(),
		sharedUsable:       uuid.New(),
		sharedWrongAccount: uuid.New(),
		sharedLeft:         uuid.New(),
		sharedNoSend:       uuid.New(),
		sharedNoHash:       uuid.New(),
		sharedZeroHash:     uuid.New(),
		sharedOperatorB:    uuid.New(),
	}
	fixture.privateUserPeer = consolePeerID(uuid.New())
	fixture.privateChatPeer = consolePeerID(uuid.New())
	fixture.privateOperatorBPeer = consolePeerID(uuid.New())

	transaction, failure := database.Begin(context)
	if failure != nil {
		return fixture, failure
	}
	defer transaction.Rollback(context)
	exec := func(query string, arguments ...any) error {
		_, failure := transaction.Exec(context, query, arguments...)
		return failure
	}
	if failure = exec(`INSERT INTO operators (id, username) VALUES ($1, $2), ($3, $4)`, fixture.operatorA, "console-a-"+fixture.operatorA.String()[:8], fixture.operatorB, "console-b-"+fixture.operatorB.String()[:8]); failure != nil {
		return fixture, fmt.Errorf("insert operators: %w", failure)
	}
	if failure = exec(
		`INSERT INTO operator_accounts (id, operator_id, phone, telegram_username, telegram_first_name, api_id)
		 VALUES ($1, $2, '+12025551001', $3, 'Console A', 1),
		        ($4, $2, '+12025551002', $5, 'Console A2', 2),
		        ($6, $7, '+12025551003', $8, 'Console B', 3)`,
		fixture.accountA, fixture.operatorA, "console-a-"+fixture.accountA.String()[:8],
		fixture.accountA2, "console-a2-"+fixture.accountA2.String()[:8],
		fixture.accountB, fixture.operatorB, "console-b-"+fixture.accountB.String()[:8],
	); failure != nil {
		return fixture, fmt.Errorf("insert accounts: %w", failure)
	}
	insertShared := func(dialogID, accountID uuid.UUID, status string, canSend bool, accessHash *int64) error {
		if failure := exec(`INSERT INTO telegram_shared_dialogs (id, telegram_peer_id, dialog_kind, title, metadata_synced_at) VALUES ($1, $2, 'supergroup', $3, CURRENT_TIMESTAMP)`, dialogID, consolePeerID(dialogID), "Console "+dialogID.String()[:8]); failure != nil {
			return failure
		}
		_, failure := transaction.Exec(context, `INSERT INTO operator_accounts_shared_dialogs (account_id, shared_dialog_id, access_hash, membership_status, last_joined_at, can_send) VALUES ($1, $2, $3, $4::membership_status_type, CURRENT_TIMESTAMP, $5)`, accountID, dialogID, accessHash, status, canSend)
		return failure
	}
	sharedFixtures := []struct {
		id      uuid.UUID
		account uuid.UUID
		status  string
		canSend bool
		hash    *int64
	}{
		{fixture.sharedUsable, fixture.accountA, "joined", true, consoleInt64(1001)},
		{fixture.sharedWrongAccount, fixture.accountA2, "joined", true, consoleInt64(1002)},
		{fixture.sharedLeft, fixture.accountA, "left", true, consoleInt64(1003)},
		{fixture.sharedNoSend, fixture.accountA, "joined", false, consoleInt64(1004)},
		{fixture.sharedNoHash, fixture.accountA, "joined", true, nil},
		{fixture.sharedZeroHash, fixture.accountA, "joined", true, consoleInt64(0)},
		{fixture.sharedOperatorB, fixture.accountB, "joined", true, consoleInt64(2001)},
	}
	for _, shared := range sharedFixtures {
		if failure = insertShared(shared.id, shared.account, shared.status, shared.canSend, shared.hash); failure != nil {
			return fixture, fmt.Errorf("insert shared fixture: %w", failure)
		}
	}
	insertPrivate := func(accountID uuid.UUID, peerType mailingconsole.PeerType, peerID int64, hash *int64) error {
		_, failure := transaction.Exec(context, `INSERT INTO operator_accounts_private_dialogs (account_id, peer_type, telegram_peer_id, title, access_hash, can_send) VALUES ($1, $2::telegram_peer_type, $3, $4, $5, TRUE)`, accountID, peerType, peerID, "Console private", hash)
		return failure
	}
	if failure = insertPrivate(fixture.accountA, mailingconsole.PeerTypeUser, fixture.privateUserPeer, consoleInt64(3001)); failure != nil {
		return fixture, fmt.Errorf("insert private user: %w", failure)
	}
	if failure = insertPrivate(fixture.accountA, mailingconsole.PeerTypeChat, fixture.privateChatPeer, nil); failure != nil {
		return fixture, fmt.Errorf("insert private chat: %w", failure)
	}
	if failure = insertPrivate(fixture.accountB, mailingconsole.PeerTypeUser, fixture.privateOperatorBPeer, consoleInt64(4001)); failure != nil {
		return fixture, fmt.Errorf("insert operator B private user: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fixture, fmt.Errorf("commit fixture: %w", failure)
	}
	return fixture, nil
}

func cleanupMailingConsoleFixture(context context.Context, database *pgxpool.Pool, fixture mailingConsoleFixture) error {
	transaction, failure := database.Begin(context)
	if failure != nil {
		return failure
	}
	defer transaction.Rollback(context)
	if _, failure = transaction.Exec(
		context,
		`DELETE FROM telegram_mailing_routes
		 WHERE mailing_id IN (
			SELECT id FROM mailings WHERE operator_id IN ($1, $2)
		 )`,
		fixture.operatorA,
		fixture.operatorB,
	); failure != nil {
		return fmt.Errorf("delete fixture mailing routes: %w", failure)
	}
	if _, failure = transaction.Exec(context, `DELETE FROM mailings WHERE operator_id IN ($1, $2)`, fixture.operatorA, fixture.operatorB); failure != nil {
		return fmt.Errorf("delete fixture mailings: %w", failure)
	}
	if _, failure = transaction.Exec(context, `DELETE FROM operators WHERE id IN ($1, $2)`, fixture.operatorA, fixture.operatorB); failure != nil {
		return fmt.Errorf("delete fixture operators: %w", failure)
	}
	sharedIDs := []uuid.UUID{fixture.sharedUsable, fixture.sharedWrongAccount, fixture.sharedLeft, fixture.sharedNoSend, fixture.sharedNoHash, fixture.sharedZeroHash, fixture.sharedOperatorB}
	if _, failure = transaction.Exec(context, `DELETE FROM telegram_shared_dialogs WHERE id = ANY($1::uuid[])`, sharedIDs); failure != nil {
		return fmt.Errorf("delete fixture shared dialogs: %w", failure)
	}
	return transaction.Commit(context)
}

func assertMailingConsoleDraftGraph(t *testing.T, context context.Context, database *pgxpool.Pool, mailingID uuid.UUID, fixture mailingConsoleFixture) {
	t.Helper()
	var (
		operatorID uuid.UUID
		name       string
		message    string
		status     string
	)
	if failure := database.QueryRow(context, `SELECT operator_id, name, message_text, status::text FROM mailings WHERE id = $1`, mailingID).Scan(&operatorID, &name, &message, &status); failure != nil {
		t.Fatalf("load draft row: %v", failure)
	}
	if operatorID != fixture.operatorA || name != "complete draft" || message != "complete message" || status != "draft" {
		t.Fatalf("unexpected draft row: %s %q %q %q", operatorID, name, message, status)
	}
	var mode, timezone string
	if failure := database.QueryRow(context, `SELECT mode::text, timezone FROM mailing_schedules WHERE mailing_id = $1`, mailingID).Scan(&mode, &timezone); failure != nil {
		t.Fatalf("load draft schedule: %v", failure)
	}
	if mode != "always" || timezone != "UTC" {
		t.Fatalf("unexpected schedule: %s %s", mode, timezone)
	}
	var routeAccount uuid.UUID
	if failure := database.QueryRow(context, `SELECT account_id FROM telegram_mailing_routes WHERE mailing_id = $1`, mailingID).Scan(&routeAccount); failure != nil {
		t.Fatalf("load draft route: %v", failure)
	}
	if routeAccount != fixture.accountA {
		t.Fatalf("expected route account %s, got %s", fixture.accountA, routeAccount)
	}
	rows, failure := database.Query(context, `SELECT recipient.position, telegram.shared_dialog_id, telegram.private_account_id, telegram.private_peer_type::text, telegram.private_peer_id FROM mailing_recipients AS recipient JOIN telegram_mailing_recipients AS telegram ON telegram.mailing_id = recipient.mailing_id AND telegram.recipient_id = recipient.id WHERE recipient.mailing_id = $1 ORDER BY recipient.position`, mailingID)
	if failure != nil {
		t.Fatalf("load draft target graph: %v", failure)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var (
			position         int
			sharedDialogID   pgtype.UUID
			privateAccountID pgtype.UUID
			privatePeerType  pgtype.Text
			privatePeerID    pgtype.Int8
		)
		if failure = rows.Scan(&position, &sharedDialogID, &privateAccountID, &privatePeerType, &privatePeerID); failure != nil {
			t.Fatalf("scan draft target graph: %v", failure)
		}
		if position != count {
			t.Fatalf("expected stable position %d, got %d", count, position)
		}
		switch count {
		case 0:
			if !sharedDialogID.Valid || uuid.UUID(sharedDialogID.Bytes) != fixture.sharedUsable || privateAccountID.Valid {
				t.Fatalf("unexpected shared target row")
			}
		case 1:
			if sharedDialogID.Valid || !privateAccountID.Valid || uuid.UUID(privateAccountID.Bytes) != fixture.accountA || !privatePeerType.Valid || privatePeerType.String != string(mailingconsole.PeerTypeUser) || !privatePeerID.Valid || privatePeerID.Int64 != fixture.privateUserPeer {
				t.Fatalf("unexpected private user target row")
			}
		case 2:
			if sharedDialogID.Valid || !privateAccountID.Valid || uuid.UUID(privateAccountID.Bytes) != fixture.accountA || !privatePeerType.Valid || privatePeerType.String != string(mailingconsole.PeerTypeChat) || !privatePeerID.Valid || privatePeerID.Int64 != fixture.privateChatPeer {
				t.Fatalf("unexpected private chat target row")
			}
		}
		count++
	}
	if failure = rows.Err(); failure != nil {
		t.Fatalf("read draft target graph: %v", failure)
	}
	if count != 3 {
		t.Fatalf("expected three target rows, got %d", count)
	}
}

func createMailingConsoleEmptyDraft(context context.Context, database *pgxpool.Pool, fixture mailingConsoleFixture) (uuid.UUID, error) {
	draftID := uuid.New()
	transaction, failure := database.Begin(context)
	if failure != nil {
		return uuid.Nil, failure
	}
	defer transaction.Rollback(context)
	if _, failure = transaction.Exec(context, `INSERT INTO mailings (id, operator_id, name, message_text, status) VALUES ($1, $2, 'empty queue draft', 'empty queue message', 'draft')`, draftID, fixture.operatorA); failure != nil {
		return uuid.Nil, failure
	}
	if _, failure = transaction.Exec(context, `INSERT INTO mailing_schedules (mailing_id, mode, timezone) VALUES ($1, 'always', 'UTC')`, draftID); failure != nil {
		return uuid.Nil, failure
	}
	if _, failure = transaction.Exec(context, `INSERT INTO telegram_mailing_routes (mailing_id, account_id) VALUES ($1, $2)`, draftID, fixture.accountA); failure != nil {
		return uuid.Nil, failure
	}
	if failure = transaction.Commit(context); failure != nil {
		return uuid.Nil, failure
	}
	return draftID, nil
}

func mailingConsoleQueueState(t *testing.T, context context.Context, database *pgxpool.Pool, mailingID uuid.UUID) (string, int, int) {
	t.Helper()
	var status string
	if failure := database.QueryRow(context, `SELECT status::text FROM mailings WHERE id = $1`, mailingID).Scan(&status); failure != nil {
		t.Fatalf("load queue status: %v", failure)
	}
	var runs, deliveries int
	if failure := database.QueryRow(context, `SELECT COUNT(*) FROM mailing_runs WHERE mailing_id = $1`, mailingID).Scan(&runs); failure != nil {
		t.Fatalf("count mailing runs: %v", failure)
	}
	if failure := database.QueryRow(context, `SELECT COUNT(*) FROM mailing_deliveries WHERE mailing_id = $1`, mailingID).Scan(&deliveries); failure != nil {
		t.Fatalf("count mailing deliveries: %v", failure)
	}
	return status, runs, deliveries
}

func assertMailingConsoleQueuedGraph(t *testing.T, context context.Context, database *pgxpool.Pool, mailingID uuid.UUID, expected int) {
	t.Helper()
	var status string
	if failure := database.QueryRow(context, `SELECT status::text FROM mailings WHERE id = $1`, mailingID).Scan(&status); failure != nil {
		t.Fatalf("load queued mailing status: %v", failure)
	}
	if status != "queued" {
		t.Fatalf("expected queued status, got %s", status)
	}
	var (
		genericCount      int
		telegramCount     int
		linkedCount       int
		uniqueRandomCount int
		minimumRandom     pgtype.Int8
	)
	if failure := database.QueryRow(
		context,
		`SELECT
			(SELECT COUNT(*) FROM mailing_deliveries WHERE mailing_id = $1),
			(SELECT COUNT(*) FROM telegram_mailing_deliveries WHERE mailing_id = $1),
			(SELECT COUNT(*)
			 FROM mailing_deliveries AS delivery
			 JOIN telegram_mailing_deliveries AS telegram
			   ON telegram.mailing_id = delivery.mailing_id
			  AND telegram.run_id = delivery.run_id
			  AND telegram.recipient_id = delivery.recipient_id
			 WHERE delivery.mailing_id = $1),
			(SELECT COUNT(DISTINCT random_id) FROM telegram_mailing_deliveries WHERE mailing_id = $1),
			(SELECT MIN(random_id) FROM telegram_mailing_deliveries WHERE mailing_id = $1)`,
		mailingID,
	).Scan(&genericCount, &telegramCount, &linkedCount, &uniqueRandomCount, &minimumRandom); failure != nil {
		t.Fatalf("load queued delivery graph: %v", failure)
	}
	if genericCount != expected || telegramCount != expected || linkedCount != expected || uniqueRandomCount != expected || !minimumRandom.Valid || minimumRandom.Int64 == 0 {
		t.Fatalf("unexpected queued delivery graph: generic=%d telegram=%d linked=%d unique=%d minimum=%v", genericCount, telegramCount, linkedCount, uniqueRandomCount, minimumRandom)
	}
}

func countMailingConsoleMailings(t *testing.T, context context.Context, database *pgxpool.Pool, operatorID uuid.UUID) int {
	t.Helper()
	var count int
	if failure := database.QueryRow(context, `SELECT COUNT(*) FROM mailings WHERE operator_id = $1`, operatorID).Scan(&count); failure != nil {
		t.Fatalf("count operator mailings: %v", failure)
	}
	return count
}

func assertMailingConsoleError(t *testing.T, failure error, expected error) {
	t.Helper()
	if !errors.Is(failure, expected) {
		t.Fatalf("expected %v, got %v", expected, failure)
	}
}

func newIsolatedMailingConsoleDatabase(
	ctx context.Context,
	t *testing.T,
	databaseURL string,
) (*pgxpool.Pool, string, error) {
	t.Helper()
	baseConfig, failure := pgxpool.ParseConfig(databaseURL)
	if failure != nil {
		return nil, "", fmt.Errorf("parse PostgreSQL URL: %w", failure)
	}
	adminDatabase, failure := pgxpool.NewWithConfig(ctx, baseConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open PostgreSQL admin pool: %w", failure)
	}
	if failure = adminDatabase.Ping(ctx); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("ping PostgreSQL admin pool: %w", failure)
	}

	schema := "mailing_console_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if _, failure = adminDatabase.Exec(ctx, "CREATE SCHEMA "+quotedSchema); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("create isolated schema: %w", failure)
	}

	var database *pgxpool.Pool
	t.Cleanup(func() {
		if database != nil {
			database.Close()
		}
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := adminDatabase.Exec(cleanupContext, "DROP SCHEMA "+quotedSchema+" CASCADE"); cleanupFailure != nil {
			t.Errorf("drop isolated schema %q: %v", schema, cleanupFailure)
		}
		adminDatabase.Close()
	})

	isolatedURL, failure := mailingConsoleDatabaseURL(databaseURL, schema)
	if failure != nil {
		return nil, "", failure
	}
	if failure = applyMailingConsoleMigrations(ctx, isolatedURL); failure != nil {
		return nil, "", fmt.Errorf("apply migrations to isolated schema: %w", failure)
	}
	isolatedConfig := baseConfig.Copy()
	if isolatedConfig.ConnConfig.RuntimeParams == nil {
		isolatedConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	options := isolatedConfig.ConnConfig.RuntimeParams["options"]
	if options != "" {
		options += " "
	}
	isolatedConfig.ConnConfig.RuntimeParams["options"] = options + "-c search_path=" + schema
	database, failure = pgxpool.NewWithConfig(ctx, isolatedConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open isolated PostgreSQL pool: %w", failure)
	}
	if failure = database.Ping(ctx); failure != nil {
		database.Close()
		return nil, "", fmt.Errorf("ping isolated PostgreSQL pool: %w", failure)
	}
	return database, schema, nil
}

func mailingConsoleDatabaseURL(databaseURL, schema string) (string, error) {
	parsedURL, failure := url.Parse(databaseURL)
	if failure != nil {
		return "", fmt.Errorf("parse isolated PostgreSQL URL: %w", failure)
	}
	if parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql" {
		return "", errors.New("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	options := query.Get("options")
	if options != "" {
		options += " "
	}
	query.Set("options", options+"-c search_path="+schema)
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String(), nil
}

func applyMailingConsoleMigrations(context context.Context, databaseURL string) error {
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
		return errors.New("locate mailing console integration test")
	}
	provider, failure := goose.NewProvider(
		goose.DialectPostgres,
		database,
		os.DirFS(filepath.Join(filepath.Dir(filename), "../../../../../migrations")),
		goose.WithAllowOutofOrder(true),
	)
	if failure != nil {
		return failure
	}
	if _, failure = provider.Up(context); failure == nil {
		return errors.New("apply migrations without delivery execution v2 acknowledgement")
	}
	if _, failure = database.ExecContext(context, `INSERT INTO delivery_execution_v2_cutover_ack (acknowledgement_id, acknowledged_by) VALUES (TRUE, current_user)`); failure != nil {
		return fmt.Errorf("acknowledge delivery execution v2 cutover: %w", failure)
	}
	_, failure = provider.Up(context)
	return failure
}

func consolePrivateKey(peerType mailingconsole.PeerType, peerID int64) string {
	return fmt.Sprintf("%s:%d", peerType, peerID)
}

func consolePeerID(value uuid.UUID) int64 {
	peerID := int64(value[0])<<56 | int64(value[1])<<48 | int64(value[2])<<40 | int64(value[3])<<32 | int64(value[4])<<24 | int64(value[5])<<16 | int64(value[6])<<8 | int64(value[7])
	if peerID < 0 {
		peerID = -peerID
	}
	if peerID == 0 {
		return 1
	}
	return peerID
}

func consoleInt64(value int64) *int64 {
	return &value
}
