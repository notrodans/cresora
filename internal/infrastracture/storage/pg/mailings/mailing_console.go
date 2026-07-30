package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/application/services/mailingconsole"
	"github.com/notrodans/cresora/internal/domain/mailing"
)

type privateTargetKey struct {
	peerType mailingconsole.PeerType
	peerID   int64
}

// pgMailingConsole is the read-only PostgreSQL mailing console projection.
type pgMailingConsole struct {
	database mailingDatabase
}

var _ mailingconsole.Console = pgMailingConsole{}

func NewMailingConsole(database *pgxpool.Pool) pgMailingConsole {
	return pgMailingConsole{database: database}
}

func (all pgMailingConsole) OperatorExists(context context.Context, operatorID uuid.UUID) (bool, error) {
	all.validate(operatorID)
	var exists bool
	if failure := all.database.QueryRow(
		context,
		`SELECT EXISTS (SELECT 1 FROM operators WHERE id = $1)`,
		operatorID,
	).Scan(&exists); failure != nil {
		return false, fmt.Errorf("verify operator exists: %w", failure)
	}
	return exists, nil
}

func (all pgMailingConsole) Dashboard(
	context context.Context,
	operatorID uuid.UUID,
) ([]mailingconsole.Account, []mailingconsole.SharedDialog, []mailingconsole.PrivateDialog, []mailingconsole.MailingSummary, error) {
	all.validate(operatorID)
	accounts, failure := all.accounts(context, operatorID)
	if failure != nil {
		return nil, nil, nil, nil, failure
	}
	dialogs, failure := all.sharedDialogs(context, operatorID)
	if failure != nil {
		return nil, nil, nil, nil, failure
	}
	privateDialogs, failure := all.privateDialogs(context, operatorID)
	if failure != nil {
		return nil, nil, nil, nil, failure
	}
	mailings, failure := all.mailings(context, operatorID)
	if failure != nil {
		return nil, nil, nil, nil, failure
	}
	return accounts, dialogs, privateDialogs, mailings, nil
}

func (all pgMailingConsole) accounts(
	context context.Context,
	operatorID uuid.UUID,
) ([]mailingconsole.Account, error) {
	rows, failure := all.database.Query(
		context,
		`SELECT id, phone, telegram_username, telegram_first_name, telegram_last_name
		 FROM operator_accounts
		 WHERE operator_id = $1
		 ORDER BY id`,
		operatorID,
	)
	if failure != nil {
		return nil, fmt.Errorf("list operator Telegram accounts: %w", failure)
	}
	defer rows.Close()
	accounts := make([]mailingconsole.Account, 0)
	for rows.Next() {
		var (
			account  mailingconsole.Account
			lastName pgtype.Text
		)
		if failure = rows.Scan(
			&account.ID,
			&account.Phone,
			&account.TelegramUsername,
			&account.TelegramFirstName,
			&lastName,
		); failure != nil {
			return nil, fmt.Errorf("scan operator Telegram account: %w", failure)
		}
		if lastName.Valid {
			account.TelegramLastName = lastName.String
		}
		accounts = append(accounts, account)
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("read operator Telegram accounts: %w", failure)
	}
	return accounts, nil
}

func (all pgMailingConsole) sharedDialogs(
	context context.Context,
	operatorID uuid.UUID,
) ([]mailingconsole.SharedDialog, error) {
	rows, failure := all.database.Query(
		context,
		`SELECT dialog.id,
		        shared_access.account_id,
		        dialog.telegram_peer_id,
		        dialog.dialog_kind::text,
		        dialog.title,
		        dialog.canonical_username,
		        shared_access.access_hash
		 FROM operator_accounts AS account
		 JOIN operator_accounts_shared_dialogs AS shared_access
		   ON shared_access.account_id = account.id
		 JOIN telegram_shared_dialogs AS dialog
		   ON dialog.id = shared_access.shared_dialog_id
		 WHERE account.operator_id = $1
		   AND shared_access.membership_status = 'joined'
		   AND shared_access.can_send
		   AND shared_access.access_hash IS NOT NULL
		   AND shared_access.access_hash <> 0
		 ORDER BY shared_access.account_id, dialog.id`,
		operatorID,
	)
	if failure != nil {
		return nil, fmt.Errorf("list sendable shared Telegram dialogs: %w", failure)
	}
	defer rows.Close()
	dialogs := make([]mailingconsole.SharedDialog, 0)
	for rows.Next() {
		var (
			dialog   mailingconsole.SharedDialog
			username pgtype.Text
		)
		if failure = rows.Scan(
			&dialog.ID,
			&dialog.AccountID,
			&dialog.PeerID,
			&dialog.Kind,
			&dialog.Title,
			&username,
			&dialog.AccessHash,
		); failure != nil {
			return nil, fmt.Errorf("scan sendable shared Telegram dialog: %w", failure)
		}
		if username.Valid {
			dialog.CanonicalUsername = username.String
		}
		dialogs = append(dialogs, dialog)
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("read sendable shared Telegram dialogs: %w", failure)
	}
	return dialogs, nil
}

func (all pgMailingConsole) privateDialogs(
	context context.Context,
	operatorID uuid.UUID,
) ([]mailingconsole.PrivateDialog, error) {
	rows, failure := all.database.Query(
		context,
		`SELECT private_dialog.account_id,
		        private_dialog.peer_type::text,
		        private_dialog.telegram_peer_id,
		        private_dialog.title,
		        private_dialog.username,
		        private_dialog.access_hash
		 FROM operator_accounts AS account
		 JOIN operator_accounts_private_dialogs AS private_dialog
		   ON private_dialog.account_id = account.id
		 WHERE account.operator_id = $1
		   AND private_dialog.can_send
		   AND (
			private_dialog.peer_type = 'chat'
			OR (private_dialog.access_hash IS NOT NULL AND private_dialog.access_hash <> 0)
		   )
		 ORDER BY private_dialog.account_id, private_dialog.peer_type, private_dialog.telegram_peer_id`,
		operatorID,
	)
	if failure != nil {
		return nil, fmt.Errorf("list sendable private Telegram dialogs: %w", failure)
	}
	defer rows.Close()
	dialogs := make([]mailingconsole.PrivateDialog, 0)
	for rows.Next() {
		var (
			dialog     mailingconsole.PrivateDialog
			peerType   string
			username   pgtype.Text
			accessHash pgtype.Int8
		)
		if failure = rows.Scan(
			&dialog.AccountID,
			&peerType,
			&dialog.PeerID,
			&dialog.Title,
			&username,
			&accessHash,
		); failure != nil {
			return nil, fmt.Errorf("scan sendable private Telegram dialog: %w", failure)
		}
		dialog.PeerType = mailingconsole.PeerType(peerType)
		if username.Valid {
			dialog.Username = username.String
		}
		if accessHash.Valid {
			value := accessHash.Int64
			dialog.AccessHash = &value
		}
		dialogs = append(dialogs, dialog)
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("read sendable private Telegram dialogs: %w", failure)
	}
	return dialogs, nil
}

func (all pgMailingConsole) mailings(
	context context.Context,
	operatorID uuid.UUID,
) ([]mailingconsole.MailingSummary, error) {
	rows, failure := all.database.Query(
		context,
		`SELECT mailing.id,
		        mailing.name,
		        mailing.status::text,
		        route.account_id,
		        COUNT(recipient.id),
		        mailing.created_at,
		        mailing.updated_at
		 FROM mailings AS mailing
		 LEFT JOIN telegram_mailing_routes AS route
		   ON route.mailing_id = mailing.id
		 LEFT JOIN mailing_recipients AS recipient
		   ON recipient.mailing_id = mailing.id
		 WHERE mailing.operator_id = $1
		 GROUP BY mailing.id, route.account_id
		 ORDER BY mailing.created_at DESC, mailing.id DESC`,
		operatorID,
	)
	if failure != nil {
		return nil, fmt.Errorf("list operator mailings: %w", failure)
	}
	defer rows.Close()
	mailings := make([]mailingconsole.MailingSummary, 0)
	for rows.Next() {
		var (
			summary      mailingconsole.MailingSummary
			routeAccount pgtype.UUID
		)
		if failure = rows.Scan(
			&summary.ID,
			&summary.Name,
			&summary.Status,
			&routeAccount,
			&summary.RecipientCount,
			&summary.CreatedAt,
			&summary.UpdatedAt,
		); failure != nil {
			return nil, fmt.Errorf("scan operator mailing: %w", failure)
		}
		if routeAccount.Valid {
			summary.AccountID = uuid.UUID(routeAccount.Bytes)
		}
		mailings = append(mailings, summary)
	}
	if failure = rows.Err(); failure != nil {
		return nil, fmt.Errorf("read operator mailings: %w", failure)
	}
	return mailings, nil
}

func (all pgOperatorMailings) CreateDraft(
	context context.Context,
	input mailingconsole.CreateDraftInput,
) (mailing.ID, error) {
	all.validate()
	operatorID := all.operatorID
	transaction, failure := all.database.Begin(context)
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("begin mailing draft transaction: %w", failure)
	}
	defer func() {
		_ = transaction.Rollback(context)
	}()

	var accountID uuid.UUID
	failure = transaction.QueryRow(
		context,
		`SELECT id
		 FROM operator_accounts
		 WHERE id = $1
		   AND operator_id = $2
		 FOR UPDATE`,
		input.AccountID,
		operatorID,
	).Scan(&accountID)
	if errors.Is(failure, pgx.ErrNoRows) {
		return mailing.ID{}, fmt.Errorf("%w: selected account is not owned by operator", mailingconsole.ErrNotFound)
	}
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("verify selected account ownership: %w", failure)
	}

	sharedRows, failure := transaction.Query(
		context,
		`SELECT dialog.id
		 FROM telegram_shared_dialogs AS dialog
		 JOIN operator_accounts_shared_dialogs AS shared_access
		   ON shared_access.shared_dialog_id = dialog.id
		 JOIN operator_accounts AS account
		   ON account.id = shared_access.account_id
		 WHERE account.operator_id = $2
		   AND shared_access.account_id = $1
		   AND shared_access.membership_status = 'joined'
		   AND shared_access.can_send
		   AND shared_access.access_hash IS NOT NULL
		   AND shared_access.access_hash <> 0
		 FOR UPDATE OF shared_access`,
		input.AccountID,
		operatorID,
	)
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("lock shared Telegram dialog availability: %w", failure)
	}
	validShared := make(map[uuid.UUID]struct{}, len(input.SharedDialogIDs))
	for sharedRows.Next() {
		var dialogID uuid.UUID
		if failure = sharedRows.Scan(&dialogID); failure != nil {
			sharedRows.Close()
			return mailing.ID{}, fmt.Errorf("scan locked shared Telegram dialog: %w", failure)
		}
		validShared[dialogID] = struct{}{}
	}
	if failure = sharedRows.Err(); failure != nil {
		sharedRows.Close()
		return mailing.ID{}, fmt.Errorf("read locked shared Telegram dialogs: %w", failure)
	}
	sharedRows.Close()
	for _, dialogID := range input.SharedDialogIDs {
		if _, exists := validShared[dialogID]; !exists {
			return mailing.ID{}, fmt.Errorf("%w: shared dialog is unavailable", mailingconsole.ErrNotFound)
		}
	}

	privateRows, failure := transaction.Query(
		context,
		`SELECT private_dialog.peer_type::text, private_dialog.telegram_peer_id
		 FROM operator_accounts_private_dialogs AS private_dialog
		 JOIN operator_accounts AS account
		   ON account.id = private_dialog.account_id
		 WHERE account.operator_id = $2
		   AND private_dialog.account_id = $1
		   AND private_dialog.can_send
		   AND (
			private_dialog.peer_type = 'chat'
			OR (private_dialog.access_hash IS NOT NULL AND private_dialog.access_hash <> 0)
		   )
		 FOR UPDATE OF private_dialog`,
		input.AccountID,
		operatorID,
	)
	if failure != nil {
		return mailing.ID{}, fmt.Errorf("lock private Telegram dialog availability: %w", failure)
	}
	validPrivate := make(map[privateTargetKey]struct{}, len(input.PrivateTargets))
	for privateRows.Next() {
		var (
			peerType string
			peerID   int64
		)
		if failure = privateRows.Scan(&peerType, &peerID); failure != nil {
			privateRows.Close()
			return mailing.ID{}, fmt.Errorf("scan locked private Telegram dialog: %w", failure)
		}
		validPrivate[privateTargetKey{
			peerType: mailingconsole.PeerType(peerType),
			peerID:   peerID,
		}] = struct{}{}
	}
	if failure = privateRows.Err(); failure != nil {
		privateRows.Close()
		return mailing.ID{}, fmt.Errorf("read locked private Telegram dialogs: %w", failure)
	}
	privateRows.Close()
	for _, target := range input.PrivateTargets {
		if _, exists := validPrivate[privateTargetKey{
			peerType: target.PeerType,
			peerID:   target.PeerID,
		}]; !exists {
			return mailing.ID{}, fmt.Errorf("%w: private dialog is unavailable", mailingconsole.ErrNotFound)
		}
	}

	draftID := uuid.New()
	if _, failure = transaction.Exec(
		context,
		`INSERT INTO mailings (id, operator_id, name, message_text, status)
		 VALUES ($1, $2, $3, $4, 'draft')`,
		draftID,
		operatorID,
		input.Name,
		input.MessageText,
	); failure != nil {
		return mailing.ID{}, fmt.Errorf("insert mailing draft: %w", failure)
	}
	if _, failure = transaction.Exec(
		context,
		`INSERT INTO mailing_schedules (mailing_id, mode, timezone)
		 VALUES ($1, 'always', 'UTC')`,
		draftID,
	); failure != nil {
		return mailing.ID{}, fmt.Errorf("insert mailing draft schedule: %w", failure)
	}
	if _, failure = transaction.Exec(
		context,
		`INSERT INTO telegram_mailing_routes (mailing_id, account_id)
		 VALUES ($1, $2)`,
		draftID,
		input.AccountID,
	); failure != nil {
		return mailing.ID{}, fmt.Errorf("insert mailing draft route: %w", failure)
	}
	insertRecipient := func(position int, insertTarget func(uuid.UUID) error) error {
		recipientID := uuid.New()
		if _, failure = transaction.Exec(
			context,
			`INSERT INTO mailing_recipients (mailing_id, id, position)
			 VALUES ($1, $2, $3)`,
			draftID,
			recipientID,
			position,
		); failure != nil {
			return failure
		}
		if failure = insertTarget(recipientID); failure != nil {
			return failure
		}
		return nil
	}
	position := 0
	for _, dialogID := range input.SharedDialogIDs {
		if failure = insertRecipient(position, func(recipientID uuid.UUID) error {
			_, insertFailure := transaction.Exec(
				context,
				`INSERT INTO telegram_mailing_recipients (mailing_id, recipient_id, shared_dialog_id)
				 VALUES ($1, $2, $3)`,
				draftID,
				recipientID,
				dialogID,
			)
			return insertFailure
		}); failure != nil {
			return mailing.ID{}, fmt.Errorf("insert mailing draft recipient: %w", failure)
		}
		position++
	}
	for _, target := range input.PrivateTargets {
		if failure = insertRecipient(position, func(recipientID uuid.UUID) error {
			_, insertFailure := transaction.Exec(
				context,
				`INSERT INTO telegram_mailing_recipients (mailing_id, recipient_id, private_account_id, private_peer_type, private_peer_id)
				 VALUES ($1, $2, $3, $4::telegram_peer_type, $5)`,
				draftID,
				recipientID,
				input.AccountID,
				target.PeerType,
				target.PeerID,
			)
			return insertFailure
		}); failure != nil {
			return mailing.ID{}, fmt.Errorf("insert mailing draft private recipient: %w", failure)
		}
		position++
	}
	if failure = transaction.Commit(context); failure != nil {
		return mailing.ID{}, fmt.Errorf("commit mailing draft: %w", failure)
	}
	return mailing.Identity(draftID), nil
}

func (all pgMailingConsole) validate(operatorID uuid.UUID) {
	validateOperatorID(operatorID, "use mailing console PostgreSQL projection")
}
