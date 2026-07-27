package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

// queryRow is the narrow database contract needed by telegramPeerLookup.
type queryRow interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

var _ telegram.PeerLookup = telegramPeerLookup{}

// telegramPeerLookup resolves account-scoped Telegram recipient projections.
type telegramPeerLookup struct {
	database queryRow
}

// NewTelegramPeerLookup creates an account-scoped Telegram peer lookup.
func NewTelegramPeerLookup(database *pgxpool.Pool) telegram.PeerLookup {
	if database == nil {
		return telegramPeerLookup{}
	}
	return newTelegramPeerLookup(database)
}

func newTelegramPeerLookup(database queryRow) telegram.PeerLookup {
	return telegramPeerLookup{database: database}
}

func (lookup telegramPeerLookup) Lookup(
	context context.Context,
	request telegram.PeerLookupRequest,
) (telegram.PeerProjection, error) {
	if context == nil {
		panic("lookup Telegram target without context")
	}
	if lookup.database == nil {
		panic("lookup Telegram target without database")
	}
	if request.AccountID == uuid.Nil {
		panic("lookup Telegram target without account identity")
	}
	if request.RecipientID == uuid.Nil {
		panic("lookup Telegram target without recipient identity")
	}

	var (
		peerType   string
		peerID     int64
		accessHash pgtype.Int8
	)
	failure := lookup.database.QueryRow(
		context,
		`SELECT
			CASE
				WHEN target.shared_dialog_id IS NOT NULL THEN 'channel'
				ELSE target.private_peer_type::text
			END AS peer_type,
			CASE
				WHEN target.shared_dialog_id IS NOT NULL THEN shared.telegram_peer_id
				ELSE private.telegram_peer_id
			END AS peer_id,
			CASE
				WHEN target.shared_dialog_id IS NOT NULL THEN shared_access.access_hash
				ELSE private.access_hash
			END AS access_hash
		FROM mailing_recipients AS recipient
		JOIN telegram_mailing_recipients AS target
		  ON target.mailing_id = recipient.mailing_id
		 AND target.recipient_id = recipient.id
		JOIN telegram_mailing_routes AS route
		  ON route.mailing_id = recipient.mailing_id
		 AND route.account_id = $2
		LEFT JOIN telegram_shared_dialogs AS shared
		  ON shared.id = target.shared_dialog_id
		LEFT JOIN operator_accounts_shared_dialogs AS shared_access
		  ON shared_access.account_id = route.account_id
		 AND shared_access.shared_dialog_id = target.shared_dialog_id
		LEFT JOIN operator_accounts_private_dialogs AS private
		  ON private.account_id = route.account_id
		 AND private.account_id = target.private_account_id
		 AND private.peer_type = target.private_peer_type
		 AND private.telegram_peer_id = target.private_peer_id
		WHERE recipient.id = $1
		  AND (
			(target.shared_dialog_id IS NOT NULL
			 AND shared.id IS NOT NULL
			 AND shared_access.shared_dialog_id IS NOT NULL)
			OR
			(target.shared_dialog_id IS NULL
			 AND target.private_account_id IS NOT NULL
			 AND target.private_peer_type IS NOT NULL
			 AND target.private_peer_id IS NOT NULL
			 AND private.account_id IS NOT NULL)
		)`,
		request.RecipientID,
		request.AccountID,
	).Scan(&peerType, &peerID, &accessHash)
	if errors.Is(failure, pgx.ErrNoRows) {
		return telegram.PeerProjection{}, fmt.Errorf(
			"lookup Telegram target for account %s and recipient %s: %w",
			request.AccountID,
			request.RecipientID,
			telegram.ErrTargetNotFound,
		)
	}
	if failure != nil {
		return telegram.PeerProjection{}, fmt.Errorf(
			"lookup Telegram target for account %s and recipient %s: %w",
			request.AccountID,
			request.RecipientID,
			failure,
		)
	}

	projection := telegram.PeerProjection{
		Type: telegram.PeerType(peerType),
		ID:   peerID,
	}
	if accessHash.Valid {
		value := accessHash.Int64
		projection.AccessHash = &value
	}
	return projection, nil
}
