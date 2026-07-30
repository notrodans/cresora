package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	operatorsessions "github.com/notrodans/cresora/internal/application/operatorsessions"
)

const (
	operatorWebSessionIdleLifetime     = 12 * time.Hour
	operatorWebSessionAbsoluteLifetime = 7 * 24 * time.Hour
	operatorWebSessionSeenThrottle     = 5 * time.Minute
	operatorWebSessionLimit            = 5
)

type operatorWebSessionDatabase interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

var _ operatorsessions.SessionRepository = (*OperatorWebSessionStore)(nil)

// OperatorWebSessionStore is the PostgreSQL implementation of opaque browser
// sessions. Every security boundary in this adapter uses PostgreSQL's clock;
// caller timestamps are not accepted.
type OperatorWebSessionStore struct {
	database operatorWebSessionDatabase
}

func NewOperatorWebSessionStore(database *pgxpool.Pool) *OperatorWebSessionStore {
	return &OperatorWebSessionStore{database: database}
}

// CreateSession first locks the operator row and compares the exact username
// and password hash that Login verified. A reset that commits before this
// statement obtains the lock therefore cannot issue a session. Expired/revoked
// rows are removed, then the oldest live rows beyond the five-session limit
// are revoked before the new row is inserted.
func (store *OperatorWebSessionStore) CreateSession(
	context context.Context,
	operatorID uuid.UUID,
	verifiedUsername string,
	verifiedPasswordHash string,
	tokenHash []byte,
) (operatorsessions.StoredSession, error) {
	if operatorID == uuid.Nil || verifiedUsername == "" || verifiedPasswordHash == "" || len(tokenHash) != sha256Size {
		return operatorsessions.StoredSession{}, errors.New("create operator web session: invalid input")
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return operatorsessions.StoredSession{}, errors.New("begin operator web session transaction")
	}
	defer func() { _ = transaction.Rollback(context) }()

	var operatorExists bool
	failure = transaction.QueryRow(
		context,
		`SELECT TRUE
		 FROM operators
		 WHERE id = $1
		   AND username = $2
		   AND password_hash = $3
		   AND enabled = TRUE
		   AND password_hash IS NOT NULL
		   AND password_changed_at IS NOT NULL
		 FOR UPDATE`,
		operatorID,
		verifiedUsername,
		verifiedPasswordHash,
	).Scan(&operatorExists)
	if failure != nil || !operatorExists {
		return operatorsessions.StoredSession{}, errors.New("operator is not enabled or provisioned")
	}

	if _, failure = transaction.Exec(
		context,
		`DELETE FROM operator_web_sessions
		 WHERE revoked_at IS NOT NULL
		    OR idle_expires_at <= clock_timestamp()
		    OR absolute_expires_at <= clock_timestamp()`,
	); failure != nil {
		return operatorsessions.StoredSession{}, errors.New("prune operator web sessions")
	}
	if _, failure = transaction.Exec(
		context,
		`WITH ranked AS (
			SELECT id,
			       row_number() OVER (ORDER BY last_seen_at DESC, created_at DESC, id DESC) AS position
			FROM operator_web_sessions
			WHERE operator_id = $1
			  AND revoked_at IS NULL
			  AND idle_expires_at > clock_timestamp()
			  AND absolute_expires_at > clock_timestamp()
		)
		UPDATE operator_web_sessions sessions
		SET revoked_at = clock_timestamp()
		FROM ranked
		WHERE sessions.id = ranked.id
		  AND ranked.position > $2`,
		operatorID,
		operatorWebSessionLimit-1,
	); failure != nil {
		return operatorsessions.StoredSession{}, errors.New("enforce operator web session limit")
	}

	var session operatorsessions.StoredSession
	failure = transaction.QueryRow(
		context,
		`INSERT INTO operator_web_sessions (
			operator_id,
			token_hash,
			created_at,
			last_seen_at,
			idle_expires_at,
			absolute_expires_at
		)
		VALUES ($1, $2, clock_timestamp(), clock_timestamp(),
		        clock_timestamp() + interval '12 hours',
		        clock_timestamp() + interval '7 days')
		RETURNING id, operator_id, created_at, last_seen_at,
		          idle_expires_at, absolute_expires_at`,
		operatorID,
		tokenHash,
	).Scan(
		&session.ID,
		&session.OperatorID,
		&session.CreatedAt,
		&session.LastSeenAt,
		&session.IdleExpiresAt,
		&session.AbsoluteExpiresAt,
	)
	if failure != nil {
		return operatorsessions.StoredSession{}, fmt.Errorf("insert operator web session: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return operatorsessions.StoredSession{}, errors.New("commit operator web session")
	}
	return session, nil
}

// FindValidSession checks the token hash, revocation, both expiry bounds,
// credential provisioning, enabled state, and the password-reset boundary in
// one database-time query. last_seen is advanced at most once per five
// minutes, also using PostgreSQL time.
func (store *OperatorWebSessionStore) FindValidSession(
	context context.Context,
	tokenHash []byte,
) (operatorsessions.StoredSession, error) {
	if len(tokenHash) != sha256Size {
		return operatorsessions.StoredSession{}, errors.New("find operator web session: invalid input")
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return operatorsessions.StoredSession{}, errors.New("begin operator web session validation")
	}
	defer func() { _ = transaction.Rollback(context) }()

	var session operatorsessions.StoredSession
	failure = transaction.QueryRow(
		context,
		`SELECT sessions.id,
		        sessions.operator_id,
		        sessions.created_at,
		        sessions.last_seen_at,
		        sessions.idle_expires_at,
		        sessions.absolute_expires_at
		 FROM operator_web_sessions sessions
		 JOIN operators operators ON operators.id = sessions.operator_id
		 WHERE sessions.token_hash = $1
		   AND sessions.revoked_at IS NULL
		   AND sessions.idle_expires_at > clock_timestamp()
		   AND sessions.absolute_expires_at > clock_timestamp()
		   AND sessions.created_at > operators.tokens_invalid_before
		   AND operators.enabled = TRUE
		   AND operators.password_hash IS NOT NULL
		   AND operators.password_changed_at IS NOT NULL
		 FOR UPDATE OF sessions, operators`,
		tokenHash,
	).Scan(
		&session.ID,
		&session.OperatorID,
		&session.CreatedAt,
		&session.LastSeenAt,
		&session.IdleExpiresAt,
		&session.AbsoluteExpiresAt,
	)
	if failure != nil {
		return operatorsessions.StoredSession{}, errors.New("operator web session is not valid")
	}
	if _, failure = transaction.Exec(
		context,
		`UPDATE operator_web_sessions
		 SET last_seen_at = clock_timestamp(),
		     idle_expires_at = LEAST(clock_timestamp() + interval '12 hours', absolute_expires_at)
		 WHERE id = $1
		   AND revoked_at IS NULL
		   AND clock_timestamp() - last_seen_at >= interval '5 minutes'`,
		session.ID,
	); failure != nil {
		return operatorsessions.StoredSession{}, errors.New("refresh operator web session")
	}
	if failure = transaction.Commit(context); failure != nil {
		return operatorsessions.StoredSession{}, errors.New("commit operator web session validation")
	}
	return session, nil
}

func (store *OperatorWebSessionStore) RevokeSession(context context.Context, tokenHash []byte) error {
	if len(tokenHash) != sha256Size {
		return nil
	}
	_, failure := store.database.Exec(
		context,
		`UPDATE operator_web_sessions
		 SET revoked_at = clock_timestamp()
		 WHERE token_hash = $1 AND revoked_at IS NULL`,
		tokenHash,
	)
	if failure != nil {
		return errors.New("revoke operator web session")
	}
	return nil
}

func (store *OperatorWebSessionStore) RevokeOperatorSessions(context context.Context, operatorID uuid.UUID) error {
	if operatorID == uuid.Nil {
		return nil
	}
	_, failure := store.database.Exec(
		context,
		`UPDATE operator_web_sessions
		 SET revoked_at = clock_timestamp()
		 WHERE operator_id = $1 AND revoked_at IS NULL`,
		operatorID,
	)
	if failure != nil {
		return errors.New("revoke operator web sessions")
	}
	return nil
}

const sha256Size = 32
