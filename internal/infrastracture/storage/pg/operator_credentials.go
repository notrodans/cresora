package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	operatorcredentials "github.com/notrodans/nebula-go/internal/application/operatorcredentials"
	operatorsessions "github.com/notrodans/nebula-go/internal/application/operatorsessions"
)

type operatorCredentialDatabase interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

var _ operatorcredentials.Repository = (*OperatorCredentialStore)(nil)

// OperatorCredentialStore is the PostgreSQL adapter for the local bootstrap
// use case. The single INSERT ... ON CONFLICT statement is atomic and updates
// token revocation state together with the new credential.
type OperatorCredentialStore struct {
	database operatorCredentialDatabase
}

// NewOperatorCredentialStore creates a PostgreSQL-backed bootstrap store.
func NewOperatorCredentialStore(database *pgxpool.Pool) *OperatorCredentialStore {
	return &OperatorCredentialStore{database: database}
}

// BootstrapOrReset creates an unprovisioned operator or replaces its
// password_hash. The plaintext password never reaches this adapter.
func (store *OperatorCredentialStore) BootstrapOrReset(
	context context.Context,
	username string,
	passwordHash string,
) (operatorcredentials.Operator, error) {
	if context == nil {
		return operatorcredentials.Operator{}, errors.New("bootstrap operator credential: context is required")
	}
	if store == nil || store.database == nil {
		return operatorcredentials.Operator{}, errors.New("bootstrap operator credential: database is required")
	}
	if username == "" || strings.TrimSpace(username) != username || passwordHash == "" {
		return operatorcredentials.Operator{}, errors.New("bootstrap operator credential: invalid update")
	}

	var operator operatorcredentials.Operator
	failure := store.database.QueryRow(
		context,
		`INSERT INTO operators (
			username,
			password_hash,
			password_changed_at,
			created_at,
			updated_at,
			tokens_invalid_before
		)
		VALUES ($1, $2, clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp())
		ON CONFLICT (username) DO UPDATE
		SET password_hash = EXCLUDED.password_hash,
		    password_changed_at = clock_timestamp(),
		    updated_at = clock_timestamp(),
		    tokens_invalid_before = GREATEST(operators.tokens_invalid_before, clock_timestamp())
		RETURNING id, username`,
		username,
		passwordHash,
	).Scan(&operator.ID, &operator.Username)
	if failure != nil {
		return operatorcredentials.Operator{}, fmt.Errorf("write operator credential: %w", failure)
	}
	return operator, nil
}

// FindCredential returns only the credential projection required for an
// Argon2 verification. A missing row is intentionally distinguishable only to
// the application service, which turns it into a generic login failure.
func (store *OperatorCredentialStore) FindCredential(
	context context.Context,
	username string,
) (operatorsessions.Credential, error) {
	if context == nil {
		return operatorsessions.Credential{}, errors.New("find operator credential: context is required")
	}
	if store == nil || store.database == nil {
		return operatorsessions.Credential{}, errors.New("find operator credential: database is required")
	}
	if username == "" || strings.TrimSpace(username) != username {
		return operatorsessions.Credential{}, operatorsessions.ErrCredentialNotFound
	}

	var (
		credential operatorsessions.Credential
		hash       pgtype.Text
	)
	failure := store.database.QueryRow(
		context,
		`SELECT id, username, password_hash, enabled
		 FROM operators
		 WHERE username = $1`,
		username,
	).Scan(&credential.OperatorID, &credential.Username, &hash, &credential.Enabled)
	if errors.Is(failure, pgx.ErrNoRows) {
		return operatorsessions.Credential{}, operatorsessions.ErrCredentialNotFound
	}
	if failure != nil {
		return operatorsessions.Credential{}, errors.New("read operator credential")
	}
	if hash.Valid {
		credential.PasswordHash = hash.String
	}
	return credential, nil
}
