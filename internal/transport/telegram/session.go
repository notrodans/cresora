package telegram

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// SessionScope identifies an owned Telegram account without coupling the
// application boundary to gotd session types.
type SessionScope struct {
	OperatorID uuid.UUID
	AccountID  uuid.UUID
}

// Session is the result of loading an owned account. Bytes is opaque and is
// meaningful only when Present is true; Present is false when the account is
// owned by the operator but has not authenticated yet.
type Session struct {
	Bytes   []byte
	Present bool
}

// SessionStore persists opaque Telegram session bytes for an owned account.
// Implementations must not expose or infer storage details to callers.
type SessionStore interface {
	Load(context.Context, SessionScope) (Session, error)
	Store(context.Context, SessionScope, []byte) error
}

var (
	// ErrSessionUnauthorized is returned when the requested account is not
	// owned by the operator. It intentionally does not distinguish unknown
	// accounts from accounts owned by another operator.
	ErrSessionUnauthorized = errors.New("telegram session access denied")
	// ErrSessionInvalid indicates an envelope that cannot be accepted by the
	// current session store configuration.
	ErrSessionInvalid = errors.New("telegram session invalid")
	// ErrSessionCorrupt indicates an authenticated envelope that failed
	// integrity verification.
	ErrSessionCorrupt = errors.New("telegram session corrupt")
	// ErrSessionTooLarge indicates that opaque session bytes exceed the fixed
	// persistence limit.
	ErrSessionTooLarge = errors.New("telegram session too large")
)
