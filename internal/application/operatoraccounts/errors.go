package operatoraccounts

import "errors"

var (
	// ErrAccountNotFound covers both an unknown account and an account outside
	// the actor's ownership scope. Keeping these cases equivalent avoids an
	// ownership oracle at application boundaries.
	ErrAccountNotFound = errors.New("operator account not found")
	// ErrAccountVersionConflict indicates that another writer changed the
	// account lifecycle after it was read.
	ErrAccountVersionConflict = errors.New("operator account lifecycle version conflict")
	// ErrSessionNotFound indicates that the requested account has no persisted
	// Telegram session.
	ErrSessionNotFound = errors.New("operator account session not found")
)
