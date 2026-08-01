package operatoraccount

import "github.com/google/uuid"

// ID identifies one operator-owned Telegram account.
//
// The UUID is kept private so an account identity cannot be changed after it
// has been created.
type ID struct {
	value uuid.UUID
}

// Identity constructs an account identity. A zero UUID is not a valid domain
// identity and indicates a broken internal invariant.
func Identity(value uuid.UUID) ID {
	if value == uuid.Nil {
		panic("create operator account identity from zero UUID")
	}
	return ID{value: value}
}

// UUID returns the underlying UUID representation.
func (identity ID) UUID() uuid.UUID {
	return identity.value
}

// IsZero reports whether identity is the zero value rather than a valid
// account identity. It is useful when validating data loaded by an adapter.
func (identity ID) IsZero() bool {
	return identity.value == uuid.Nil
}
