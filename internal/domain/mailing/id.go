package mailing

import "github.com/google/uuid"

// ID identifies one mailing.
type ID struct {
	value uuid.UUID
}

// Identifies one mailing run
type RunID uuid.UUID

func Identity(value uuid.UUID) ID {
	if value == uuid.Nil {
		panic("create mailing identity from zero UUID")
	}
	return ID{value: value}
}

func Run(value uuid.UUID) RunID {
	return RunID(value)
}

func (identity ID) UUID() uuid.UUID {
	return identity.value
}

func (identity RunID) UUID() uuid.UUID {
	return uuid.UUID(identity)
}
