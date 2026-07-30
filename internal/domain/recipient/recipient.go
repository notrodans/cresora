package recipient

import (
	"fmt"
	"io"

	"github.com/google/uuid"
)

// Identifies one mailing recipient
type ID uuid.UUID

// Represents a recipient identity in the domain
type Recipient interface {
	Print(io.Writer) error
	UUID() uuid.UUID
}

// Stores one immutable recipient identity
type identity struct {
	value ID
}

func Identity(value uuid.UUID) Recipient {
	return identity{value: ID(value)}
}

func Identifier(value uuid.UUID) ID {
	return ID(value)
}

func (identity ID) UUID() uuid.UUID {
	return uuid.UUID(identity)
}

func (recipient identity) Print(destination io.Writer) error {
	if uuid.UUID(recipient.value) == uuid.Nil {
		panic("print recipient identity with zero value")
	}
	if _, failure := io.WriteString(destination, uuid.UUID(recipient.value).String()); failure != nil {
		return fmt.Errorf("print recipient identity %s: %w", uuid.UUID(recipient.value), failure)
	}
	return nil
}

func (recipient identity) UUID() uuid.UUID {
	return uuid.UUID(recipient.value)
}
