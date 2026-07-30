// Package operatorcredentials contains the transport-neutral local operator
// bootstrap/reset use case and its narrow persistence port.
package operatorcredentials

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	ErrInvalidInput = errors.New("invalid operator credential input")
)

// Operator is the only identity returned by bootstrap/reset. It deliberately
// does not contain a password or its hash.
type Operator struct {
	ID       uuid.UUID
	Username string
}

// Repository is the minimal atomic storage port for local credential
// bootstrap. Implementations must create or update one operator in a single
// database statement/transaction and invalidate existing token state as part
// of that operation.
type Repository interface {
	BootstrapOrReset(context.Context, string, string) (Operator, error)
}

// Hasher is deliberately smaller than the password package so the use case is
// straightforward to test without running a memory-hard KDF in every test.
type Hasher interface {
	Hash(string) (string, error)
}

// Service provisions or resets an operator credential. Security timestamps are
// deliberately owned by the PostgreSQL adapter rather than this service.
type Service struct {
	repository Repository
	hasher     Hasher
}

// NewService constructs the local bootstrap/reset service.
func NewService(repository Repository, hasher Hasher) Service {
	return Service{repository: repository, hasher: hasher}
}

// BootstrapOrReset hashes password in memory and atomically creates or resets
// the named operator. No plaintext is passed to the repository.
func (service Service) BootstrapOrReset(context context.Context, username, plaintext string) (Operator, error) {
	if failure := validateUsername(username); failure != nil {
		return Operator{}, failure
	}
	hash, failure := service.hasher.Hash(plaintext)
	if failure != nil {
		return Operator{}, fmt.Errorf("hash operator credential: %w", failure)
	}
	operator, failure := service.repository.BootstrapOrReset(context, username, hash)
	if failure != nil {
		return Operator{}, fmt.Errorf("persist operator credential: %w", failure)
	}
	return operator, nil
}

// ValidateUsername validates the value before it is echoed in any terminal
// result. Operator names are intentionally restricted to printable ASCII:
// this excludes C0/C1 controls, ANSI and other terminal control bytes, format
// characters, bidi overrides, zero-width characters, and Unicode confusables.
func ValidateUsername(username string) error {
	return validateUsername(username)
}

func validateUsername(username string) error {
	if !utf8.ValidString(username) || username == "" || strings.TrimSpace(username) != username || len(username) > 255 {
		return fmt.Errorf("%w: username must be a non-empty printable ASCII value of at most 255 bytes", ErrInvalidInput)
	}
	for index := 0; index < len(username); index++ {
		if username[index] < 0x21 || username[index] > 0x7e {
			return fmt.Errorf("%w: username contains a control, whitespace, or non-ASCII character", ErrInvalidInput)
		}
	}
	return nil
}
