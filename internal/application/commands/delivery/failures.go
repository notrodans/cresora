package delivery

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// The delivery application owns this small taxonomy.  Transport adapters may
// wrap their native errors with one of these values, but no transport error
// type is allowed to cross the Port boundary.
var (
	ErrPermanent      = errors.New("delivery permanently failed")
	ErrTransient      = errors.New("delivery transiently failed")
	ErrFloodWait      = errors.New("delivery flood wait")
	ErrUnknownOutcome = errors.New("delivery outcome is unknown")
)

// FloodWaitError carries the server supplied retry-after duration.  It is
// intentionally transport neutral: a Telegram adapter can translate its
// native rate-limit error into this value and the persistence consumer can
// classify it without importing gotd.
type FloodWaitError struct {
	Duration time.Duration
}

func (failure *FloodWaitError) Error() string {
	if failure == nil {
		return ErrFloodWait.Error()
	}
	return fmt.Sprintf("%s: retry after %s", ErrFloodWait, failure.Duration)
}

func (failure *FloodWaitError) Unwrap() error {
	return ErrFloodWait
}

// RetryAfter reports the duration supplied by the remote service.
func (failure *FloodWaitError) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.Duration
}

// WrapPermanent, WrapTransient, and WrapUnknown preserve both the application
// classification and the original cause for errors.Is/errors.As callers.
func WrapPermanent(cause error) error {
	return wrapFailure(ErrPermanent, cause)
}

func WrapTransient(cause error) error {
	return wrapFailure(ErrTransient, cause)
}

func WrapUnknown(cause error) error {
	return wrapFailure(ErrUnknownOutcome, cause)
}

func wrapFailure(kind, cause error) error {
	if cause == nil {
		return kind
	}
	return fmt.Errorf("%w: %w", kind, cause)
}

// FailureKind is the bounded set of negative outcome classes consumed by
// delivery persistence.  Zero is deliberately unknown, so an uninitialized
// classification cannot accidentally opt into retry.
type FailureKind uint8

const (
	FailureUnknown FailureKind = iota
	FailurePermanent
	FailureTransient
	FailureFloodWait
)

// Classification is the result of Classify. RetryAfter is meaningful only for
// FailureFloodWait and is retained separately so SQL can choose the retry
// delay while it decides whether the attempt limit has been reached.
type Classification struct {
	Kind       FailureKind
	RetryAfter time.Duration
}

// Classify consumes only the application taxonomy. It deliberately treats
// malformed typed errors, bare sentinels without their required payload, and
// every untyped error as unknown/no-auto-retry.
func Classify(failure error) Classification {
	if failure == nil {
		return Classification{}
	}

	var floodWait *FloodWaitError
	if errors.As(failure, &floodWait) {
		if floodWait != nil && floodWait.Duration > 0 {
			return Classification{
				Kind:       FailureFloodWait,
				RetryAfter: floodWait.Duration,
			}
		}
		return Classification{}
	}

	switch {
	case errors.Is(failure, ErrPermanent):
		return Classification{Kind: FailurePermanent}
	case errors.Is(failure, ErrTransient):
		return Classification{Kind: FailureTransient}
	case errors.Is(failure, ErrUnknownOutcome):
		return Classification{}
	default:
		return Classification{}
	}
}

const (
	// MaxErrorMessageCodePoints is the database-facing diagnostic bound. It is
	// measured in Unicode code points rather than bytes.
	MaxErrorMessageCodePoints = 1024
	defaultErrorMessage       = "delivery outcome has no diagnostic"
)

// BoundedErrorMessage turns an arbitrary error into a non-empty diagnostic
// safe for the delivery error_message column. A recovering guard keeps a
// broken third-party error from preventing outcome finalization.
func BoundedErrorMessage(failure error) string {
	message := ""
	if failure != nil {
		func() {
			defer func() {
				if recover() != nil {
					message = ""
				}
			}()
			message = failure.Error()
		}()
	}
	if message == "" {
		message = defaultErrorMessage
	}
	// PostgreSQL text values cannot contain NUL bytes. Error implementations
	// may also return malformed UTF-8 because Go strings are byte sequences, so
	// normalize both hazards before applying the database-facing bound.
	message = strings.ReplaceAll(message, "\x00", "")
	message = strings.ToValidUTF8(message, "\uFFFD")
	message = strings.TrimSpace(message)
	if message == "" {
		message = defaultErrorMessage
	}
	runes := []rune(message)
	if len(runes) > MaxErrorMessageCodePoints {
		runes = runes[:MaxErrorMessageCodePoints]
	}
	message = strings.TrimSpace(string(runes))
	if message == "" {
		return defaultErrorMessage
	}
	if !utf8.ValidString(message) {
		return defaultErrorMessage
	}
	return message
}
