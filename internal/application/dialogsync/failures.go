package dialogsync

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// The dialog sync application owns a small transport-neutral failure taxonomy.
// Transport adapters wrap their native errors with exactly one of these
// values; no transport type crosses the Fetcher boundary.
var (
	// ErrPermanent means the dialog cannot be synchronized ever and must be
	// marked failed rather than retried. Examples: revoked session, invalid
	// Telegram app id, permanent phone rejection.
	ErrPermanent = errors.New("account dialog sync permanently failed")
	// ErrTransient means the dialog fetch should be retried with backoff.
	ErrTransient = errors.New("account dialog sync transiently failed")
	// ErrFloodWait means the server supplied a retry-after duration.
	ErrFloodWait = errors.New("account dialog sync flood wait")
)

// FloodWaitError carries the server-supplied retry-after duration. It is
// transport neutral so a Telegram adapter can translate its native rate-limit
// error into this value without importing gotd.
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

// RetryAfter reports the server-supplied pause.
func (failure *FloodWaitError) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.Duration
}

// WrapPermanent preserves the classification and the original cause for
// errors.Is/errors.As consumers.
func WrapPermanent(cause error) error {
	return wrapFailure(ErrPermanent, cause)
}

// WrapTransient preserves a retryable classification and the original cause.
func WrapTransient(cause error) error {
	return wrapFailure(ErrTransient, cause)
}

func wrapFailure(kind, cause error) error {
	if cause == nil {
		return kind
	}
	return fmt.Errorf("%w: %w", kind, cause)
}

const (
	// maxErrorMessageCodePoints is the database-facing diagnostic bound.
	maxErrorMessageCodePoints = 1024
	defaultErrorMessage       = "account dialog sync failed"
)

// BoundedErrorMessage turns an arbitrary error into a non-empty diagnostic
// safe for the last_error column. A recovering guard keeps a broken error
// implementation from preventing outcome finalization.
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
	runes := []rune(strings.TrimSpace(strings.ToValidUTF8(strings.ReplaceAll(message, "\x00", ""), "\uFFFD")))
	if len(runes) > maxErrorMessageCodePoints {
		runes = runes[:maxErrorMessageCodePoints]
	}
	message = strings.TrimSpace(string(runes))
	if message == "" {
		return defaultErrorMessage
	}
	return message
}
