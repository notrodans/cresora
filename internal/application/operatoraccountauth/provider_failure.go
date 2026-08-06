package operatoraccountauth

import (
	"context"
	"errors"
	"time"
)

func isAbortProviderFailure(err error) bool {
	return errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrSessionUnavailable) || errors.Is(err, ErrProviderUnavailable)
}

func classifyProviderFailure(err error, expiresAt, now time.Time) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if retry, ok := errors.AsType[*RetryAfterError](err); ok {
		remaining := expiresAt.Sub(now)
		if remaining <= 0 {
			return ErrChallengeExpired
		}
		after := min(retry.RetryAfter(), remaining)
		bounded, boundedErr := NewRetryAfterError(after)
		if boundedErr == nil {
			return bounded
		}
		return ErrFloodWait
	}
	var providerFailure *ProviderFailureError
	if errors.As(err, &providerFailure) && providerFailure != nil && validProviderFailureKind(providerFailure.Kind()) {
		return providerFailure
	}
	switch {
	case errors.Is(err, ErrInvalidCode):
		return ErrInvalidCode
	case errors.Is(err, ErrInvalidPassword):
		return ErrInvalidPassword
	case errors.Is(err, ErrPasswordRequired):
		return ErrPasswordRequired
	case errors.Is(err, ErrUnauthorized):
		return ErrUnauthorized
	case errors.Is(err, ErrSessionUnavailable):
		return ErrSessionUnavailable
	case errors.Is(err, ErrProviderUnavailable):
		return ErrProviderUnavailable
	case errors.Is(err, ErrProviderTransient):
		return ErrProviderTransient
	case errors.Is(err, ErrFloodWait):
		return ErrFloodWait
	default:
		return ErrProviderTransient
	}
}
