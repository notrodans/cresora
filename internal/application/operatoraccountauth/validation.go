package operatoraccountauth

import (
	"context"
	"strings"
	"unicode"

	"github.com/google/uuid"
	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func validateServiceContext(ctx context.Context, actor applicationroot.Actor) error {
	if ctx == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if actor.OperatorID == uuid.Nil {
		return ErrInvalidInput
	}
	return nil
}

func validateBeginResult(begin BeginResult) error {
	if err := begin.Validate(); err != nil {
		return err
	}
	if begin.Account.ID == uuid.Nil || begin.Account.Version == 0 {
		return ErrInvalidInput
	}
	switch begin.Outcome {
	case BeginStarted, BeginResumed, BeginInProgress, BeginAlreadyActive:
		return nil
	default:
		return ErrInvalidInput
	}
}

func validateTarget(target AuthTarget) error {
	if target.Actor.OperatorID == uuid.Nil || target.AccountID.IsZero() || target.Version == 0 ||
		(target.Status != operatoraccount.StatusAuthenticating && target.Status != operatoraccount.StatusDisconnecting) {
		return ErrInvalidInput
	}
	return nil
}

func normalizePhone(phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", ErrInvalidInput
	}
	var normalized strings.Builder
	for index, character := range phone {
		switch {
		case character == '+' && index == 0:
			normalized.WriteRune(character)
		case character >= '0' && character <= '9':
			normalized.WriteRune(character)
		case unicode.IsSpace(character) || character == '-' || character == '(' || character == ')' || character == '.':
		default:
			return "", ErrInvalidInput
		}
	}
	value := normalized.String()
	if after, ok := strings.CutPrefix(value, "00"); ok {
		value = "+" + after
	}
	if !strings.HasPrefix(value, "+") {
		value = "+" + value
	}
	digits := strings.TrimPrefix(value, "+")
	if len(digits) < 7 || len(digits) > 15 || digits == "" || digits[0] == '0' {
		return "", ErrInvalidInput
	}
	return value, nil
}
