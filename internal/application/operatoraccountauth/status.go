package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"

	applicationroot "github.com/notrodans/cresora/internal/application"
)

// Status merges durable actor-owned accounts with the process-local challenge.
func (service *Service) Status(ctx context.Context, actor applicationroot.Actor) (Status, error) {
	if err := validateServiceContext(ctx, actor); err != nil {
		return Status{}, err
	}
	accounts, err := service.lifecycle.List(ctx, actor)
	if err != nil {
		return Status{}, fmt.Errorf("list operator accounts: %w", err)
	}
	challenge, err := service.registry.Status(ctx, actor)
	if err != nil {
		if errors.Is(err, errRuntimeChallengeExpired) && challenge != nil {
			if _, expireErr := service.expireChallenge(ctx, *challenge); expireErr != ErrChallengeExpired {
				return Status{}, expireErr
			}
			accounts, err = service.lifecycle.List(ctx, actor)
			if err != nil {
				return Status{}, fmt.Errorf("list operator accounts: %w", err)
			}
			challenge = nil
		} else {
			return Status{}, mapChallengeError(err)
		}
	}
	return Status{Accounts: append([]Account(nil), accounts...), Challenge: challenge}, nil
}
