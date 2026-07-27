package account

import (
	"context"
	"fmt"

	application "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

// Provides account-specific Telegram APIs
type APIs interface {
	API(context.Context, application.Route) (telegram.API, error)
}

// Provides account-specific target projections
type Targets interface {
	Targets(application.Route) telegram.Targets
}

// Builds one delivery command per Telegram account
type commands struct {
	apis       APIs
	targets    Targets
	deliveries application.Deliveries
}

func NewCommands(
	apis APIs,
	targets Targets,
	deliveries application.Deliveries,
) commands {
	return commands{
		apis:       apis,
		targets:    targets,
		deliveries: deliveries,
	}
}

func (commands commands) Command(
	context context.Context,
	route application.Route,
) (application.Command, error) {
	api, failure := commands.apis.API(context, route)
	if failure != nil {
		return nil, fmt.Errorf("open Telegram account %s API: %w", route.UUID(), failure)
	}
	port := telegram.New(api, commands.targets.Targets(route))
	return application.New(commands.deliveries, port), nil
}
