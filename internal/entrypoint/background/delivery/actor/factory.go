package actor

import (
	"context"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
)

// Provides route-specific delivery commands
type Commands interface {
	Command(context.Context, delivery.Route) (delivery.Command, error)
}

// Creates account actor processes
type Factory interface {
	Process(context.Context, delivery.Route) (Process, error)
}

// Configures route-specific actor processes
type factory struct {
	commands Commands
	limit    int
	capacity int
}

func NewFactory(
	commands Commands,
	limit int,
	capacity int,
) Factory {
	return factory{
		commands: commands,
		limit:    limit,
		capacity: capacity,
	}
}

func (factory factory) Process(
	context context.Context,
	route delivery.Route,
) (Process, error) {
	command, failure := factory.commands.Command(context, route)
	if failure != nil {
		return nil, failure
	}
	return NewProcess(command, factory.limit, factory.capacity), nil
}
