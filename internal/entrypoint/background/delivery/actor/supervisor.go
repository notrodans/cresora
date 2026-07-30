package actor

import (
	"context"
	"fmt"
	"sync"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
)

// Routes tasks to supervised account actors
type Exchange interface {
	Submit(context.Context, delivery.Task) error
	Wait() error
}

// Protects the live actor registry
type registry struct {
	mutex     sync.Mutex
	processes map[delivery.Route]Process
}

// Owns lazy actor creation and process lifecycles
type lifecycle struct {
	group    sync.WaitGroup
	failures chan error
}

// Owns lazy actor creation and process lifecycles
type supervisor struct {
	context   context.Context
	factory   Factory
	registry  *registry
	lifecycle *lifecycle
}

func NewSupervisor(
	context context.Context,
	factory Factory,
) Exchange {
	return &supervisor{
		context: context,
		factory: factory,
		registry: &registry{
			processes: make(map[delivery.Route]Process),
		},
		lifecycle: &lifecycle{
			failures: make(chan error, 1),
		},
	}
}

func (supervisor *supervisor) Submit(
	context context.Context,
	task delivery.Task,
) error {
	if task == nil {
		panic("submit nil task to actor supervisor")
	}
	process, failure := supervisor.process(context, task.Route())
	if failure != nil {
		return fmt.Errorf("obtain delivery actor for route %s: %w", task.Route().UUID(), failure)
	}
	return process.Submit(context, task)
}

func (supervisor *supervisor) Wait() error {
	supervisor.lifecycle.group.Wait()
	select {
	case failure := <-supervisor.lifecycle.failures:
		return failure
	default:
		return nil
	}
}

func (supervisor *supervisor) process(
	context context.Context,
	route delivery.Route,
) (Process, error) {
	supervisor.registry.mutex.Lock()
	defer supervisor.registry.mutex.Unlock()
	if process, exists := supervisor.registry.processes[route]; exists {
		return process, nil
	}
	process, failure := supervisor.factory.Process(context, route)
	if failure != nil {
		return nil, failure
	}
	supervisor.registry.processes[route] = process
	supervisor.lifecycle.group.Add(1)
	go supervisor.serve(route, process)
	return process, nil
}

func (supervisor *supervisor) serve(
	route delivery.Route,
	process Process,
) {
	defer supervisor.lifecycle.group.Done()
	defer supervisor.remove(route, process)
	defer func() {
		if cause := recover(); cause != nil {
			select {
			case supervisor.lifecycle.failures <- fmt.Errorf(
				"delivery actor %s panicked: %v",
				route.UUID(),
				cause,
			):
			default:
			}
		}
	}()
	_ = process.Run(supervisor.context)
}

func (supervisor *supervisor) remove(
	route delivery.Route,
	process Process,
) {
	supervisor.registry.mutex.Lock()
	defer supervisor.registry.mutex.Unlock()
	current, exists := supervisor.registry.processes[route]
	if exists && current == process {
		delete(supervisor.registry.processes, route)
	}
}
