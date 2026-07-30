package actor

import (
	"context"
	"time"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
)

type Task interface {
	Route() delivery.Route
	Execute(context.Context, delivery.Command) error
	Release(context.Context, error) error
}

// Receives tasks owned by one execution route
type Process interface {
	Submit(context.Context, Task) error
	Run(context.Context) error
}

// Reports one completed task to its actor
type result struct {
	failure error
}

// Owns one route mailbox and concurrency state
type process struct {
	mailbox  chan Task
	complete chan result
	command  delivery.Command
	limit    int
}

func NewProcess(
	command delivery.Command,
	limit int,
	capacity int,
) Process {
	return &process{
		mailbox:  make(chan Task, capacity),
		complete: make(chan result, limit),
		command:  command,
		limit:    limit,
	}
}

func (process *process) Submit(
	context context.Context,
	task Task,
) error {
	if task == nil {
		panic("submit nil task to delivery actor")
	}
	select {
	case <-context.Done():
		return context.Err()
	case process.mailbox <- task:
		return nil
	}
}

func (process *process) Run(context context.Context) error {
	if context == nil {
		panic("run delivery actor without context")
	}
	if process.command == nil {
		panic("run delivery actor without command")
	}
	if process.limit < 1 {
		panic("run delivery actor with invalid limit")
	}
	pending := make([]Task, 0, cap(process.mailbox))
	active := 0
	for {
		for active < process.limit && len(pending) > 0 {
			task := pending[0]
			pending[0] = nil
			pending = pending[1:]
			active++
			go process.execute(context, task)
		}
		select {
		case <-context.Done():
			return context.Err()
		case task := <-process.mailbox:
			pending = append(pending, task)
		case <-process.complete:
			active--
		}
	}
}

func (process *process) execute(
	ctx context.Context,
	task Task,
) {
	failure := task.Execute(ctx, process.command)
	if failure != nil {
		release, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = task.Release(release, failure)
		cancel()
	}
	select {
	case process.complete <- result{failure: failure}:
	case <-ctx.Done():
	}
}
