package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	application "github.com/notrodans/nebula-go/internal/application/commands/delivery"
)

// Accepts claimed tasks for actor routing
type Exchange interface {
	Submit(context.Context, application.Task) error
}

// Moves persistent tasks into account actors
type Pump struct {
	claims   application.Claims
	exchange Exchange
	width    int
	pause    time.Duration
}

func New(
	claims application.Claims,
	exchange Exchange,
	width int,
	pause time.Duration,
) Pump {
	return Pump{
		claims:   claims,
		exchange: exchange,
		width:    width,
		pause:    pause,
	}
}

func (pump Pump) Run(parent context.Context) error {
	if parent == nil {
		panic("run delivery pump without context")
	}
	if pump.claims == nil {
		panic("run delivery pump without claims")
	}
	if pump.exchange == nil {
		panic("run delivery pump without exchange")
	}
	if pump.width < 1 {
		panic("run delivery pump with invalid width")
	}
	if pump.pause <= 0 {
		panic("run delivery pump with invalid pause")
	}
	context, cancel := context.WithCancel(parent)
	defer cancel()
	failures := make(chan error, pump.width)
	var group sync.WaitGroup
	group.Add(pump.width)
	for range pump.width {
		go func() {
			defer group.Done()
			if failure := pump.loop(context); failure != nil && !errors.Is(failure, context.Err()) {
				failures <- failure
			}
		}()
	}
	complete := make(chan struct{})
	go func() {
		group.Wait()
		close(complete)
	}()
	select {
	case <-context.Done():
		<-complete
		return context.Err()
	case failure := <-failures:
		cancel()
		<-complete
		return failure
	case <-complete:
		return nil
	}
}

func (pump Pump) loop(context context.Context) error {
	for {
		task, failure := pump.claims.Claim(context)
		if errors.Is(failure, application.ErrEmpty) {
			timer := time.NewTimer(pump.pause)
			select {
			case <-context.Done():
				timer.Stop()
				return context.Err()
			case <-timer.C:
				continue
			}
		}
		if failure != nil {
			return fmt.Errorf("claim background mailing delivery: %w", failure)
		}
		if failure = pump.exchange.Submit(context, task); failure != nil {
			release := task.Release(context, failure)
			if release != nil {
				return fmt.Errorf("submit mailing delivery after release failure %v: %w", failure, release)
			}
			return fmt.Errorf("submit mailing delivery to actor: %w", failure)
		}
	}
}
