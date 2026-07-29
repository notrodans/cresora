// Package deliveryreaper runs transport-neutral delivery lease recovery.
package deliveryreaper

import (
	"context"
	"errors"
	"fmt"
	"time"

	application "github.com/notrodans/nebula-go/internal/application/commands/delivery"
)

const DefaultInterval = time.Minute

var ErrInvalidConfig = errors.New("invalid delivery reaper loop configuration")

// Config controls only how often the transport-neutral reaper is polled. The
// PostgreSQL adapter owns batch size, lease grace, and retry policy defaults.
type Config struct {
	Interval time.Duration
}

func Defaults() Config {
	return Config{Interval: DefaultInterval}
}

func (config Config) Validate() error {
	if config.Interval <= 0 {
		return fmt.Errorf("%w: interval must be positive", ErrInvalidConfig)
	}
	return nil
}

// Loop performs one reaper pass immediately, then waits between later passes.
// Waiting is cancellation-aware, so a quiet loop cannot delay shutdown or spin
// while the database contains no expired work.
type Loop struct {
	reaper application.Reaper
	config Config
}

func New(reaper application.Reaper, config Config) *Loop {
	return &Loop{reaper: reaper, config: config}
}

// Run keeps reaping until its context is canceled or a pass fails. The first
// pass is deliberately immediate rather than delayed by one interval.
func (loop *Loop) Run(context context.Context) error {
	if context == nil {
		panic("run delivery reaper without context")
	}
	if loop == nil {
		return errors.New("run delivery reaper without loop")
	}
	if loop.reaper == nil {
		return errors.New("run delivery reaper without reaper")
	}
	if failure := loop.config.Validate(); failure != nil {
		return fmt.Errorf("validate delivery reaper loop: %w", failure)
	}
	if failure := context.Err(); failure != nil {
		return failure
	}

	if _, failure := loop.reaper.Reap(context); failure != nil {
		return fmt.Errorf("run delivery reaper pass: %w", failure)
	}

	ticker := time.NewTicker(loop.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-context.Done():
			return context.Err()
		case <-ticker.C:
			if _, failure := loop.reaper.Reap(context); failure != nil {
				return fmt.Errorf("run delivery reaper pass: %w", failure)
			}
		}
	}
}
