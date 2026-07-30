// Package deliveryreconciler runs transport-neutral terminal run
// reconciliation.
package deliveryreconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
)

const DefaultInterval = time.Minute

var ErrInvalidConfig = errors.New("invalid delivery reconciler loop configuration")

// Config controls only the polling cadence.  The PostgreSQL adapter owns the
// bounded candidate batch size.
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

// Loop performs one reconciliation pass immediately, then waits between
// later passes.  A failed pass is returned to the generic background
// supervisor; it is never retried in a tight loop.
type Loop struct {
	reconciler application.RunReconciler
	config     Config
}

func New(reconciler application.RunReconciler, config Config) *Loop {
	return &Loop{reconciler: reconciler, config: config}
}

func (loop *Loop) Run(context context.Context) error {
	if failure := loop.config.Validate(); failure != nil {
		return fmt.Errorf("validate delivery reconciler loop: %w", failure)
	}
	if failure := context.Err(); failure != nil {
		return failure
	}

	if _, failure := loop.reconciler.Reconcile(context); failure != nil {
		return fmt.Errorf("run delivery reconciler pass: %w", failure)
	}

	ticker := time.NewTicker(loop.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-context.Done():
			return context.Err()
		case <-ticker.C:
			if _, failure := loop.reconciler.Reconcile(context); failure != nil {
				return fmt.Errorf("run delivery reconciler pass: %w", failure)
			}
		}
	}
}
