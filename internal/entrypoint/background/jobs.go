// Package background contains the small amount of lifecycle coordination
// shared by process background jobs.
package background

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidConfig identifies an unusable background runner configuration.
	ErrInvalidConfig = errors.New("invalid background runner configuration")
	// ErrJobCompleted indicates that a job returned nil without the runner being
	// stopped. A long-running job is expected to report why it stopped.
	ErrJobCompleted = errors.New("background job completed without error")
	// ErrShutdownTimeout indicates that one or more jobs did not join before the
	// bounded shutdown wait elapsed.
	ErrShutdownTimeout = errors.New("background job shutdown timed out")
)

// Job is one supervised process job.
type Job func(context.Context) error

// Runner starts a fixed set of jobs and owns their cancellation and joins.
// It intentionally does not restart jobs: a job failure is a process failure,
// not a reason to spin up an unbounded restart loop.
type Runner struct {
	jobs            []Job
	shutdownTimeout time.Duration
}

// NewRunner creates a runner with a bounded wait for jobs to stop after
// cancellation. The timeout is validated by Run so configuration errors are
// returned through the same lifecycle path as job errors.
func NewRunner(jobs []Job, shutdownTimeout time.Duration) Runner {
	return Runner{
		jobs:            jobs,
		shutdownTimeout: shutdownTimeout,
	}
}

type jobResult struct {
	index int
	fail  error
}

// Run supervises all jobs until the parent is canceled, a job fails, or a job
// completes unexpectedly. On cancellation it cancels every job and joins all
// cooperative jobs before returning. A job that ignores cancellation is
// bounded by shutdownTimeout and contributes ErrShutdownTimeout.
func (runner Runner) Run(parent context.Context) error {
	if len(runner.jobs) == 0 {
		return fmt.Errorf("%w: at least one job is required", ErrInvalidConfig)
	}
	if runner.shutdownTimeout <= 0 {
		return fmt.Errorf("%w: shutdown timeout must be positive", ErrInvalidConfig)
	}
	for index, job := range runner.jobs {
		if job == nil {
			return fmt.Errorf("%w: job %d is nil", ErrInvalidConfig, index)
		}
	}
	if parent.Err() != nil {
		return nil
	}

	jobContext, cancel := context.WithCancel(parent)
	defer cancel()
	results := make(chan jobResult, len(runner.jobs))
	for index, job := range runner.jobs {
		go func() {
			results <- jobResult{index: index, fail: runJob(job, jobContext)}
		}()
	}

	joined := 0
	var primaryFailure error
	var shutdownTimer *time.Timer
	var shutdown <-chan time.Time
	parentDone := parent.Done()
	stopping := false
	beginShutdown := func() {
		if stopping {
			return
		}
		stopping = true
		// A canceled context remains readable forever. Disable this arm once
		// shutdown starts so joining an uncooperative job waits on results or
		// the bounded shutdown timer instead of repeatedly selecting parent.Done.
		parentDone = nil
		cancel()
		shutdownTimer = time.NewTimer(runner.shutdownTimeout)
		shutdown = shutdownTimer.C
	}
	defer func() {
		if shutdownTimer != nil && !shutdownTimer.Stop() {
			select {
			case <-shutdownTimer.C:
			default:
			}
		}
	}()

	for joined < len(runner.jobs) {
		select {
		case result := <-results:
			joined++
			if !stopping && parent.Err() != nil {
				beginShutdown()
				if result.fail != nil && !isCancellation(result.fail) {
					primaryFailure = fmt.Errorf("background job %d: %w", result.index, result.fail)
				}
				continue
			}
			if result.fail == nil {
				if !stopping {
					primaryFailure = fmt.Errorf("background job %d: %w", result.index, ErrJobCompleted)
					beginShutdown()
				}
				continue
			}
			if stopping {
				if primaryFailure == nil && !isCancellation(result.fail) {
					primaryFailure = fmt.Errorf("background job %d: %w", result.index, result.fail)
				}
				continue
			}
			primaryFailure = fmt.Errorf("background job %d: %w", result.index, result.fail)
			beginShutdown()
		case <-parentDone:
			beginShutdown()
		case <-shutdown:
			return errors.Join(primaryFailure, ErrShutdownTimeout)
		}
	}
	if primaryFailure == nil && parent.Err() != nil {
		return parent.Err()
	}
	return primaryFailure
}

func runJob(job Job, context context.Context) (failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = fmt.Errorf("background job panicked: %v", recovered)
		}
	}()
	return job(context)
}

func isCancellation(failure error) bool {
	return errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded)
}
