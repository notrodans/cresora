// Package dialogsync runs durable account dialog synchronizations with
// bounded concurrency. It is transport neutral and owns no gotd state.
package dialogsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/notrodans/cresora/internal/application/dialogsync"
)

const (
	// executionCapacity bounds how many dialog fetches run concurrently. It is
	// independent of the number of persisted accounts.
	executionCapacity = 2

	defaultEmptyPoll         = 250 * time.Millisecond
	defaultExecutionTimeout  = 90 * time.Second
	defaultDrainTimeout      = 8 * time.Second
	defaultCleanupTimeout    = 2 * time.Second
	defaultLeaseDuration     = 120 * time.Second
	defaultLeaseSafetyMargin = 5 * time.Second
	defaultRetryBackoff      = 30 * time.Second
	// defaultFloodFloor is the minimum quiet period applied after a flood wait.
	// It must exceed the tight server flood window so a throttled account is not
	// re-probed exactly when the window closes, which keeps the rate limit warm.
	defaultFloodFloor = 90 * time.Second
	defaultMaxBackoff = 10 * time.Minute
)

var (
	// ErrInvalidConfig identifies an unusable dialog sync worker configuration.
	ErrInvalidConfig = errors.New("invalid account dialog sync worker configuration")
	// ErrShutdownTimeout indicates that work did not stop within the worker's
	// bounded shutdown budgets.
	ErrShutdownTimeout = errors.New("account dialog sync worker shutdown timed out")
)

// Store leases account dialog sync tasks.
type Store interface {
	Claim(context.Context, time.Duration) (dialogsync.Task, error)
}

// Executor runs one claimed account dialog synchronization.
type Executor interface {
	Sync(context.Context, dialogsync.Task) error
}

// Config contains the bounded worker timings.
type Config struct {
	EmptyPoll        time.Duration
	ExecutionTimeout time.Duration
	DrainTimeout     time.Duration
	CleanupTimeout   time.Duration
	LeaseDuration    time.Duration
	RetryBackoff     time.Duration
	// FloodFloor is a quiet period applied after a flood wait, at least as long
	// as the tight server flood window so a throttled account is not re-probed
	// exactly when the window closes.
	FloodFloor time.Duration
	// MaxBackoff bounds the retry delay applied to transient and flood waits so
	// a persistently restricted account is not polled faster than Telegram will
	// allow, which would otherwise keep the rate limit alive.
	MaxBackoff time.Duration
}

// Defaults returns the approved bounded-worker configuration.
func Defaults() Config {
	return Config{
		EmptyPoll:        defaultEmptyPoll,
		ExecutionTimeout: defaultExecutionTimeout,
		DrainTimeout:     defaultDrainTimeout,
		CleanupTimeout:   defaultCleanupTimeout,
		LeaseDuration:    defaultLeaseDuration,
		RetryBackoff:     defaultRetryBackoff,
		FloodFloor:       defaultFloodFloor,
		MaxBackoff:       defaultMaxBackoff,
	}
}

func (config Config) validate() error {
	for name, duration := range map[string]time.Duration{
		"empty poll":        config.EmptyPoll,
		"execution timeout": config.ExecutionTimeout,
		"drain timeout":     config.DrainTimeout,
		"cleanup timeout":   config.CleanupTimeout,
		"lease duration":    config.LeaseDuration,
		"retry backoff":     config.RetryBackoff,
		"flood floor":       config.FloodFloor,
	} {
		if duration <= 0 {
			return fmt.Errorf("%w: %s must be positive", ErrInvalidConfig, name)
		}
	}
	if config.LeaseDuration <= config.ExecutionTimeout+config.LeaseSafetyMargin() {
		return fmt.Errorf("%w: lease duration must exceed execution timeout plus safety margin", ErrInvalidConfig)
	}
	return nil
}

func (config Config) LeaseSafetyMargin() time.Duration {
	return defaultLeaseSafetyMargin
}

// Worker claims and executes account dialog syncs with bounded concurrency.
type Worker struct {
	store  Store
	syncer Executor
	config Config
	logger *slog.Logger
}

// New creates an unwired dialog sync worker.
func New(store Store, syncer Executor, config Config) *Worker {
	return &Worker{
		store:  store,
		syncer: syncer,
		config: config,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// WithLogger attaches a structured logger to the worker for operational
// tracing of claims, completions, and classified failures.
func (worker *Worker) WithLogger(logger *slog.Logger) *Worker {
	if logger != nil {
		worker.logger = logger
	}
	return worker
}

// Run executes until the parent is stopped or a fatal worker failure occurs.
// Claimed work uses a root context independent of the parent so shutdown can
// first drain active syncs before forcing cancellation.
func (worker *Worker) Run(parent context.Context) error {
	if err := worker.config.validate(); err != nil {
		return err
	}
	if err := parent.Err(); err != nil {
		return err
	}

	claimContext, cancelClaims := context.WithCancelCause(parent)
	defer cancelClaims(nil)

	executionBase := context.WithoutCancel(parent)
	executionRoot, cancelExecution := context.WithCancel(executionBase)
	defer cancelExecution()

	cleanup := &cleanupState{}
	failures := newFailureState()
	stopClaims := func(cause error) {
		if cause == nil {
			cause = context.Canceled
		}
		cancelClaims(cause)
	}
	reportFatal := func(failure error) {
		if failure == nil {
			return
		}
		failures.fatal(failure)
		stopClaims(failure)
	}

	active := newActiveTracker()
	coordinatorDone := make(chan struct{})
	go func() {
		worker.coordinate(
			claimContext,
			executionRoot,
			active,
			cleanup,
			reportFatal,
			coordinatorDone,
		)
	}()

	var stopCause error
	select {
	case <-parent.Done():
		stopCause = parent.Err()
		stopClaims(stopCause)
	case <-failures.fatalSignal:
		stopCause = failures.firstFatal()
		stopClaims(stopCause)
	case <-coordinatorDone:
		stopCause = failures.firstFatal()
		if stopCause == nil {
			stopCause = parent.Err()
		}
		if stopCause == nil {
			stopCause = errors.New("dialog sync worker coordinator stopped unexpectedly")
			failures.fatal(stopCause)
		}
		stopClaims(stopCause)
	}
	active.stop()

	shutdownStarted := time.Now()
	drainDeadline := shutdownStarted.Add(worker.config.DrainTimeout)
	shutdownDeadline := shutdownStarted.Add(worker.config.DrainTimeout + worker.config.CleanupTimeout)

	waitForChannelsUntil(drainDeadline, active.done())
	cancelExecution()

	cleanupDeadline := time.Now().Add(worker.config.CleanupTimeout)
	if cleanupDeadline.After(shutdownDeadline) {
		cleanupDeadline = shutdownDeadline
	}
	_, cancelCleanup := cleanup.beginUntil(cleanupDeadline)
	finished := waitForChannelsUntil(cleanupDeadline, active.done(), coordinatorDone)
	cancelCleanup()

	failure := failures.joined()
	if failures.firstFatal() == nil {
		failure = errors.Join(stopCause, failure)
	}
	if !finished {
		failure = errors.Join(failure, ErrShutdownTimeout)
	}
	return failure
}

func (worker *Worker) coordinate(
	claimContext context.Context,
	executionRoot context.Context,
	active *activeTracker,
	cleanup *cleanupState,
	reportFatal func(error),
	done chan<- struct{},
) {
	defer close(done)
	defer func() {
		if recovered := recover(); recovered != nil {
			reportFatal(panicFailure("dialog sync worker coordinator", recovered))
		}
	}()

	slots := make(chan struct{}, executionCapacity)
	for {
		select {
		case slots <- struct{}{}:
		case <-claimContext.Done():
			return
		}

		task, claimFailure := worker.store.Claim(claimContext, worker.config.LeaseDuration)
		if errors.Is(claimFailure, dialogsync.ErrEmpty) {
			<-slots
			if !waitForPoll(claimContext, worker.config.EmptyPoll) {
				return
			}
			continue
		}
		if claimFailure != nil {
			<-slots
			if claimContext.Err() != nil {
				return
			}
			reportFatal(fmt.Errorf("claim account dialog sync: %w", claimFailure))
			return
		}
		if task == nil {
			<-slots
			reportFatal(errors.New("claim account dialog sync: nil task without error"))
			return
		}
		if claimContext.Err() != nil {
			releaseQuiet(cleanup, worker.config.CleanupTimeout, task, claimContext.Err())
			<-slots
			return
		}
		if !active.add() {
			releaseQuiet(cleanup, worker.config.CleanupTimeout, task, errors.New("dialog sync worker is stopping"))
			<-slots
			return
		}
		go worker.execute(
			executionRoot,
			task,
			active,
			slots,
			cleanup,
			reportFatal,
		)
	}
}

func (worker *Worker) execute(
	executionRoot context.Context,
	task dialogsync.Task,
	active *activeTracker,
	slots chan struct{},
	cleanup *cleanupState,
	reportFatal func(error),
) {
	defer active.doneOne()
	defer func() { <-slots }()

	taskContext, cancel := context.WithTimeout(executionRoot, worker.config.ExecutionTimeout)
	defer cancel()

	renewDone := make(chan struct{})
	defer close(renewDone)
	go renewLoop(taskContext, renewDone, task, worker.config.LeaseDuration)

	var runFailure error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				runFailure = panicFailure("account dialog sync execution", recovered)
			}
		}()
		runFailure = worker.syncer.Sync(taskContext, task)
	}()

	switch {
	case runFailure == nil:
		worker.logger.Log(context.Background(), slog.LevelInfo,
			"account dialog sync completed",
			"account_id", task.Key().AccountID,
		)
		return
	case errors.Is(runFailure, dialogsync.ErrLeaseLost):
		return
	case errors.Is(runFailure, context.Canceled), errors.Is(runFailure, context.DeadlineExceeded):
		releaseQuiet(cleanup, worker.config.CleanupTimeout, task, runFailure)
		return
	}

	var floodWait *dialogsync.FloodWaitError
	switch {
	case errors.As(runFailure, &floodWait):
		delay := worker.floodBackoff(floodWait.RetryAfter())
		worker.logger.Log(context.Background(), slog.LevelWarn,
			"account dialog sync flood wait",
			"account_id", task.Key().AccountID,
			"retry_after", floodWait.RetryAfter().Seconds(),
			"next_retry", delay.Seconds(),
		)
		if failure := task.Retry(context.Background(), runFailure, delay); failure != nil && !errors.Is(failure, dialogsync.ErrLeaseLost) {
			reportFatal(fmt.Errorf("retry account dialog sync after flood wait: %w", failure))
		}
	case errors.Is(runFailure, dialogsync.ErrPermanent):
		worker.logger.Log(context.Background(), slog.LevelError,
			"account dialog sync permanently failed",
			"account_id", task.Key().AccountID,
		)
		if failure := task.Fail(context.Background(), runFailure); failure != nil && !errors.Is(failure, dialogsync.ErrLeaseLost) {
			reportFatal(fmt.Errorf("fail account dialog sync: %w", failure))
		}
	default:
		delay := worker.backoff(worker.config.RetryBackoff)
		worker.logger.Log(context.Background(), slog.LevelWarn,
			"account dialog sync transiently failed",
			"account_id", task.Key().AccountID,
			"next_retry", delay.Seconds(),
		)
		if failure := task.Retry(context.Background(), runFailure, delay); failure != nil && !errors.Is(failure, dialogsync.ErrLeaseLost) {
			reportFatal(fmt.Errorf("retry account dialog sync: %w", failure))
		}
	}
}

// floodBackoff is the retry delay after a flood wait. It never returns below
// FloodFloor (which exceeds the tight server flood window) and adds jitter so a
// throttled account is not re-probed the moment its window closes, which would
// otherwise keep the rate limit warm indefinitely.
func (worker *Worker) floodBackoff(server time.Duration) time.Duration {
	base := server
	if floor := worker.config.FloodFloor; floor > 0 && base < floor {
		base = floor
	}
	return cappedBackoff(base, worker.config.MaxBackoff)
}

// backoff computes the retry delay for one failed attempt. It never retries
// below the configured floor and adds jitter so a restricted account does not
// re-issue a request exactly when Telegram expects it, which keeps a persistent
// rate limit alive.
func (worker *Worker) backoff(server time.Duration) time.Duration {
	base := max(server, worker.config.RetryBackoff)
	return cappedBackoff(base, worker.config.MaxBackoff)
}

func cappedBackoff(base time.Duration, max time.Duration) time.Duration {
	// Up to 25% jitter above the base.
	jitter := time.Duration(rand.Int63n(int64(base)/25 + 1))
	delay := base + jitter
	if max > 0 && delay > max {
		delay = max
	}
	return delay
}

// renewLoop keeps the claim lease alive while the dialog fetch is running.
func renewLoop(
	taskContext context.Context,
	done <-chan struct{},
	task dialogsync.Task,
	lease time.Duration,
) {
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	for {
		select {
		case <-taskContext.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			if failure := task.Renew(taskContext, lease); failure != nil {
				return
			}
		}
	}
}

func releaseQuiet(
	cleanup *cleanupState,
	timeout time.Duration,
	task dialogsync.Task,
	cause error,
) {
	context, cancel := cleanup.forRelease(timeout)
	defer cancel()
	_ = task.Release(context, cause)
}

func waitForPoll(context context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-context.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitForChannelsUntil(deadline time.Time, channels ...<-chan struct{}) bool {
	timer := time.NewTimer(time.Until(deadline))
	defer stopTimer(timer)
	if !time.Now().Before(deadline) {
		return false
	}
	for len(channels) > 0 {
		select {
		case <-timer.C:
			return false
		default:
		}
		remaining := channels[:0]
		for _, channel := range channels {
			select {
			case <-channel:
			default:
				remaining = append(remaining, channel)
			}
		}
		if len(remaining) == 0 {
			return time.Now().Before(deadline)
		}
		channels = remaining
		select {
		case <-timer.C:
			return false
		case <-channels[0]:
			channels = channels[1:]
		}
	}
	return time.Now().Before(deadline)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func panicFailure(stage string, recovered any) error {
	if cause, ok := recovered.(error); ok {
		return fmt.Errorf("%s panicked: %w", stage, cause)
	}
	return fmt.Errorf("%s panicked: %v", stage, recovered)
}

type cleanupState struct {
	mutex sync.RWMutex
	ctx   context.Context
}

func (state *cleanupState) beginUntil(deadline time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	state.mutex.Lock()
	state.ctx = ctx
	state.mutex.Unlock()
	return ctx, cancel
}

func (state *cleanupState) forRelease(timeout time.Duration) (context.Context, context.CancelFunc) {
	state.mutex.RLock()
	ctx := state.ctx
	state.mutex.RUnlock()
	if ctx != nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

type activeTracker struct {
	mutex   sync.Mutex
	count   int
	stopped bool
	doneCh  chan struct{}
}

func newActiveTracker() *activeTracker {
	done := make(chan struct{})
	close(done)
	return &activeTracker{doneCh: done}
}

func (tracker *activeTracker) add() bool {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	if tracker.stopped {
		return false
	}
	if tracker.count == 0 {
		tracker.doneCh = make(chan struct{})
	}
	tracker.count++
	return true
}

func (tracker *activeTracker) doneOne() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.count--
	if tracker.count == 0 {
		close(tracker.doneCh)
	}
}

func (tracker *activeTracker) stop() {
	tracker.mutex.Lock()
	tracker.stopped = true
	tracker.mutex.Unlock()
}

func (tracker *activeTracker) done() <-chan struct{} {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	return tracker.doneCh
}

type failureState struct {
	mutex           sync.Mutex
	failures        []error
	firstFatalError error
	fatalSignal     chan struct{}
	fatalOnce       sync.Once
}

func newFailureState() *failureState {
	return &failureState{fatalSignal: make(chan struct{})}
}

func (state *failureState) fatal(failure error) {
	if failure == nil {
		return
	}
	state.mutex.Lock()
	state.failures = append(state.failures, failure)
	if state.firstFatalError == nil {
		state.firstFatalError = failure
	}
	state.mutex.Unlock()
	state.fatalOnce.Do(func() { close(state.fatalSignal) })
}

func (state *failureState) firstFatal() error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return state.firstFatalError
}

func (state *failureState) joined() error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return errors.Join(state.failures...)
}
