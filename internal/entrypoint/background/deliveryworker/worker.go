// Package deliveryworker executes claimed delivery tasks with bounded
// concurrency. It deliberately has no transport or application wiring.
package deliveryworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	applicationdelivery "github.com/notrodans/cresora/internal/application/commands/delivery"
)

const (
	executionCapacity = 4

	defaultEmptyPoll         = 250 * time.Millisecond
	defaultExecutionTimeout  = 30 * time.Second
	defaultDrainTimeout      = 8 * time.Second
	defaultCleanupTimeout    = 2 * time.Second
	defaultLeaseSafetyMargin = 5 * time.Second
	defaultLeaseDuration     = 60 * time.Second
)

var (
	// ErrInvalidConfig identifies an invalid worker configuration.
	ErrInvalidConfig = errors.New("invalid delivery worker configuration")
	// ErrShutdownTimeout indicates that an execution or cleanup dependency did
	// not stop within the worker's bounded shutdown budgets.
	ErrShutdownTimeout = errors.New("delivery worker shutdown timed out")
)

// Commands resolves the command for one delivery route.
type Commands interface {
	Command(context.Context, applicationdelivery.Route) (applicationdelivery.Command, error)
}

// Config contains the bounded worker timings. Capacity is intentionally not a
// config option in this slice; the worker always has four execution slots.
type Config struct {
	EmptyPoll         time.Duration
	ExecutionTimeout  time.Duration
	DrainTimeout      time.Duration
	CleanupTimeout    time.Duration
	LeaseDuration     time.Duration
	LeaseSafetyMargin time.Duration
}

// Defaults returns the approved bounded-worker configuration.
func Defaults() Config {
	return Config{
		EmptyPoll:         defaultEmptyPoll,
		ExecutionTimeout:  defaultExecutionTimeout,
		DrainTimeout:      defaultDrainTimeout,
		CleanupTimeout:    defaultCleanupTimeout,
		LeaseDuration:     defaultLeaseDuration,
		LeaseSafetyMargin: defaultLeaseSafetyMargin,
	}
}

// LeaseSafetyDuration calculates the minimum lease bound for one execution
// deadline, outcome finalization, and its safety margin.
func (config Config) LeaseSafetyDuration() (time.Duration, error) {
	if config.ExecutionTimeout <= 0 {
		return 0, fmt.Errorf("%w: execution timeout must be positive", ErrInvalidConfig)
	}
	if config.LeaseSafetyMargin <= 0 {
		return 0, fmt.Errorf("%w: lease safety margin must be positive", ErrInvalidConfig)
	}
	if config.ExecutionTimeout > maxDuration-applicationdelivery.OutcomeFinalizationTimeout {
		return 0, fmt.Errorf("%w: execution timeout and outcome finalization timeout overflow", ErrInvalidConfig)
	}
	withoutMargin := config.ExecutionTimeout + applicationdelivery.OutcomeFinalizationTimeout
	if withoutMargin > maxDuration-config.LeaseSafetyMargin {
		return 0, fmt.Errorf("%w: execution timeout, outcome finalization timeout, and lease safety margin overflow", ErrInvalidConfig)
	}
	return withoutMargin + config.LeaseSafetyMargin, nil
}

// Validate checks all worker durations and the arithmetic used by its lease
// and shutdown safety bounds.
func (config Config) Validate() error {
	for name, duration := range map[string]time.Duration{
		"empty poll":          config.EmptyPoll,
		"execution timeout":   config.ExecutionTimeout,
		"drain timeout":       config.DrainTimeout,
		"cleanup timeout":     config.CleanupTimeout,
		"lease duration":      config.LeaseDuration,
		"lease safety margin": config.LeaseSafetyMargin,
	} {
		if duration <= 0 {
			return fmt.Errorf("%w: %s must be positive", ErrInvalidConfig, name)
		}
	}

	leaseSafety, err := config.LeaseSafetyDuration()
	if err != nil {
		return err
	}
	if config.LeaseDuration <= leaseSafety {
		return fmt.Errorf(
			"%w: lease duration %s must exceed execution safety bound %s",
			ErrInvalidConfig,
			config.LeaseDuration,
			leaseSafety,
		)
	}
	if config.DrainTimeout > maxDuration-config.CleanupTimeout {
		return fmt.Errorf("%w: drain and cleanup timeouts overflow", ErrInvalidConfig)
	}
	return nil
}

const maxDuration = time.Duration(1<<63 - 1)

// Worker owns one claim coordinator and four execution slots.
type Worker struct {
	claims   applicationdelivery.Claims
	commands Commands
	config   Config
}

// New creates an unwired transport-neutral delivery worker.
func New(
	claims applicationdelivery.Claims,
	commands Commands,
	config Config,
) *Worker {
	return &Worker{
		claims:   claims,
		commands: commands,
		config:   config,
	}
}

// Run executes until the parent is stopped or a fatal worker failure occurs.
// Claimed task execution intentionally uses a root context independent of the
// parent so shutdown can first drain active work before forcing cancellation.
func (worker *Worker) Run(parent context.Context) error {
	if parent == nil {
		panic("run delivery worker without context")
	}
	if worker == nil {
		return errors.New("run delivery worker without worker")
	}
	if err := worker.config.Validate(); err != nil {
		return fmt.Errorf("validate delivery worker config: %w", err)
	}
	if worker.claims == nil {
		return errors.New("run delivery worker without claims")
	}
	if worker.commands == nil {
		return errors.New("run delivery worker without commands")
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
			failures.record,
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
			stopCause = errors.New("delivery worker coordinator stopped unexpectedly")
			failures.fatal(stopCause)
		}
		stopClaims(stopCause)
	}
	active.stop()
	activeDone := active.done()

	// Shutdown has one absolute budget: a drain phase followed by a cleanup
	// phase. The cleanup deadline cannot extend beyond the combined budget, even
	// when a dependency ignores cancellation and finishes later.
	shutdownStarted := time.Now()
	drainDeadline := shutdownStarted.Add(worker.config.DrainTimeout)
	shutdownDeadline := shutdownStarted.Add(worker.config.DrainTimeout + worker.config.CleanupTimeout)

	// Let active executions finish naturally before canceling their independent
	// root. This is the only drain phase; no new claim can pass the canceled
	// claim context.
	waitForChannelsUntil(drainDeadline, activeDone)
	cancelExecution()

	// Release work that was claimed too late, and give forced executions one
	// global, parent-independent cleanup context.
	cleanupDeadline := time.Now().Add(worker.config.CleanupTimeout)
	if cleanupDeadline.After(shutdownDeadline) {
		cleanupDeadline = shutdownDeadline
	}
	_, cancelCleanup := cleanup.beginUntil(cleanupDeadline)
	cleanupFinished := waitForChannelsUntil(cleanupDeadline, activeDone, coordinatorDone)
	cancelCleanup()

	failure := failures.joined()
	if failures.firstFatal() == nil {
		failure = errors.Join(stopCause, failure)
	}
	if !cleanupFinished {
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
	recordFailure func(error),
	done chan<- struct{},
) {
	defer close(done)
	defer func() {
		if recovered := recover(); recovered != nil {
			reportFatal(panicFailure("delivery worker coordinator", recovered))
		}
	}()

	slots := make(chan struct{}, executionCapacity)
	for {
		select {
		case slots <- struct{}{}:
		case <-claimContext.Done():
			return
		}

		task, claimFailure := worker.claims.Claim(claimContext)
		if errors.Is(claimFailure, applicationdelivery.ErrEmpty) {
			<-slots
			if !waitForPoll(claimContext, worker.config.EmptyPoll) {
				return
			}
			continue
		}
		if claimFailure != nil {
			var releaseFailure error
			if task != nil {
				releaseFailure = worker.release(task, claimFailure, cleanup)
			}
			<-slots
			if claimContext.Err() != nil {
				if releaseFailure != nil {
					// A task returned after cancellation still needs a best-effort
					// release, but cancellation itself is not a new fatal error.
					recordFailure(releaseFailure)
				}
				return
			}
			reportFatal(errors.Join(
				fmt.Errorf("claim delivery task: %w", claimFailure),
				releaseFailure,
			))
			return
		}
		if task == nil {
			<-slots
			reportFatal(errors.New("claim delivery task: nil task without error"))
			return
		}
		if claimContext.Err() != nil {
			worker.releaseLate(task, claimContext, cleanup, reportFatal, recordFailure)
			<-slots
			return
		}

		route, routeFailure := taskRoute(task)
		if routeFailure != nil {
			releaseFailure := worker.release(task, routeFailure, cleanup)
			<-slots
			reportFatal(errors.Join(routeFailure, releaseFailure))
			return
		}
		command, resolveFailure := resolveCommand(worker.commands, claimContext, route)
		if resolveFailure != nil {
			releaseFailure := worker.release(task, resolveFailure, cleanup)
			<-slots
			if claimContext.Err() != nil {
				if releaseFailure != nil {
					recordFailure(releaseFailure)
				}
				return
			}
			reportFatal(errors.Join(
				fmt.Errorf("resolve delivery command: %w", resolveFailure),
				releaseFailure,
			))
			return
		}
		if command == nil {
			failure := errors.New("resolve delivery command: nil command without error")
			releaseFailure := worker.release(task, failure, cleanup)
			<-slots
			reportFatal(errors.Join(failure, releaseFailure))
			return
		}
		if claimContext.Err() != nil {
			worker.releaseLate(task, claimContext, cleanup, reportFatal, recordFailure)
			<-slots
			return
		}

		renewalFailure := safeRenew(task, claimContext, worker.config.LeaseDuration)
		if renewalFailure != nil {
			releaseFailure := worker.release(task, renewalFailure, cleanup)
			<-slots
			if releaseFailure != nil {
				reportFatal(errors.Join(
					fmt.Errorf("renew delivery lease: %w", renewalFailure),
					releaseFailure,
				))
			}
			continue
		}

		if !active.add() {
			worker.releaseLate(task, claimContext, cleanup, reportFatal, recordFailure)
			<-slots
			return
		}
		go worker.execute(
			executionRoot,
			task,
			command,
			active,
			slots,
			cleanup,
			reportFatal,
		)
	}
}

func (worker *Worker) execute(
	executionRoot context.Context,
	task applicationdelivery.Task,
	command applicationdelivery.Command,
	active *activeTracker,
	slots chan struct{},
	cleanup *cleanupState,
	reportFatal func(error),
) {
	defer active.doneOne()
	defer func() { <-slots }()

	taskContext, cancel := context.WithTimeout(executionRoot, worker.config.ExecutionTimeout)
	defer cancel()

	var executeFailure error
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		executeFailure = task.Execute(taskContext, command)
	}()
	if panicValue != nil {
		panicFailure := panicFailure("delivery task execution", panicValue)
		reportFatal(errors.Join(panicFailure, worker.release(task, panicFailure, cleanup)))
		return
	}
	if executeFailure == nil {
		return
	}
	releaseFailure := worker.release(task, executeFailure, cleanup)
	if errors.Is(executeFailure, applicationdelivery.ErrOutcomeFinalization) {
		reportFatal(errors.Join(
			fmt.Errorf("execute delivery task: %w", executeFailure),
			releaseFailure,
		))
		return
	}
	if releaseFailure != nil {
		reportFatal(errors.Join(
			fmt.Errorf("execute delivery task: %w", executeFailure),
			releaseFailure,
		))
	}
}

func safeRenew(
	task applicationdelivery.Task,
	context context.Context,
	duration time.Duration,
) (failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = panicFailure("delivery task renewal", recovered)
		}
	}()
	return task.Renew(context, duration)
}

func (worker *Worker) release(
	task applicationdelivery.Task,
	cause error,
	cleanup *cleanupState,
) error {
	if cause == nil {
		cause = errors.New("delivery task release without cause")
	}
	context, cancel := cleanup.forRelease(worker.config.CleanupTimeout)
	defer cancel()
	return safeRelease(task, context, cause)
}

func (worker *Worker) releaseLate(
	task applicationdelivery.Task,
	claimContext context.Context,
	cleanup *cleanupState,
	reportFatal func(error),
	recordFailure func(error),
) {
	releaseFailure := worker.release(task, claimCause(claimContext), cleanup)
	if releaseFailure == nil {
		return
	}
	if claimContext.Err() != nil {
		recordFailure(fmt.Errorf("release late delivery task: %w", releaseFailure))
		return
	}
	reportFatal(fmt.Errorf("release late delivery task: %w", releaseFailure))
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

func claimCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("delivery claim stopped")
}

func taskRoute(task applicationdelivery.Task) (route applicationdelivery.Route, failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = panicFailure("delivery task route", recovered)
		}
	}()
	return task.Route(), nil
}

func resolveCommand(
	commands Commands,
	context context.Context,
	route applicationdelivery.Route,
) (command applicationdelivery.Command, failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = panicFailure("delivery command resolver", recovered)
		}
	}()
	return commands.Command(context, route)
}

func safeRelease(
	task applicationdelivery.Task,
	context context.Context,
	cause error,
) (failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = panicFailure("delivery task release", recovered)
		}
	}()
	return task.Release(context, cause)
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

// activeTracker permits the coordinator to stop admitting tasks before the
// shutdown drain begins. A plain WaitGroup cannot safely be Add-ed while a
// waiter is already observing a zero counter.
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

func (state *failureState) record(failure error) {
	if failure == nil {
		return
	}
	state.mutex.Lock()
	state.failures = append(state.failures, failure)
	state.mutex.Unlock()
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
