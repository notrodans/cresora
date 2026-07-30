package deliveryworker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	applicationdelivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/recipient"
)

func TestDefaultsAndConfigValidation(t *testing.T) {
	config := Defaults()
	if err := config.Validate(); err != nil {
		t.Fatalf("Defaults().Validate() returned %v, want nil", err)
	}
	checks := map[string]struct {
		got  time.Duration
		want time.Duration
	}{
		"empty poll":          {config.EmptyPoll, 250 * time.Millisecond},
		"execution timeout":   {config.ExecutionTimeout, 30 * time.Second},
		"drain timeout":       {config.DrainTimeout, 8 * time.Second},
		"cleanup timeout":     {config.CleanupTimeout, 2 * time.Second},
		"lease duration":      {config.LeaseDuration, 60 * time.Second},
		"lease safety margin": {config.LeaseSafetyMargin, 5 * time.Second},
	}
	for name, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %s, want %s", name, check.got, check.want)
		}
	}

	leaseSafety, err := config.LeaseSafetyDuration()
	if err != nil {
		t.Fatalf("LeaseSafetyDuration() returned %v, want nil error", err)
	}
	if leaseSafety != 37*time.Second {
		t.Fatalf("LeaseSafetyDuration() = %s, want 37s", leaseSafety)
	}

	invalid := []struct {
		name  string
		apply func(*Config)
	}{
		{"empty poll", func(config *Config) { config.EmptyPoll = 0 }},
		{"execution timeout", func(config *Config) { config.ExecutionTimeout = 0 }},
		{"drain timeout", func(config *Config) { config.DrainTimeout = 0 }},
		{"cleanup timeout", func(config *Config) { config.CleanupTimeout = 0 }},
		{"lease duration", func(config *Config) { config.LeaseDuration = 0 }},
		{"lease safety margin", func(config *Config) { config.LeaseSafetyMargin = 0 }},
		{"lease bound", func(config *Config) { config.LeaseDuration = 37 * time.Second }},
		{"lease arithmetic overflow", func(config *Config) {
			config.ExecutionTimeout = maxDuration - time.Nanosecond
			config.LeaseSafetyMargin = 2 * time.Nanosecond
		}},
		{"shutdown arithmetic overflow", func(config *Config) {
			config.DrainTimeout = maxDuration
			config.CleanupTimeout = time.Nanosecond
		}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			config := Defaults()
			test.apply(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestWorkerClaimsWithOneCoordinatorAndNoPrefetch(t *testing.T) {
	config := testConfig()
	claims := &claimsStub{}
	commands := &commandsStub{}
	started := make(chan struct{}, executionCapacity)
	// Содержит текущее количество одновременно работающих задач.
	var running atomic.Int32
	// Хранит максимальное количество одновременно выполнявшихся задач.
	// Например, значения running менялись так: 0 -> 1 -> 2 -> 3 -> 4
	// Тогда maximum должно стать равным 4.
	var maximum atomic.Int32
	for range executionCapacity {
		task := &taskStub{}
		task.execute = func(ctx context.Context, _ applicationdelivery.Command) error {
			current := running.Add(1)
			// Повторный цикл нужен из-за возможной гонки между горутинами
			// Горутина A прочитала maximum = 2
			// Горутина B изменила maximum на 3
			// Горутина A пытается заменить 2 на 4
			// CAS не срабатывает, потому что там уже 3
			// Горутина A повторяет попытку
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) { // CompareAndSwap установит current, только если значение всё ещё равно old.
					break
				}
			}
			started <- struct{}{}
			// Блокируем пока воркер не отменит контест.
			<-ctx.Done()
			// Задача уменьшает счётчик и возвращает context.Canceled.
			running.Add(-1)
			return ctx.Err()
		}
		claims.tasks = append(claims.tasks, task)
	}
	// Используется в качестве одноразового сигнала.
	claimFive := make(chan struct{})
	claims.after = func(number int) {
		// пятый Claim произошёл -> claimFive закрыт.
		if number == executionCapacity+1 {
			close(claimFive)
		}
	}

	worker := New(claims, commands, config)
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(worker, parent)
	waitForCount(t, started, executionCapacity)
	if got := claims.count(); got != executionCapacity {
		t.Fatalf("Claim called %d times while all slots were active, want %d", got, executionCapacity)
	}
	assertNoSignal(t, claimFive, 25*time.Millisecond)
	if got := maximum.Load(); got != executionCapacity {
		t.Fatalf("maximum concurrent executions = %d, want %d", got, executionCapacity)
	}

	cancel()
	err := awaitResult(t, result)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestConcurrentWorkersProcessSharedClaimsWithoutDuplicateTasks(t *testing.T) {
	const taskCount = 8
	claims := &claimsStub{}
	executed := make(chan struct{}, taskCount)
	tasks := make([]*taskStub, 0, taskCount)
	for range taskCount {
		task := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
			executed <- struct{}{}
			return nil
		}}
		tasks = append(tasks, task)
		claims.tasks = append(claims.tasks, task)
	}

	parentOne, cancelOne := context.WithCancel(context.Background())
	parentTwo, cancelTwo := context.WithCancel(context.Background())
	resultOne := runWorker(New(claims, &commandsStub{}, testConfig()), parentOne)
	resultTwo := runWorker(New(claims, &commandsStub{}, testConfig()), parentTwo)
	waitForCount(t, executed, taskCount)
	cancelOne()
	cancelTwo()
	if err := awaitResult(t, resultOne); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run() error = %v, want context.Canceled", err)
	}
	if err := awaitResult(t, resultTwo); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Run() error = %v, want context.Canceled", err)
	}
	for index, task := range tasks {
		if got := task.executeCount(); got != 1 {
			t.Errorf("task %d Execute() called %d times, want 1", index, got)
		}
	}
}

func TestWorkerEmptyPollCancelsImmediately(t *testing.T) {
	config := testConfig()
	config.EmptyPoll = time.Hour
	claimed := make(chan struct{})
	claims := &claimsStub{claim: func(ctx context.Context) (applicationdelivery.Task, error) {
		closeOnce(claimed)
		return nil, applicationdelivery.ErrEmpty
	}}
	worker := New(claims, &commandsStub{}, config)
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(worker, parent)
	awaitSignal(t, claimed)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestWorkerNonEmptyClaimFailureIsFatal(t *testing.T) {
	want := errors.New("claim database failed")
	claimStopped := make(chan struct{})
	claims := &claimsStub{claim: func(ctx context.Context) (applicationdelivery.Task, error) {
		go func() {
			<-ctx.Done()
			closeOnce(claimStopped)
		}()
		return nil, want
	}}
	worker := New(claims, &commandsStub{}, testConfig())
	err := runAndAwait(t, worker)
	if !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want claim error %v", err, want)
	}
	awaitSignal(t, claimStopped)
}

func TestWorkerResolverFailureReleasesAndIsFatal(t *testing.T) {
	want := errors.New("command resolver failed")
	released := make(chan struct{})
	task := &taskStub{release: func(_ context.Context, cause error) error {
		if !errors.Is(cause, want) {
			t.Errorf("Release() cause = %v, want %v", cause, want)
		}
		closeOnce(released)
		return nil
	}}
	claims := &claimsStub{tasks: []applicationdelivery.Task{task}}
	commands := &commandsStub{err: want}
	err := runAndAwait(t, New(claims, commands, testConfig()))
	awaitSignal(t, released)
	if !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want resolver error %v", err, want)
	}
	if got := task.releaseCount(); got != 1 {
		t.Fatalf("Release() called %d times, want 1", got)
	}
}

func TestWorkerSuccessDoesNotRelease(t *testing.T) {
	executed := make(chan struct{})
	claims := &claimsStub{}
	task := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		closeOnce(executed)
		return nil
	}}
	claims.tasks = []applicationdelivery.Task{task}
	worker := New(claims, &commandsStub{}, testConfig())
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(worker, parent)
	awaitSignal(t, executed)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := task.releaseCount(); got != 0 {
		t.Fatalf("Release() called %d times after success, want 0", got)
	}
}

func TestWorkerRenewsExactlyOnceBeforeExecution(t *testing.T) {
	config := testConfig()
	renewed := make(chan struct{})
	executed := make(chan struct{})
	var task *taskStub
	task = &taskStub{
		renew: func(_ context.Context, duration time.Duration) error {
			if duration != config.LeaseDuration {
				t.Errorf("Renew() duration = %s, want %s", duration, config.LeaseDuration)
			}
			closeOnce(renewed)
			return nil
		},
		execute: func(context.Context, applicationdelivery.Command) error {
			if got := task.renewCount(); got != 1 {
				t.Errorf("Renew() count before Execute() = %d, want 1", got)
			}
			closeOnce(executed)
			return nil
		},
	}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, config), parent)
	awaitSignal(t, renewed)
	awaitSignal(t, executed)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := task.renewCount(); got != 1 {
		t.Fatalf("Renew() called %d times, want 1", got)
	}
}

func TestWorkerRenewalFailureReleasesAndContinues(t *testing.T) {
	renewalFailure := errors.New("renewal failed")
	released := make(chan struct{})
	secondExecuted := make(chan struct{})
	first := &taskStub{
		renew: func(context.Context, time.Duration) error { return renewalFailure },
		release: func(_ context.Context, cause error) error {
			if !errors.Is(cause, renewalFailure) {
				t.Errorf("Release() cause = %v, want %v", cause, renewalFailure)
			}
			closeOnce(released)
			return nil
		},
	}
	second := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		closeOnce(secondExecuted)
		return nil
	}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(&claimsStub{tasks: []applicationdelivery.Task{first, second}}, &commandsStub{}, testConfig()), parent)
	awaitSignal(t, released)
	awaitSignal(t, secondExecuted)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := first.executeCount(); got != 0 {
		t.Fatalf("failed-renewal Execute() count = %d, want 0", got)
	}
}

func TestWorkerRenewalPanicReleasesAndContinues(t *testing.T) {
	panicCause := errors.New("renewal panic")
	released := make(chan struct{})
	secondExecuted := make(chan struct{})
	first := &taskStub{
		renew: func(context.Context, time.Duration) error { panic(panicCause) },
		release: func(_ context.Context, cause error) error {
			if !errors.Is(cause, panicCause) {
				t.Errorf("Release() panic cause = %v, want %v", cause, panicCause)
			}
			closeOnce(released)
			return nil
		},
	}
	second := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		closeOnce(secondExecuted)
		return nil
	}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(&claimsStub{tasks: []applicationdelivery.Task{first, second}}, &commandsStub{}, testConfig()), parent)
	awaitSignal(t, released)
	awaitSignal(t, secondExecuted)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestWorkerRenewalReleaseFailureIsFatal(t *testing.T) {
	renewalFailure := errors.New("renewal failed")
	releaseFailure := errors.New("release failed")
	task := &taskStub{
		renew:   func(context.Context, time.Duration) error { return renewalFailure },
		release: func(context.Context, error) error { return releaseFailure },
	}
	err := runAndAwait(t, New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, testConfig()))
	if !errors.Is(err, renewalFailure) {
		t.Fatalf("Run() error = %v, want renewal error %v", err, renewalFailure)
	}
	if !errors.Is(err, releaseFailure) {
		t.Fatalf("Run() error = %v, want release error %v", err, releaseFailure)
	}
}

func TestWorkerRenewalReleasePanicIsFatal(t *testing.T) {
	renewalFailure := errors.New("renewal failed")
	releasePanic := errors.New("release panic")
	task := &taskStub{
		renew: func(context.Context, time.Duration) error { return renewalFailure },
		release: func(context.Context, error) error {
			panic(releasePanic)
		},
	}
	err := runAndAwait(t, New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, testConfig()))
	if !errors.Is(err, renewalFailure) {
		t.Fatalf("Run() error = %v, want renewal error %v", err, renewalFailure)
	}
	if !errors.Is(err, releasePanic) {
		t.Fatalf("Run() error = %v, want release panic %v", err, releasePanic)
	}
}

func TestWorkerOutcomeFinalizationFailureIsFatalWithoutReleaseFailure(t *testing.T) {
	finalizationFailure := errors.New("database finalization failed")
	task := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		return errors.Join(applicationdelivery.ErrOutcomeFinalization, finalizationFailure)
	}}
	err := runAndAwait(t, New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, testConfig()))
	if !errors.Is(err, applicationdelivery.ErrOutcomeFinalization) {
		t.Fatalf("Run() error = %v, want ErrOutcomeFinalization", err)
	}
	if !errors.Is(err, finalizationFailure) {
		t.Fatalf("Run() error = %v, want finalization failure %v", err, finalizationFailure)
	}
}

func TestWorkerExecutionErrorReleasesAndContinues(t *testing.T) {
	executionFailure := errors.New("delivery execution failed")
	firstReleased := make(chan struct{})
	secondExecuted := make(chan struct{})
	first := &taskStub{
		execute: func(context.Context, applicationdelivery.Command) error { return executionFailure },
		release: func(_ context.Context, cause error) error {
			if !errors.Is(cause, executionFailure) {
				t.Errorf("Release() cause = %v, want %v", cause, executionFailure)
			}
			closeOnce(firstReleased)
			return nil
		},
	}
	second := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		closeOnce(secondExecuted)
		return nil
	}}
	claims := &claimsStub{tasks: []applicationdelivery.Task{first, second}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(claims, &commandsStub{}, testConfig()), parent)
	awaitSignal(t, firstReleased)
	awaitSignal(t, secondExecuted)
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := first.releaseCount(); got != 1 {
		t.Fatalf("first Release() called %d times, want 1", got)
	}
	if got := second.releaseCount(); got != 0 {
		t.Fatalf("second Release() called %d times, want 0", got)
	}
}

func TestWorkerExecutionErrorWithReleaseFailureIsFatalAndJoined(t *testing.T) {
	executionFailure := errors.New("execution failed")
	releaseFailure := errors.New("release failed")
	task := &taskStub{
		execute: func(context.Context, applicationdelivery.Command) error { return executionFailure },
		release: func(context.Context, error) error { return releaseFailure },
	}
	err := runAndAwait(t, New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, testConfig()))
	if !errors.Is(err, executionFailure) {
		t.Fatalf("Run() error = %v, want execution error %v", err, executionFailure)
	}
	if !errors.Is(err, releaseFailure) {
		t.Fatalf("Run() error = %v, want release error %v", err, releaseFailure)
	}
}

func TestWorkerExecutionRootDrainsAfterParentCancellation(t *testing.T) {
	config := testConfig()
	config.ExecutionTimeout = time.Second
	config.LeaseSafetyMargin = time.Millisecond
	config.LeaseDuration = 4 * time.Second
	type contextKey struct{}
	type observation struct {
		value any
		err   error
	}
	started := make(chan struct{})
	probe := make(chan chan observation)
	finish := make(chan struct{})
	task := &taskStub{execute: func(ctx context.Context, _ applicationdelivery.Command) error {
		closeOnce(started)
		select {
		case response := <-probe:
			response <- observation{value: ctx.Value(contextKey{}), err: ctx.Err()}
		case <-ctx.Done():
			return ctx.Err()
		}
		<-finish
		return nil
	}}
	parent := context.WithValue(context.Background(), contextKey{}, "trace-value")
	parent, cancel := context.WithCancel(parent)
	result := runWorker(New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, config), parent)
	awaitSignal(t, started)
	cancel()
	response := make(chan observation, 1)
	select {
	case probe <- response:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not accept context probe")
	}
	select {
	case got := <-response:
		if got.value != "trace-value" {
			t.Fatalf("Task.Execute() context value = %v, want trace-value", got.value)
		}
		if got.err != nil {
			t.Fatalf("Task.Execute() context error = %v, want nil after parent cancellation", got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task did not report context probe")
	}
	close(finish)
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestWorkerForceCancelsExecutionAndReportsShutdownTimeout(t *testing.T) {
	config := testConfig()
	config.DrainTimeout = 10 * time.Millisecond
	config.CleanupTimeout = 10 * time.Millisecond
	executionStarted := make(chan struct{})
	allowExecution := make(chan struct{})
	executionDone := make(chan struct{})
	task := &taskStub{execute: func(context.Context, applicationdelivery.Command) error {
		closeOnce(executionStarted)
		<-allowExecution
		closeOnce(executionDone)
		return nil
	}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, config), parent)
	awaitSignal(t, executionStarted)
	shutdownStarted := time.Now()
	cancel()
	err := awaitResult(t, result)
	elapsed := time.Since(shutdownStarted)
	shutdownBudget := config.DrainTimeout + config.CleanupTimeout
	if elapsed > 4*shutdownBudget {
		t.Fatalf("Run() returned after %s, want no more than %s", elapsed, 4*shutdownBudget)
	}
	close(allowExecution)
	awaitSignal(t, executionDone)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Run() error = %v after late execution completion, want ErrShutdownTimeout", err)
	}
}

func TestWorkerCleanupContextIsIndependentAndLateClaimIsReleased(t *testing.T) {
	config := testConfig()
	config.DrainTimeout = 10 * time.Millisecond
	config.CleanupTimeout = 20 * time.Millisecond
	claimEntered := make(chan struct{})
	allowLateClaim := make(chan struct{})
	releaseStarted := make(chan struct{})
	releaseDone := make(chan struct{})
	task := &taskStub{release: func(ctx context.Context, _ error) error {
		if ctx.Err() != nil {
			t.Errorf("late Release() context initially has error %v, want nil", ctx.Err())
		}
		closeOnce(releaseStarted)
		<-ctx.Done()
		closeOnce(releaseDone)
		return ctx.Err()
	}}
	claims := &claimsStub{claim: func(ctx context.Context) (applicationdelivery.Task, error) {
		closeOnce(claimEntered)
		<-ctx.Done()
		<-allowLateClaim
		return task, nil
	}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(claims, &commandsStub{}, config), parent)
	awaitSignal(t, claimEntered)
	cancel()
	// The drain budget expires before allowing the cancellation-ignoring claim
	// to return, so this release observes the global cleanup context.
	waitForTimer(t, config.DrainTimeout+5*time.Millisecond)
	close(allowLateClaim)
	awaitSignal(t, releaseStarted)
	awaitSignal(t, releaseDone)
	err := awaitResult(t, result)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled, got %v", err, err)
	}
	if got := task.executeCount(); got != 0 {
		t.Fatalf("late task Execute() called %d times, want 0", got)
	}
}

func TestWorkerTaskPanicIsRecoveredReleasedAndFatal(t *testing.T) {
	want := errors.New("task panic")
	released := make(chan struct{})
	task := &taskStub{
		execute: func(context.Context, applicationdelivery.Command) error { panic(want) },
		release: func(context.Context, error) error {
			closeOnce(released)
			return nil
		},
	}
	err := runAndAwait(t, New(&claimsStub{tasks: []applicationdelivery.Task{task}}, &commandsStub{}, testConfig()))
	awaitSignal(t, released)
	if !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want panic cause %v", err, want)
	}
	if got := task.releaseCount(); got != 1 {
		t.Fatalf("Release() called %d times, want 1", got)
	}
}

func TestWorkerExecutionDeadlineReleasesAndContinues(t *testing.T) {
	config := testConfig()
	config.ExecutionTimeout = 10 * time.Millisecond
	deadline := make(chan struct{})
	released := make(chan struct{})
	first := &taskStub{execute: func(ctx context.Context, _ applicationdelivery.Command) error {
		<-ctx.Done()
		closeOnce(deadline)
		return ctx.Err()
	}, release: func(context.Context, error) error {
		closeOnce(released)
		return nil
	}}
	claims := &claimsStub{tasks: []applicationdelivery.Task{first}}
	parent, cancel := context.WithCancel(context.Background())
	result := runWorker(New(claims, &commandsStub{}, config), parent)
	awaitSignal(t, deadline)
	awaitSignal(t, released)
	cause := first.releaseCause()
	if !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("Release() cause = %v, want context.DeadlineExceeded", cause)
	}
	cancel()
	if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func testConfig() Config {
	config := Defaults()
	config.EmptyPoll = time.Millisecond
	config.ExecutionTimeout = 50 * time.Millisecond
	config.DrainTimeout = 25 * time.Millisecond
	config.CleanupTimeout = 25 * time.Millisecond
	return config
}

func runAndAwait(t *testing.T, worker *Worker) error {
	t.Helper()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	return awaitResult(t, runWorker(worker, parent))
}

func runWorker(worker *Worker, parent context.Context) <-chan error {
	result := make(chan error, 1)
	go func() { result <- worker.Run(parent) }()
	return result
}

func awaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not return before test deadline")
		return nil
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("signal was not observed before test deadline")
	}
}

func waitForCount(t *testing.T, signal <-chan struct{}, count int) {
	t.Helper()
	for range count {
		awaitSignal(t, signal)
	}
}

func assertNoSignal(t *testing.T, signal <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal("unexpected signal before capacity was released")
	case <-time.After(timeout):
	}
}

func waitForTimer(t *testing.T, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-time.After(3 * time.Second):
		t.Fatal("timer did not fire before test deadline")
	}
}

var closeMutex sync.Mutex

func closeOnce(channel chan struct{}) {
	closeMutex.Lock()
	defer closeMutex.Unlock()
	select {
	case <-channel:
	default:
		close(channel)
	}
}

type claimsStub struct {
	mutex         sync.Mutex
	tasks         []applicationdelivery.Task
	claim         func(context.Context) (applicationdelivery.Task, error)
	after         func(int)
	onContextDone chan struct{}
	calls         int
}

func (stub *claimsStub) Claim(ctx context.Context) (applicationdelivery.Task, error) {
	stub.mutex.Lock()
	stub.calls++
	number := stub.calls
	claim := stub.claim
	if claim == nil && len(stub.tasks) > 0 {
		task := stub.tasks[0]
		stub.tasks = stub.tasks[1:]
		stub.mutex.Unlock()
		if stub.after != nil {
			stub.after(number)
		}
		return task, nil
	}
	after := stub.after
	contextDone := stub.onContextDone
	stub.mutex.Unlock()
	if after != nil {
		after(number)
	}
	if claim != nil {
		return claim(ctx)
	}
	if contextDone != nil {
		closeOnce(contextDone)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stub *claimsStub) count() int {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.calls
}

type commandsStub struct {
	err error
}

func (stub *commandsStub) Command(context.Context, applicationdelivery.Route) (applicationdelivery.Command, error) {
	if stub.err != nil {
		return nil, stub.err
	}
	return commandStub{}, nil
}

type commandStub struct{}

func (commandStub) Execute(
	context.Context,
	mailing.ID,
	mailing.RunID,
	recipient.ID,
	applicationdelivery.Token,
) error {
	return nil
}

type taskStub struct {
	mutex             sync.Mutex
	route             applicationdelivery.Route
	renew             func(context.Context, time.Duration) error
	renewals          int
	execute           func(context.Context, applicationdelivery.Command) error
	release           func(context.Context, error) error
	executions        int
	releases          int
	releaseErr        error
	releaseCauseValue error
	executeDoneCh     chan struct{}
}

func (stub *taskStub) Route() applicationdelivery.Route {
	return stub.route
}

func (stub *taskStub) Renew(ctx context.Context, duration time.Duration) error {
	stub.mutex.Lock()
	stub.renewals++
	renew := stub.renew
	stub.mutex.Unlock()
	if renew == nil {
		return nil
	}
	return renew(ctx, duration)
}

func (stub *taskStub) Execute(ctx context.Context, command applicationdelivery.Command) error {
	stub.mutex.Lock()
	stub.executions++
	execute := stub.execute
	if stub.executeDoneCh == nil {
		stub.executeDoneCh = make(chan struct{})
	}
	stub.mutex.Unlock()
	defer closeOnce(stub.executeDone())
	if execute == nil {
		return nil
	}
	return execute(ctx, command)
}

func (stub *taskStub) Release(ctx context.Context, cause error) error {
	stub.mutex.Lock()
	stub.releases++
	stub.releaseCauseValue = cause
	release := stub.release
	stub.mutex.Unlock()
	if release == nil {
		return stub.releaseErr
	}
	return release(ctx, cause)
}

func (stub *taskStub) executeCount() int {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.executions
}

func (stub *taskStub) renewCount() int {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.renewals
}

func (stub *taskStub) releaseCount() int {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.releases
}

func (stub *taskStub) releaseCause() error {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	return stub.releaseCauseValue
}

func (stub *taskStub) executeDone() chan struct{} {
	stub.mutex.Lock()
	defer stub.mutex.Unlock()
	if stub.executeDoneCh == nil {
		stub.executeDoneCh = make(chan struct{})
	}
	return stub.executeDoneCh
}
