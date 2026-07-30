package challenges

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultCleanupWorkers is deliberately small. Provider cancellation is
	// best-effort and a provider which ignores its context can occupy a worker
	// until the process exits.
	DefaultCleanupWorkers = 4
	// DefaultCleanupQueueSize bounds the number of provider cancellations which
	// may wait behind occupied cleanup workers.
	DefaultCleanupQueueSize = 256
	// MaxCleanupWorkers and MaxCleanupQueueSize prevent a malformed composition
	// from turning the configured bounds into an effectively unbounded pool.
	MaxCleanupWorkers   = 32
	MaxCleanupQueueSize = 4096
)

type cleanupTask struct {
	kind   Kind
	handle ProviderHandle
}

// cleanupExecutor owns all provider cancellation calls. Enqueue is
// intentionally nonblocking: state has already been removed before a cleanup
// task is offered to this executor, so dropping a task is preferable to
// delaying a request behind a provider which does not honor cancellation.
// The provider call receives a timeout context, but Go cannot forcibly stop a
// provider which ignores that context. Such a call consumes one bounded worker
// until the provider returns.
type cleanupExecutor struct {
	queue   chan cleanupTask
	workers int
	timeout time.Duration
	phone   PhoneProvider
	qr      QRProvider

	stopOnce sync.Once
	mu       sync.Mutex
	stopped  bool
	stop     chan struct{}
	done     chan struct{}
	wait     sync.WaitGroup
	dropped  atomic.Uint64
}

func newCleanupExecutor(workers, queueSize int, timeout time.Duration, phone PhoneProvider, qr QRProvider) *cleanupExecutor {
	if workers <= 0 {
		workers = DefaultCleanupWorkers
	}
	if workers > MaxCleanupWorkers {
		workers = MaxCleanupWorkers
	}
	if queueSize <= 0 {
		queueSize = DefaultCleanupQueueSize
	}
	if queueSize > MaxCleanupQueueSize {
		queueSize = MaxCleanupQueueSize
	}

	executor := &cleanupExecutor{
		queue:   make(chan cleanupTask, queueSize),
		workers: workers,
		timeout: timeout,
		phone:   phone,
		qr:      qr,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	executor.wait.Add(workers)
	for index := 0; index < workers; index++ {
		go executor.worker()
	}
	go func() {
		executor.wait.Wait()
		close(executor.done)
	}()
	return executor
}

func (executor *cleanupExecutor) worker() {
	defer executor.wait.Done()
	for {
		select {
		case <-executor.stop:
			executor.drain()
			return
		case task := <-executor.queue:
			executor.run(task)
		}
	}
}

// drain completes tasks which were accepted before stop. It is deliberately
// performed by workers, not by Stop itself: Stop remains nonblocking while the
// caller of Shutdown can put a deadline around cooperative cleanup. No task
// can be enqueued after stopped is set.
func (executor *cleanupExecutor) drain() {
	for {
		select {
		case task := <-executor.queue:
			executor.run(task)
		default:
			return
		}
	}
}

func (executor *cleanupExecutor) run(task cleanupTask) {
	cleanupContext, cancel := context.WithTimeout(context.Background(), executor.timeout)
	defer cancel()
	switch task.kind {
	case KindPhone:
		if executor.phone != nil {
			_ = executor.phone.CancelPhone(cleanupContext, task.handle)
		}
	case KindQR:
		if executor.qr != nil {
			_ = executor.qr.CancelQR(cleanupContext, task.handle)
		}
	}
}

func (executor *cleanupExecutor) enqueue(task cleanupTask) bool {
	if executor == nil || task.handle.empty() {
		return false
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.stopped {
		return false
	}
	select {
	case executor.queue <- task:
		return true
	default:
		executor.dropped.Add(1)
		return false
	}
}

// stop is nonblocking. It rejects new tasks; workers drain tasks accepted
// before stop, but it does not pretend it can interrupt a provider call which
// is already running or ignores its context.
func (executor *cleanupExecutor) stopWorkers() {
	if executor == nil {
		return
	}
	executor.stopOnce.Do(func() {
		executor.mu.Lock()
		executor.stopped = true
		close(executor.stop)
		executor.mu.Unlock()
	})
}

func (executor *cleanupExecutor) waitForStop(ctx context.Context) error {
	if executor == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidInput
	}
	select {
	case <-executor.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
