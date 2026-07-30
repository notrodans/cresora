package deliveryreconciler

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
)

type fakeReconciler struct {
	calls   chan time.Time
	failure error
	count   atomic.Int32
}

func (reconciler *fakeReconciler) Reconcile(context.Context) (application.ReconciliationResult, error) {
	reconciler.count.Add(1)
	if reconciler.calls != nil {
		reconciler.calls <- time.Now()
	}
	return application.ReconciliationResult{}, reconciler.failure
}

func TestLoopReconcilesImmediatelyThenRunsSecondPassAfterInterval(t *testing.T) {
	reconciler := &fakeReconciler{calls: make(chan time.Time, 4)}
	interval := 50 * time.Millisecond
	loop := New(reconciler, Config{Interval: interval})
	root, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- loop.Run(root) }()

	select {
	case firstPassAt := <-reconciler.calls:
		select {
		case secondPassAt := <-reconciler.calls:
			if elapsed := secondPassAt.Sub(firstPassAt); elapsed < interval {
				t.Fatalf("second reconciliation pass started after %s, want at least %s", elapsed, interval)
			}
		case <-time.After(time.Second):
			t.Fatal("reconciler did not receive a second periodic pass")
		}
	case <-time.After(time.Second):
		t.Fatal("reconciler did not receive the immediate first pass")
	}
	cancel()

	select {
	case failure := <-finished:
		if !errors.Is(failure, context.Canceled) {
			t.Fatalf("canceled loop = %v, want context cancellation", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("loop did not stop promptly after cancellation")
	}
}

func TestLoopWrapsReconcilerFailureWithoutRetrying(t *testing.T) {
	expected := errors.New("database unavailable")
	reconciler := &fakeReconciler{failure: expected}
	failure := New(reconciler, Config{Interval: time.Minute}).Run(context.Background())
	if !errors.Is(failure, expected) {
		t.Fatalf("reconciler failure = %v, want %v", failure, expected)
	}
	if !strings.Contains(failure.Error(), "run delivery reconciler pass") {
		t.Fatalf("reconciler failure = %v, want wrapped pass context", failure)
	}
	if reconciler.count.Load() != 1 {
		t.Fatalf("reconciler pass count = %d, want one", reconciler.count.Load())
	}
}

func TestLoopRejectsNonPositiveIntervalBeforeReconciliation(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		reconciler := &fakeReconciler{}
		failure := New(reconciler, Config{Interval: interval}).Run(context.Background())
		if !errors.Is(failure, ErrInvalidConfig) {
			t.Fatalf("interval %s failure = %v, want invalid config", interval, failure)
		}
		if reconciler.count.Load() != 0 {
			t.Fatalf("interval %s invoked reconciler %d times", interval, reconciler.count.Load())
		}
	}
}

func TestLoopCancellationBeforeStartDoesNotReconcile(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	cancel()
	reconciler := &fakeReconciler{}
	failure := New(reconciler, Defaults()).Run(root)
	if !errors.Is(failure, context.Canceled) {
		t.Fatalf("pre-canceled loop = %v, want context cancellation", failure)
	}
	if reconciler.count.Load() != 0 {
		t.Fatal("pre-canceled loop ran a reconciliation pass")
	}
}
