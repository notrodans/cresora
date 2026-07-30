package deliveryreaper

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	application "github.com/notrodans/cresora/internal/application/commands/delivery"
)

type fakeReaper struct {
	calls   chan time.Time
	failure error
	count   atomic.Int32
}

func (reaper *fakeReaper) Reap(context.Context) (application.ReapResult, error) {
	reaper.count.Add(1)
	if reaper.calls != nil {
		reaper.calls <- time.Now()
	}
	return application.ReapResult{}, reaper.failure
}

func TestLoopReapsImmediatelyThenRunsSecondPassAfterInterval(t *testing.T) {
	reaper := &fakeReaper{calls: make(chan time.Time, 4)}
	interval := 50 * time.Millisecond
	loop := New(reaper, Config{Interval: interval})
	root, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- loop.Run(root) }()

	select {
	case firstPassAt := <-reaper.calls:
		select {
		case secondPassAt := <-reaper.calls:
			if elapsed := secondPassAt.Sub(firstPassAt); elapsed < interval {
				t.Fatalf("second reaper pass started after %s, want at least %s", elapsed, interval)
			}
		case <-time.After(time.Second):
			t.Fatal("reaper did not receive a second periodic pass")
		}
	case <-time.After(time.Second):
		t.Fatal("reaper did not receive the immediate first pass")
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

func TestLoopWrapsReaperFailure(t *testing.T) {
	expected := errors.New("database unavailable")
	loop := New(&fakeReaper{failure: expected}, Config{Interval: time.Minute})
	failure := loop.Run(context.Background())
	if !errors.Is(failure, expected) {
		t.Fatalf("reaper failure = %v, want %v", failure, expected)
	}
	if !strings.Contains(failure.Error(), "run delivery reaper pass") {
		t.Fatalf("reaper failure = %v, want wrapped pass context", failure)
	}
}

func TestLoopRejectsNonPositiveIntervalBeforeReaping(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		reaper := &fakeReaper{}
		failure := New(reaper, Config{Interval: interval}).Run(context.Background())
		if !errors.Is(failure, ErrInvalidConfig) {
			t.Fatalf("interval %s failure = %v, want invalid config", interval, failure)
		}
		if reaper.count.Load() != 0 {
			t.Fatalf("interval %s invoked reaper %d times", interval, reaper.count.Load())
		}
	}
}

func TestLoopCancellationBeforeStartDoesNotReap(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	cancel()
	reaper := &fakeReaper{}
	failure := New(reaper, Defaults()).Run(root)
	if !errors.Is(failure, context.Canceled) {
		t.Fatalf("pre-canceled loop = %v, want context cancellation", failure)
	}
	if reaper.count.Load() != 0 {
		t.Fatal("pre-canceled loop ran a reaper pass")
	}
}
