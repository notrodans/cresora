package background

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunnerParentCancellationJoinsJobsCleanly(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	joined := make(chan struct{}, 2)
	job := func(context.Context) error {
		started <- struct{}{}
		<-root.Done()
		joined <- struct{}{}
		return root.Err()
	}

	finished := make(chan error, 1)
	go func() {
		finished <- NewRunner([]Job{job, job}, time.Second).Run(root)
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("background job did not start")
		}
	}
	cancel()

	select {
	case failure := <-finished:
		if !errors.Is(failure, context.Canceled) {
			t.Fatalf("parent cancellation = %v, want context cancellation", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
	for range 2 {
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Fatal("background job was not joined")
		}
	}
}

func TestRunnerFailureCancelsAndJoinsSiblings(t *testing.T) {
	expected := errors.New("job failed")
	siblingStopped := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- NewRunner([]Job{
			func(context.Context) error { return expected },
			func(context context.Context) error {
				<-context.Done()
				close(siblingStopped)
				return nil
			},
		}, time.Second).Run(context.Background())
	}()
	select {
	case failure := <-finished:
		if !errors.Is(failure, expected) {
			t.Fatalf("job failure = %v, want %v", failure, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not propagate job failure")
	}
	select {
	case <-siblingStopped:
	case <-time.After(time.Second):
		t.Fatal("runner did not cancel and join sibling")
	}
}

func TestRunnerNilCompletionIsFailure(t *testing.T) {
	failure := NewRunner([]Job{func(context.Context) error { return nil }}, time.Second).Run(context.Background())
	if !errors.Is(failure, ErrJobCompleted) {
		t.Fatalf("nil completion = %v, want %v", failure, ErrJobCompleted)
	}
}

func TestRunnerPanicIsConvertedToFailure(t *testing.T) {
	failure := NewRunner([]Job{func(context.Context) error {
		panic("boom")
	}}, time.Second).Run(context.Background())
	if failure == nil || !strings.Contains(failure.Error(), "background job panicked: boom") {
		t.Fatalf("panic failure = %v, want converted panic", failure)
	}
}

func TestRunnerShutdownTimeoutIsBounded(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	finished := make(chan struct{})
	runnerFinished := make(chan error, 1)
	root, cancel := context.WithCancel(context.Background())
	go func() {
		failure := NewRunner([]Job{func(context.Context) error {
			close(started)
			<-release
			close(finished)
			return nil
		}}, time.Millisecond).Run(root)
		runnerFinished <- failure
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("uncooperative job did not start")
	}
	cancel()

	select {
	case <-finished:
		t.Fatal("uncooperative job completed before timeout")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case failure := <-runnerFinished:
		if !errors.Is(failure, ErrShutdownTimeout) {
			t.Fatalf("shutdown failure = %v, want %v", failure, ErrShutdownTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not enforce shutdown timeout")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("timed-out job did not eventually finish")
	}
}
