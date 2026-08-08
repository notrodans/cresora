package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	backgroundjobs "github.com/notrodans/cresora/internal/entrypoint/background"
)

type fakeServerController struct {
	done         chan struct{}
	serveReady   chan struct{}
	once         sync.Once
	serveOnce    sync.Once
	shutdownErr  error
	closeErr     error
	shutdownCall int
}

func newFakeServerController() *fakeServerController {
	return &fakeServerController{done: make(chan struct{}), serveReady: make(chan struct{})}
}

func (server *fakeServerController) Shutdown(context.Context) error {
	server.shutdownCall++
	server.once.Do(func() { close(server.done) })
	return server.shutdownErr
}

func (server *fakeServerController) Close() error {
	server.once.Do(func() { close(server.done) })
	return server.closeErr
}

func (server *fakeServerController) serve() error {
	server.serveOnce.Do(func() { close(server.serveReady) })
	<-server.done
	return http.ErrServerClosed
}

type disconnectRecoveryProbe struct {
	started chan struct{}
	release <-chan struct{}
	result  applicationoperatoraccounts.RecoveryResult
	failure error
	once    sync.Once
}

func (probe *disconnectRecoveryProbe) Recover(ctx context.Context) (applicationoperatoraccounts.RecoveryResult, error) {
	probe.once.Do(func() { close(probe.started) })
	if probe.release != nil {
		select {
		case <-probe.release:
		case <-ctx.Done():
			return applicationoperatoraccounts.RecoveryResult{}, ctx.Err()
		}
	}
	return probe.result, probe.failure
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestMonitorRuntimeWebOnlyCleanCancellation(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	if failure := monitorRuntime(root, cancel, server, server.serve); failure != nil {
		t.Fatalf("clean cancellation: %v", failure)
	}
	if server.shutdownCall != 1 {
		t.Fatalf("expected one server shutdown, got %d", server.shutdownCall)
	}
}

func TestMonitorRuntimeServerFailureCancelsRoot(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := newFakeServerController()
	expected := errors.New("listen failed")

	failure := monitorRuntime(root, cancel, server, func() error { return expected })
	if !errors.Is(failure, expected) {
		t.Fatalf("expected server failure, got %v", failure)
	}
	if root.Err() == nil {
		t.Fatal("expected server failure to cancel root")
	}
}

func TestDisconnectRecoveryDoesNotBlockHTTPServe(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	probe := &disconnectRecoveryProbe{started: make(chan struct{}), release: release}
	backgroundErrors := make(chan error, 1)
	go func() {
		backgroundErrors <- backgroundjobs.NewRunner(
			[]backgroundjobs.Job{operatorAccountDisconnectRecoveryJob(probe, nil)},
			lifecycleWaitTimeout,
		).Run(root)
	}()
	server := newFakeServerController()
	monitorDone := make(chan error, 1)
	go func() {
		monitorDone <- monitorRuntime(root, cancel, server, server.serve, backgroundErrors)
	}()

	waitForSignal(t, probe.started, "disconnect recovery")
	waitForSignal(t, server.serveReady, "HTTP serve")
	cancel()
	select {
	case failure := <-monitorDone:
		if failure != nil {
			t.Fatalf("clean cancellation with blocked recovery: %v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("monitor did not join blocked recovery")
	}
}

func TestDisconnectRecoveryJobIsNonfatalUntilSupervisorCancellation(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  applicationoperatoraccounts.RecoveryResult
		failure error
	}{
		{name: "pending", result: applicationoperatoraccounts.RecoveryResult{Pending: 1}},
		{name: "deadline", failure: context.DeadlineExceeded},
		{name: "durable recovery error", failure: errors.New("durable recovery failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := &disconnectRecoveryProbe{
				started: make(chan struct{}),
				result:  test.result,
				failure: test.failure,
			}
			jobDone := make(chan error, 1)
			go func() {
				jobDone <- backgroundjobs.NewRunner(
					[]backgroundjobs.Job{operatorAccountDisconnectRecoveryJob(probe, nil)},
					lifecycleWaitTimeout,
				).Run(root)
			}()
			waitForSignal(t, probe.started, "disconnect recovery")
			select {
			case failure := <-jobDone:
				t.Fatalf("recovery %s became fatal before cancellation: %v", test.name, failure)
			default:
			}

			cancel()
			select {
			case failure := <-jobDone:
				if !errors.Is(failure, context.Canceled) {
					t.Fatalf("recovery %s cancellation: %v, want context cancellation", test.name, failure)
				}
			case <-time.After(time.Second):
				t.Fatalf("recovery %s did not join after cancellation", test.name)
			}
		})
	}
}

func TestMonitorRuntimeReaperFailureShutsDownHTTP(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := newFakeServerController()
	expected := errors.New("reaper failed")
	reaper := make(chan error, 1)
	reaper <- expected

	failure := monitorRuntime(root, cancel, server, server.serve, reaper)
	if !errors.Is(failure, expected) {
		t.Fatalf("expected reaper failure, got %v", failure)
	}
	if server.shutdownCall != 1 {
		t.Fatalf("expected one server shutdown, got %d", server.shutdownCall)
	}
	if root.Err() == nil {
		t.Fatal("expected reaper failure to cancel root")
	}
}

func TestMonitorRuntimeReaperCleanCancellation(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	reaper := make(chan error, 1)
	go func() {
		<-root.Done()
		reaper <- nil
	}()
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	if failure := monitorRuntime(root, cancel, server, server.serve, reaper); failure != nil {
		t.Fatalf("clean reaper cancellation: %v", failure)
	}
}

func TestMonitorRuntimeClosedReaperChannelShutsDownHTTP(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := newFakeServerController()
	reaper := make(chan error)
	close(reaper)

	failure := monitorRuntime(root, cancel, server, server.serve, reaper)
	if !errors.Is(failure, errBackgroundErrorsClosed) {
		t.Fatalf("closed reaper channel = %v, want unexpected completion", failure)
	}
	if server.shutdownCall != 1 {
		t.Fatalf("expected one server shutdown, got %d", server.shutdownCall)
	}
	if root.Err() == nil {
		t.Fatal("expected closed reaper channel to cancel root")
	}
}

func TestMonitorRuntimeShutdownFailure(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	expected := errors.New("shutdown failed")
	server.shutdownErr = expected
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	failure := monitorRuntime(root, cancel, server, server.serve, nil)
	if !errors.Is(failure, expected) {
		t.Fatalf("expected shutdown failure, got %v", failure)
	}
}

func TestMonitorRuntimeReaperWaitTimeoutPropagatesShutdown(t *testing.T) {
	original := lifecycleWaitTimeout
	lifecycleWaitTimeout = time.Millisecond
	defer func() { lifecycleWaitTimeout = original }()

	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	reaper := make(chan error)
	cancel()

	failure := monitorRuntime(root, cancel, server, server.serve, reaper)
	if failure == nil || !containsText(failure.Error(), "background jobs shutdown timed out") {
		t.Fatalf("expected background shutdown timeout, got %v", failure)
	}
	if server.shutdownCall != 1 {
		t.Fatalf("expected one server shutdown, got %d", server.shutdownCall)
	}
}

func containsText(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
