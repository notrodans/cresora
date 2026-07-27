package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeServerController struct {
	done         chan struct{}
	once         sync.Once
	shutdownErr  error
	closeErr     error
	shutdownCall int
}

func newFakeServerController() *fakeServerController {
	return &fakeServerController{done: make(chan struct{})}
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
	<-server.done
	return http.ErrServerClosed
}

func TestMonitorRuntimeWebOnlyCleanCancellation(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	if failure := monitorRuntime(root, cancel, server, server.serve, nil); failure != nil {
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

	failure := monitorRuntime(root, cancel, server, func() error { return expected }, nil)
	if !errors.Is(failure, expected) {
		t.Fatalf("expected server failure, got %v", failure)
	}
	if root.Err() == nil {
		t.Fatal("expected server failure to cancel root")
	}
}

func TestMonitorRuntimeWorkerErrorAndNilCompletion(t *testing.T) {
	for _, test := range []struct {
		name       string
		workerErr  error
		wantSubstr string
	}{
		{name: "error", workerErr: errors.New("worker failed"), wantSubstr: "worker failed"},
		{name: "nil completion", workerErr: nil, wantSubstr: "stopped unexpectedly"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := newFakeServerController()
			worker := make(chan error, 1)
			worker <- test.workerErr
			failure := monitorRuntime(root, cancel, server, server.serve, worker)
			if failure == nil || !containsText(failure.Error(), test.wantSubstr) {
				t.Fatalf("expected %q, got %v", test.wantSubstr, failure)
			}
		})
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

func TestMonitorRuntimeWorkerWaitTimeout(t *testing.T) {
	original := lifecycleWaitTimeout
	lifecycleWaitTimeout = time.Millisecond
	defer func() { lifecycleWaitTimeout = original }()

	root, cancel := context.WithCancel(context.Background())
	server := newFakeServerController()
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()
	worker := make(chan error)

	failure := monitorRuntime(root, cancel, server, server.serve, worker)
	if failure == nil || !containsText(failure.Error(), "shutdown timed out") {
		t.Fatalf("expected worker timeout, got %v", failure)
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
