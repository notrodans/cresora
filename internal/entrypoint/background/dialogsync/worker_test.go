package dialogsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/notrodans/cresora/internal/application/dialogsync"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

func TestWorkerConfigValidation(t *testing.T) {
	valid := Defaults()
	if err := valid.validate(); err != nil {
		t.Fatalf("defaults validate: %v", err)
	}
	broken := Defaults()
	broken.ExecutionTimeout = 0
	if err := broken.validate(); err == nil {
		t.Fatalf("validate accepted a zero execution timeout")
	}
}

func TestWorkerCompletesTaskAndStops(t *testing.T) {
	task := newFakeTask()
	store := &fakeClaimStore{tasks: []dialogsync.Task{task}}
	executorDone := make(chan struct{})

	worker := New(store, fakeExecutor(func(task dialogsync.Task) error {
		close(executorDone)
		return nil
	}), Defaults())

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-executorDone
		cancel()
	}()

	result := worker.Run(parent)
	if result != nil && !errors.Is(result, context.Canceled) {
		t.Fatalf("run returned %v, want nil or cancellation", result)
	}
	if task.failed || task.retried {
		t.Fatalf("task was retried %t or failed %t on a successful run", task.retried, task.failed)
	}
}

func TestWorkerRetriesFloodWait(t *testing.T) {
	task := newFakeTask()
	processed := make(chan struct{})
	worker := New(&fakeClaimStore{tasks: []dialogsync.Task{task}},
		fakeExecutor(func(task dialogsync.Task) error {
			close(processed)
			return &dialogsync.FloodWaitError{Duration: 15 * time.Second}
		}), Defaults())

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-processed
		<-time.After(50 * time.Millisecond)
		cancel()
	}()
	result := worker.Run(parent)
	if result != nil && !errors.Is(result, context.Canceled) {
		t.Fatalf("run returned %v, want nil or cancellation", result)
	}
	if !task.retried {
		t.Fatalf("task was not retried for a flood wait")
	}
}

func TestWorkerFailPermanently(t *testing.T) {
	task := newFakeTask()
	processed := make(chan struct{})
	worker := New(&fakeClaimStore{tasks: []dialogsync.Task{task}},
		fakeExecutor(func(task dialogsync.Task) error {
			close(processed)
			return dialogsync.WrapPermanent(errors.New("session revoked"))
		}), Defaults())

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-processed
		<-time.After(50 * time.Millisecond)
		cancel()
	}()
	result := worker.Run(parent)
	if result != nil && !errors.Is(result, context.Canceled) {
		t.Fatalf("run returned %v, want nil or cancellation", result)
	}
	if !task.failed {
		t.Fatalf("task was not failed for a permanent error")
	}
}

type fakeClaimStore struct {
	tasks  []dialogsync.Task
	serial int
}

func (store *fakeClaimStore) Claim(ctx context.Context, lease time.Duration) (dialogsync.Task, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if store.serial >= len(store.tasks) {
		return nil, dialogsync.ErrEmpty
	}
	task := store.tasks[store.serial]
	store.serial++
	return task, nil
}

type fakeExecutor func(dialogsync.Task) error

func (executor fakeExecutor) Sync(ctx context.Context, task dialogsync.Task) error {
	return executor(task)
}

type fakeTask struct {
	completed bool
	retried   bool
	failed    bool
	released  bool
}

func newFakeTask() *fakeTask { return &fakeTask{} }

func (t *fakeTask) Key() dialogsync.TaskKey { return dialogsync.TaskKey{} }

func (t *fakeTask) Revalidate(ctx context.Context) (operatoraccounts.RuntimeTarget, error) {
	return operatoraccounts.RuntimeTarget{}, nil
}

func (t *fakeTask) Renew(ctx context.Context, duration time.Duration) error { return nil }

func (t *fakeTask) Complete(ctx context.Context, shared []dialogsync.SharedDialog, private []dialogsync.PrivateDialog) error {
	t.completed = true
	return nil
}

func (t *fakeTask) Retry(ctx context.Context, cause error, delay time.Duration) error {
	t.retried = true
	return nil
}

func (t *fakeTask) Fail(ctx context.Context, cause error) error {
	t.failed = true
	return nil
}

func (t *fakeTask) Release(ctx context.Context, cause error) error {
	t.released = true
	return nil
}
