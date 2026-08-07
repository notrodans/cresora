package dialogsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

func TestSyncerCompletesSuccessfulFetch(t *testing.T) {
	target := operatoraccounts.RuntimeTarget{
		Actor:     application.Actor{OperatorID: uuid.New()},
		AccountID: operatoraccount.Identity(uuid.New()),
		Status:    operatoraccount.StatusActive,
		Version:   3,
	}
	fetcher := &fakeFetcher{shared: []SharedDialog{{PeerID: 1, Kind: SharedBroadcastChannel, Title: "news"}}}
	task := &fakeTask{revalidateTarget: target}
	syncer := NewSyncer(fetcher)

	if failure := syncer.Sync(context.Background(), task); failure != nil {
		t.Fatalf("sync: %v", failure)
	}
	if !fetcher.called {
		t.Fatalf("fetcher was not called")
	}
	if !task.completed {
		t.Fatalf("task was not completed")
	}
	if task.completeShared != 1 {
		t.Fatalf("completed shared count = %d, want 1", task.completeShared)
	}
}

func TestSyncerReleasesWhenAccountGone(t *testing.T) {
	task := &fakeTask{revalidateErr: operatoraccounts.ErrAccountNotFound}
	syncer := NewSyncer(&fakeFetcher{})

	if failure := syncer.Sync(context.Background(), task); failure != nil {
		t.Fatalf("sync: %v", failure)
	}
	if !task.released {
		t.Fatalf("task was not released for a missing account")
	}
	if task.completed {
		t.Fatalf("task was completed for a missing account")
	}
}

func TestSyncerPropagatesFetchFailure(t *testing.T) {
	fetcher := &fakeFetcher{failure: WrapTransient(errors.New("remote exploded"))}
	target := operatoraccounts.RuntimeTarget{Version: 1}
	task := &fakeTask{revalidateTarget: target}
	syncer := NewSyncer(fetcher)

	result := syncer.Sync(context.Background(), task)
	if result == nil {
		t.Fatalf("sync returned nil, want the fetch failure")
	}
	if !errors.Is(result, ErrTransient) {
		t.Fatalf("sync error = %v, want ErrTransient", result)
	}
	if task.completed || task.released {
		t.Fatalf("task was finalized unexpectedly")
	}
}

type fakeFetcher struct {
	shared  []SharedDialog
	failure error
	called  bool
}

func (f *fakeFetcher) Fetch(ctx context.Context, target operatoraccounts.RuntimeTarget) ([]SharedDialog, []PrivateDialog, error) {
	f.called = true
	if f.failure != nil {
		return nil, nil, f.failure
	}
	return f.shared, nil, nil
}

type fakeTask struct {
	key              TaskKey
	revalidateErr    error
	revalidateTarget operatoraccounts.RuntimeTarget
	completed        bool
	completeShared   int
	released         bool
}

func (t *fakeTask) Key() TaskKey { return t.key }

func (t *fakeTask) Revalidate(ctx context.Context) (operatoraccounts.RuntimeTarget, error) {
	if t.revalidateErr != nil {
		return operatoraccounts.RuntimeTarget{}, t.revalidateErr
	}
	return t.revalidateTarget, nil
}

func (t *fakeTask) Renew(ctx context.Context, duration time.Duration) error { return nil }

func (t *fakeTask) Complete(ctx context.Context, shared []SharedDialog, private []PrivateDialog) error {
	t.completed = true
	t.completeShared = len(shared)
	return nil
}

func (t *fakeTask) Retry(ctx context.Context, cause error, delay time.Duration) error { return nil }
func (t *fakeTask) Fail(ctx context.Context, cause error) error                       { return nil }

func (t *fakeTask) Release(ctx context.Context, cause error) error {
	t.released = true
	return nil
}
