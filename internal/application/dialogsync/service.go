package dialogsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// Syncer runs one claimed synchronization end to end: it revalidates the
// account snapshot, fetches dialogs through the consumer-defined Fetcher, and
// finalizes the outcome. Failure classification and the retry budget are owned
// by the worker, which reads the classified result from the returned error.
type Syncer struct {
	fetch Fetcher
}

// NewSyncer constructs a dialog sync executor over one transport Fetcher.
func NewSyncer(fetch Fetcher) Syncer {
	return Syncer{fetch: fetch}
}

// Sync performs the shared and private dialog fetch for one claimed sync. A
// nil error means the sync was completed. A classified error (FloodWaitError,
// ErrPermanent, or ErrTransient) means the attempt failed and the worker must
// persist a retry or terminal state.
func (s Syncer) Sync(ctx context.Context, task Task) error {
	target, failure := task.Revalidate(ctx)
	if failure != nil {
		if errors.Is(failure, operatoraccounts.ErrAccountNotFound) {
			_ = task.Release(ctx, failure)
			return nil
		}
		return fmt.Errorf("revalidate account dialog sync: %w", failure)
	}

	shared, private, err := s.fetch.Fetch(ctx, target)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if failure := task.Complete(ctx, shared, private); failure != nil {
		return fmt.Errorf("complete account dialog sync: %w", failure)
	}
	return nil
}
