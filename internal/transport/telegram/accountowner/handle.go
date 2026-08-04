package accountowner

import (
	"context"
	"sync"

	"github.com/notrodans/cresora/internal/application/operatoraccounts"
)

// Handle is a scoped admission to one account runtime. It does not expose a
// gotd client. Call Execute for each logical operation and close the handle
// when the caller no longer needs to retain the admission.
type Handle struct {
	rentry *runtimeEntry
	target operatoraccounts.RuntimeTarget

	mu       sync.Mutex
	closed   bool
	uses     int
	released bool
}

// Execute reserves this handle for the duration of one operation. Close and
// Execute are linearized by handle.mu: Close rejects later operations, while an
// operation admitted first retains the underlying registry reference until it
// finishes.
func (handle *Handle) Execute(ctx context.Context, callback ClientCallback) error {
	if !handle.beginUse() {
		return ErrAccountStopped
	}
	defer handle.finishUse()
	return handle.rentry.execute(ctx, handle.target, callback)
}

// Close prevents new operations through this handle. The registry reference is
// released immediately when the handle is idle, or by the last operation that
// was admitted before Close.
func (handle *Handle) Close() error {
	handle.mu.Lock()
	if handle.closed {
		handle.mu.Unlock()
		return nil
	}
	handle.closed = true
	release := handle.uses == 0 && !handle.released
	if release {
		handle.released = true
	}
	handle.mu.Unlock()
	if release {
		handle.rentry.releaseRef()
	}
	return nil
}

func (handle *Handle) beginUse() bool {
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.closed {
		return false
	}
	handle.uses++
	return true
}

func (handle *Handle) finishUse() {
	handle.mu.Lock()
	handle.uses--
	release := handle.closed && handle.uses == 0 && !handle.released
	if release {
		handle.released = true
	}
	handle.mu.Unlock()
	if release {
		handle.rentry.releaseRef()
	}
}
