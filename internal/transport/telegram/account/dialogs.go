package account

import (
	"context"
	"errors"

	gotdtelegram "github.com/gotd/td/telegram"

	"github.com/notrodans/cresora/internal/application/dialogsync"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/transport/telegram"
)

// DialogFetcher is the runtime-gated dialog fetch port. It runs the Telegram
// dialog fetch inside the account runtime admission so the gotd client never
// escapes a callback.
type DialogFetcher struct {
	runtime Runtime
}

var _ dialogsync.Fetcher = DialogFetcher{}

// NewDialogFetcher constructs a fetcher over the account runtime.
func NewDialogFetcher(runtime Runtime) DialogFetcher {
	return DialogFetcher{runtime: runtime}
}

// Fetch admits one dialog synchronization against the account and returns the
// transport-neutral dialog lists. Runtime admission rejections are transient;
// the classified dialog fetch errors pass through unchanged.
func (fetcher DialogFetcher) Fetch(
	ctx context.Context,
	target operatoraccounts.RuntimeTarget,
) ([]dialogsync.SharedDialog, []dialogsync.PrivateDialog, error) {
	var (
		shared  []dialogsync.SharedDialog
		private []dialogsync.PrivateDialog
	)
	failure := fetcher.runtime.Execute(ctx, target, func(
		callbackContext context.Context,
		client *gotdtelegram.Client,
	) error {
		var err error
		shared, private, err = telegram.FetchDialogs(callbackContext, client)
		return err
	})
	if failure != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if isRuntimeAdmissionRejection(failure) {
			return nil, nil, dialogsync.WrapTransient(failure)
		}
		if isContextFailure(failure) {
			return nil, nil, failure
		}
		return nil, nil, failure
	}
	return shared, private, nil
}

func isContextFailure(failure error) bool {
	return errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded)
}
