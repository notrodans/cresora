package operatoraccountauth

import (
	"context"
	"time"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

// PhoneProvider is the complete phone-auth transport port. Each method is one
// independently admitted, sequential runtime operation:
//
//   - SendCode sends Telegram's code and returns its opaque in-memory hash.
//   - SignIn submits the code and calls Self before returning a profile.
//   - Password submits the 2FA password and calls Self before returning a
//     profile.
//
// No method waits for a later HTTP operation, and no gotd type crosses this
// boundary. Implementations must not return vendor errors, raw errors, or
// provider error strings. They may return only application sentinels, a
// validated ProviderFailureError, or a validated *RetryAfterError. The
// provider must scope every call to the supplied actor, account, and observed
// lifecycle version.
type PhoneProvider interface {
	SendCode(context.Context, AuthTarget, string) (SendCodeResult, error)
	SignIn(context.Context, AuthTarget, string, string, PhoneCodeHash) (Profile, error)
	Password(context.Context, AuthTarget, string) (Profile, error)
}

// AccountBeginner admits a phone-auth attempt durably before SendCode. A new
// account returns BeginStarted; disconnected and reauthentication-required
// accounts transition and return BeginResumed; an existing authenticating row
// returns its unmutated current result as BeginInProgress; and an active
// account returns BeginAlreadyActive without an error or provider call.
type AccountBeginner interface {
	BeginOrResume(context.Context, applicationroot.Actor, string, time.Time) (BeginResult, error)
}

// AccountFinalizer atomically records the profile and activates the account.
// Implementations must condition on actor ownership, authenticating status,
// expectedVersion, and the presence of the scoped persisted session. A
// duplicate completion for the same successful version returns the existing
// same-user result; stale or foreign writes return a semantic conflict.
type AccountFinalizer interface {
	Finalize(context.Context, applicationroot.Actor, operatoraccount.ID, operatoraccount.Version, Profile) (Account, error)
}

// AccountAborter exposes the two durable halves of abort. BeginAbort moves an
// authenticating row to disconnecting and returns the new version. The caller
// must stop the scoped runtime before CompleteAbort moves that exact
// disconnecting version to disconnected and removes the session transactionally.
type AccountAborter interface {
	BeginAbort(context.Context, applicationroot.Actor, operatoraccount.ID, operatoraccount.Version) (operatoraccount.Version, error)
	CompleteAbort(context.Context, applicationroot.Actor, operatoraccount.ID, operatoraccount.Version) error
}

// ActorAccountLister returns only accounts owned by the actor. Status is
// assembled by the application from these durable accounts and coordinator
// state; persistence never returns process-local challenges.
type ActorAccountLister interface {
	ListAccounts(context.Context, applicationroot.Actor) ([]Account, error)
}

// OrphanAuthenticationLifecycleLister is required to list every durable
// authentication lifecycle left without a process-local runtime owner after
// a restart. Candidates may be authenticating or disconnecting and carry that
// operatoraccount.Status in AuthTarget. It only lists candidates; it does not
// claim or mutate them. Authenticating candidates are passed through
// BeginAbort, RuntimeStopper, and CompleteAbort; disconnecting candidates are
// passed through RuntimeStopper and CompleteAbort.
type OrphanAuthenticationLifecycleLister interface {
	ListOrphanAuthenticationLifecycles(context.Context) ([]AuthTarget, error)
}

// RuntimeStopper is the application-owned runtime lifecycle port. It is
// intentionally separate from persistence so A1 can enforce the order
// BeginAbort -> StopAccount -> CompleteAbort.
type RuntimeStopper interface {
	StopAccount(context.Context, AuthTarget) error
}

// AuthenticationPersistence is the complete persistence port consumed by the
// phone-auth application service. Its operations are intentionally explicit;
// there is no generic account update method or monolithic abort.
type AuthenticationPersistence interface {
	AccountBeginner
	AccountFinalizer
	AccountAborter
	ActorAccountLister
	OrphanAuthenticationLifecycleLister
}
