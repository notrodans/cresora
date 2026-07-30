package delivery

import (
	"context"

	"github.com/notrodans/nebula-go/internal/domain/mailing"
)

// DeliveryState is the persisted state of one logical delivery.  The
// reconciler intentionally knows only about delivery outcomes; it does not
// know anything about a transport.
type DeliveryState string

const (
	DeliveryPending DeliveryState = "pending"
	DeliverySending DeliveryState = "sending"
	DeliverySent    DeliveryState = "sent"
	DeliverySkipped DeliveryState = "skipped"
	DeliveryFailed  DeliveryState = "failed"
	DeliveryUnknown DeliveryState = "unknown"
)

// RunTerminalStatus is the only run-level state that reconciliation can
// produce.  Cancelled is deliberately absent: Stop owns cancellation and a
// reconciler must never reopen or rewrite a cancelled run.
type RunTerminalStatus string

const (
	RunCompleted RunTerminalStatus = "completed"
	RunFailed    RunTerminalStatus = "failed"
)

// ReconciliationCandidate identifies a concrete mailing run, including its
// execution generation fence.  MailingID is part of the identity because the
// database primary key for a delivery/run relationship is composite.
type ReconciliationCandidate struct {
	MailingID           mailing.ID
	RunID               mailing.RunID
	ExecutionGeneration int64
}

// ReconciliationResult reports one bounded database pass.  Candidates is the
// number discovered by the bounded scan; Completed and Failed count successful
// terminal transitions.  A candidate can be discovered and then skipped when
// Stop, the reaper, a classifier, or another reconciler wins a race.
type ReconciliationResult struct {
	Candidates int
	Completed  int
	Failed     int
}

// RunReconciler is the consumer-owned application port for one transport-free
// terminal run reconciliation pass.
type RunReconciler interface {
	Reconcile(context.Context) (ReconciliationResult, error)
}

// TerminalRunStatus computes the terminal result for a delivery set.  Pending
// and sending are not terminal and therefore prevent a transition.  Unknown
// is treated as a failed outcome (and has precedence while scanning), while
// skipped deliveries are neutral because they are Stop semantics.
func TerminalRunStatus(states []DeliveryState) (RunTerminalStatus, bool) {
	if len(states) == 0 {
		return "", false
	}

	failed := false
	unknown := false
	for _, state := range states {
		switch state {
		case DeliveryPending, DeliverySending:
			return "", false
		case DeliveryUnknown:
			unknown = true
		case DeliveryFailed:
			failed = true
		case DeliverySent, DeliverySkipped:
			// Sent is successful and skipped is Stop semantics.
		default:
			// A state introduced by a newer schema is not safe to
			// interpret as terminal until this consumer understands it.
			return "", false
		}
	}
	if unknown || failed {
		return RunFailed, true
	}
	return RunCompleted, true
}
