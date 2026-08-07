// Package dialogsync coordinates durable, background synchronization of a
// Telegram account's shared and private dialogs. It is transport neutral: the
// work is claimed from and finalized into a durable store, and the actual
// Telegram fetch is performed through a consumer-defined port that runs inside
// the account runtime admission.
package dialogsync

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrEmpty means no account dialog sync is currently claimable.
	ErrEmpty = errors.New("no ready account dialog syncs")
	// ErrLeaseLost means the claimed sync's lease or lifecycle snapshot is no
	// longer valid and the worker must stop this attempt immediately.
	ErrLeaseLost = errors.New("account dialog sync lease lost")
)

// PeerType is the Telegram peer category stored for private dialogs. It maps
// to the telegram_peer_type database enum.
type PeerType string

const (
	PeerUser    PeerType = "user"
	PeerChat    PeerType = "chat"
	PeerChannel PeerType = "channel"
)

// SharedDialogKind is the supergroup-or-channel kind of a shared dialog. It
// maps to the shared_dialog_kind database enum. Shared dialogs are only
// created for peers that Telegram represents as channels (supergroups and
// broadcast channels); users and basic groups are private dialogs.
type SharedDialogKind string

const (
	SharedSupergroup       SharedDialogKind = "supergroup"
	SharedBroadcastChannel SharedDialogKind = "broadcast_channel"
)

// SharedDialog is one sendable shared (supergroup/broadcast channel) dialog
// discovered in an account's dialog list.
type SharedDialog struct {
	PeerID       int64
	Kind         SharedDialogKind
	Title        string
	Username     string
	Participants *int
	AccessHash   *int64
}

// PrivateDialog is one per-account private dialog (user, basic chat, or
// channel) discovered in an account's dialog list.
type PrivateDialog struct {
	PeerType   PeerType
	PeerID     int64
	Title      string
	Username   string
	AccessHash *int64
}

// TaskKey identifies one claimed account dialog synchronization.
type TaskKey struct {
	// AccountID identifies the operator account whose dialogs are synchronized.
	AccountID uuid.UUID
	// OperatorID is the account owner's operator. It is captured with the claim
	// so the worker can reconstruct a runtime admission target.
	OperatorID uuid.UUID
	// Version is the account lifecycle version observed at claim time.
	Version int64
}

// SyncTimeout bounds one full dialog fetch attempt. It is deliberately short
// enough that renewal keeps the claim alive across pagination and flood waits
// without holding the lease indefinitely.
const SyncTimeout = 60 * time.Second
