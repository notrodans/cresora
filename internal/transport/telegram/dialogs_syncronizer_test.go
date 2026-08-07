package telegram

import (
	"testing"

	"github.com/gotd/td/tg"

	"github.com/notrodans/cresora/internal/application/dialogsync"
)

func TestReduceDialogsPageClassifiesSharedAndPrivateDialogs(t *testing.T) {
	response := &tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			&tg.Dialog{Peer: &tg.PeerChannel{ChannelID: 1}},
			&tg.Dialog{Peer: &tg.PeerChannel{ChannelID: 2}},
			&tg.Dialog{Peer: &tg.PeerUser{UserID: 5}},
			&tg.Dialog{Peer: &tg.PeerChat{ChatID: 9}},
		},
		Chats: []tg.ChatClass{
			&tg.Channel{
				ID:                1,
				Broadcast:         true,
				Title:             "News Channel",
				Username:          "news",
				ParticipantsCount: 100,
				AccessHash:        11,
			},
			&tg.Channel{
				ID:                2,
				Megagroup:         true,
				Title:             "Super Group",
				ParticipantsCount: 50,
			},
			&tg.Chat{ID: 9, Title: "Basic Group"},
		},
		Users: []tg.UserClass{
			&tg.User{ID: 5, FirstName: "Alice", LastName: "Doe", Username: "alice", AccessHash: 7},
		},
	}

	result, complete, _, failure := reduceDialogsPage(response)
	if failure != nil {
		t.Fatalf("reduce dialogs page: %v", failure)
	}
	if !complete {
		t.Fatalf("complete = false, want true for a non-paginated messages.dialogs result")
	}
	if len(result.Shared) != 2 {
		t.Fatalf("shared dialog count = %d, want 2", len(result.Shared))
	}
	if len(result.Private) != 2 {
		t.Fatalf("private dialog count = %d, want 2", len(result.Private))
	}

	if got := result.Shared[0]; !(got.PeerID == 1 && got.Kind == dialogsync.SharedBroadcastChannel && got.Title == "News Channel" && got.Username == "news") {
		t.Fatalf("broadcast channel = %+v, want broadcast channel #1", got)
	}
	if got := result.Shared[1]; !(got.PeerID == 2 && got.Kind == dialogsync.SharedSupergroup) {
		t.Fatalf("supergroup = %+v, want supergroup #2", got)
	}
	if got := result.Private[0]; !(got.PeerType == dialogsync.PeerUser && got.PeerID == 5 && got.Title == "Alice Doe") {
		t.Fatalf("private user = %+v, want Alice Doe #5", got)
	}
	if got := result.Private[1]; !(got.PeerType == dialogsync.PeerChat && got.PeerID == 9 && got.Title == "Basic Group") {
		t.Fatalf("private chat = %+v, want Basic Group #9", got)
	}
}

func TestReduceDialogsPageNotModifiedIsEmpty(t *testing.T) {
	page, complete, _, failure := reduceDialogsPage(&tg.MessagesDialogsNotModified{})
	if failure != nil {
		t.Fatalf("reduce not-modified page: %v", failure)
	}
	if !complete {
		t.Fatalf("complete = false, want true for not-modified")
	}
	if len(page.Shared) != 0 || len(page.Private) != 0 {
		t.Fatalf("not-modified page returned dialogs: shared=%d private=%d", len(page.Shared), len(page.Private))
	}
}

func TestInputPeerNeverReturnsNil(t *testing.T) {
	// messages.getDialogs requires a non-nil offset peer; the fallback must be
	// InputPeerEmpty so a page of unrecognized peers cannot break the next page.
	offset := inputPeer(&tg.Dialog{})
	if offset == nil {
		t.Fatalf("input peer is nil, want non-nil fallback")
	}
	if _, ok := offset.(*tg.InputPeerEmpty); !ok {
		t.Fatalf("fallback peer = %T, want *tg.InputPeerEmpty", offset)
	}
}
