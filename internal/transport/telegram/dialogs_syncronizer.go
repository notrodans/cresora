package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/notrodans/cresora/internal/application/dialogsync"
)

const (
	// dialogPageLimit is the number of dialogs requested per messages.getDialogs
	// page. Telegram caps the practical page size well above this value.
	dialogPageLimit = 100
	// dialogMaxPages bounds a single sync attempt so an account with an
	// unbounded dialog list cannot hold a lease forever. The attempt is
	// idempotent, so later scheduled syncs continue where the list ended.
	dialogMaxPages = 50
)

var (
	errUnexpectedDialogsResponse = errors.New("unexpected Telegram dialogs response")
	errDialogPeerMissing         = errors.New("Telegram dialog peer is missing")
)

// FetchDialogs reads the account's dialog list through its live gotd client
// and reduces it to transport-neutral shared and private dialogs. Only peers
// that Telegram represents as channels become shared dialogs (supergroup or
// broadcast channel); users and basic groups become private dialogs. No send
// permission is derived from the dialog list; every discovered dialog is
// stored with can_send = false.
func FetchDialogs(
	ctx context.Context,
	client *gotdtelegram.Client,
) ([]dialogsync.SharedDialog, []dialogsync.PrivateDialog, error) {
	if client == nil {
		return nil, nil, dialogsync.WrapPermanent(errors.New("telegram client is unavailable"))
	}

	var (
		shared  []dialogsync.SharedDialog
		private []dialogsync.PrivateDialog
	)

	offsetID := 0
	offsetPeer := tg.InputPeerClass(&tg.InputPeerEmpty{})
	for page := 0; page < dialogMaxPages; page++ {
		response, failure := client.API().MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			ExcludePinned: true,
			OffsetID:      offsetID,
			OffsetPeer:    offsetPeer,
			Limit:         dialogPageLimit,
		})
		if failure != nil {
			return nil, nil, classifyDialogFailure(failure)
		}

		pageDialogs, complete, next, failure := reduceDialogsPage(response)
		if failure != nil {
			return nil, nil, failure
		}
		shared = append(shared, pageDialogs.Shared...)
		private = append(private, pageDialogs.Private...)
		if complete {
			break
		}
		offsetID, offsetPeer = next.ID, next.Peer
	}

	return shared, private, nil
}

type dialogPage struct {
	Shared  []dialogsync.SharedDialog
	Private []dialogsync.PrivateDialog
}

type dialogOffset struct {
	ID   int
	Peer tg.InputPeerClass
}

func reduceDialogsPage(
	response tg.MessagesDialogsClass,
) (dialogPage, bool, dialogOffset, error) {
	var dialogs []tg.DialogClass
	var chats []tg.ChatClass
	var users []tg.UserClass
	count := 0
	switch page := response.(type) {
	case *tg.MessagesDialogs:
		dialogs = page.GetDialogs()
		chats = page.GetChats()
		users = page.GetUsers()
	case *tg.MessagesDialogsSlice:
		dialogs = page.GetDialogs()
		chats = page.GetChats()
		users = page.GetUsers()
		count = page.GetCount()
	case *tg.MessagesDialogsNotModified:
		return dialogPage{}, true, dialogOffset{}, nil
	default:
		return dialogPage{}, false, dialogOffset{}, fmt.Errorf("%w: %T", errUnexpectedDialogsResponse, response)
	}

	channels := make(map[int64]*tg.Channel)
	basicChats := make(map[int64]*tg.Chat)
	for _, candidate := range chats {
		switch chat := candidate.(type) {
		case *tg.Channel:
			channels[chat.ID] = chat
		case *tg.Chat:
			basicChats[chat.ID] = chat
		}
	}
	resolvedUsers := make(map[int64]*tg.User)
	for _, candidate := range users {
		if user, ok := candidate.(*tg.User); ok {
			resolvedUsers[user.ID] = user
		}
	}

	var pageDialogs dialogPage
	last := dialogOffset{}
	for index, candidate := range dialogs {
		dialog, ok := candidate.(*tg.Dialog)
		if !ok {
			continue
		}
		peer := dialog.GetPeer()
		if peer == nil {
			return dialogPage{}, false, dialogOffset{}, fmt.Errorf("%w: %T", errDialogPeerMissing, peer)
		}
		switch target := peer.(type) {
		case *tg.PeerUser:
			if user, found := resolvedUsers[target.UserID]; found {
				pageDialogs.Private = append(pageDialogs.Private, privateUserDialog(user))
			}
		case *tg.PeerChat:
			if chat, found := basicChats[target.ChatID]; found {
				pageDialogs.Private = append(pageDialogs.Private, privateChatDialog(chat))
			}
		case *tg.PeerChannel:
			if channel, found := channels[target.ChannelID]; found {
				pageDialogs.Shared = append(pageDialogs.Shared, sharedChannelDialog(channel))
			}
		}
		if index == len(dialogs)-1 {
			last = dialogOffset{ID: dialog.GetTopMessage(), Peer: inputPeer(dialog)}
		}
	}

	complete := len(dialogs) < dialogPageLimit || (count > 0 && len(dialogs) >= count)
	return pageDialogs, complete, last, nil
}

// inputPeer reconstructs the offset peer for the next messages.getDialogs page
// from the last dialog on the current page.
func inputPeer(dialog *tg.Dialog) tg.InputPeerClass {
	switch target := dialog.GetPeer().(type) {
	case *tg.PeerUser:
		return &tg.InputPeerUser{UserID: target.UserID}
	case *tg.PeerChat:
		return &tg.InputPeerChat{ChatID: target.ChatID}
	case *tg.PeerChannel:
		return &tg.InputPeerChannel{ChannelID: target.ChannelID}
	default:
		return &tg.InputPeerEmpty{}
	}
}

func privateUserDialog(user *tg.User) dialogsync.PrivateDialog {
	title := strings.TrimSpace(strings.Join([]string{user.FirstName, user.LastName}, " "))
	if title == "" {
		title = user.Username
	}
	return dialogsync.PrivateDialog{
		PeerType:   dialogsync.PeerUser,
		PeerID:     user.ID,
		Title:      title,
		Username:   user.Username,
		AccessHash: optionalInt64(user.AccessHash),
	}
}

func privateChatDialog(chat *tg.Chat) dialogsync.PrivateDialog {
	return dialogsync.PrivateDialog{
		PeerType: dialogsync.PeerChat,
		PeerID:   chat.ID,
		Title:    chat.Title,
	}
}

func sharedChannelDialog(channel *tg.Channel) dialogsync.SharedDialog {
	kind := dialogsync.SharedSupergroup
	if channel.Broadcast {
		kind = dialogsync.SharedBroadcastChannel
	}
	return dialogsync.SharedDialog{
		PeerID:       channel.ID,
		Kind:         kind,
		Title:        channel.Title,
		Username:     channel.Username,
		Participants: optionalInt(channel.ParticipantsCount),
		AccessHash:   optionalInt64(channel.AccessHash),
	}
}

func optionalInt(value int) *int {
	copy := value
	return &copy
}

func optionalInt64(value int64) *int64 {
	if value == 0 {
		return nil
	}
	copy := value
	return &copy
}

// classifyDialogFailure translates only errors returned by the generated
// Telegram dialog API into the application failure taxonomy. It never calls
// gotd's FloodWait helper because that helper sleeps inside the transport
// instead of exposing the server duration to the sync worker.
func classifyDialogFailure(failure error) error {
	if failure == nil {
		return nil
	}
	if duration, ok := tgerr.AsFloodWait(failure); ok {
		if duration <= 0 {
			return dialogsync.WrapTransient(fmt.Errorf("invalid Telegram dialog FloodWait duration: %w", failure))
		}
		return &dialogsync.FloodWaitError{Duration: duration}
	}
	rpcFailure, ok := tgerr.As(failure)
	if ok && rpcFailure != nil {
		switch {
		case tgerr.Is(failure,
			"AUTH_KEY_UNREGISTERED",
			"AUTH_KEY_INVALID",
			"SESSION_REVOKED",
			"SESSION_EXPIRED",
			"AUTH_KEY_DUPLICATED",
			"UNAUTHORIZED",
		):
			return dialogsync.WrapPermanent(failure)
		case rpcFailure.Code >= 500 && rpcFailure.Code < 600:
			return dialogsync.WrapTransient(failure)
		}
	}
	return dialogsync.WrapTransient(failure)
}
