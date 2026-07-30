package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/notrodans/cresora/internal/transport/telegram"
)

type fakeQueryRowDatabase struct {
	row       pgx.Row
	query     string
	arguments []any
}

func (database *fakeQueryRowDatabase) QueryRow(
	context context.Context,
	query string,
	arguments ...any,
) pgx.Row {
	database.query = query
	database.arguments = arguments
	return database.row
}

type fakeQueryRow func(...any) error

func (row fakeQueryRow) Scan(destinations ...any) error {
	return row(destinations...)
}

func TestTelegramPeerLookupMapsProjection(t *testing.T) {
	tests := []struct {
		name       string
		peerType   string
		peerID     int64
		accessHash *int64
	}{
		{
			name:       "user with hash",
			peerType:   "user",
			peerID:     101,
			accessHash: int64Pointer(201),
		},
		{
			name:     "chat without hash",
			peerType: "chat",
			peerID:   301,
		},
		{
			name:       "channel with hash",
			peerType:   "channel",
			peerID:     401,
			accessHash: int64Pointer(501),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := &fakeQueryRowDatabase{
				row: fakeQueryRow(func(destinations ...any) error {
					*destinations[0].(*string) = test.peerType
					*destinations[1].(*int64) = test.peerID
					hash := destinations[2].(*pgtype.Int8)
					if test.accessHash == nil {
						*hash = pgtype.Int8{}
					} else {
						*hash = pgtype.Int8{Int64: *test.accessHash, Valid: true}
					}
					return nil
				}),
			}
			lookup := newTelegramPeerLookup(database)
			request := telegram.PeerLookupRequest{
				AccountID:   uuid.New(),
				RecipientID: uuid.New(),
			}

			projection, failure := lookup.Lookup(context.Background(), request)
			if failure != nil {
				t.Fatalf("lookup peer: %v", failure)
			}
			if projection.Type != telegram.PeerType(test.peerType) {
				t.Fatalf("expected peer type %q, got %q", test.peerType, projection.Type)
			}
			if projection.ID != test.peerID {
				t.Fatalf("expected peer ID %d, got %d", test.peerID, projection.ID)
			}
			if !equalInt64Pointers(projection.AccessHash, test.accessHash) {
				t.Fatalf("expected access hash %v, got %v", test.accessHash, projection.AccessHash)
			}
			if len(database.arguments) != 2 || database.arguments[0] != request.RecipientID || database.arguments[1] != request.AccountID {
				t.Fatalf("unexpected lookup arguments: %#v", database.arguments)
			}
		})
	}
}

func TestTelegramPeerLookupMapsNoRows(t *testing.T) {
	database := &fakeQueryRowDatabase{
		row: fakeQueryRow(func(...any) error {
			return pgx.ErrNoRows
		}),
	}
	lookup := newTelegramPeerLookup(database)

	_, failure := lookup.Lookup(context.Background(), telegram.PeerLookupRequest{
		AccountID:   uuid.New(),
		RecipientID: uuid.New(),
	})

	if !errors.Is(failure, telegram.ErrTargetNotFound) {
		t.Fatalf("expected target not found error, got %v", failure)
	}
}

func TestTelegramPeerLookupWrapsQueryErrors(t *testing.T) {
	expected := errors.New("query failed")
	database := &fakeQueryRowDatabase{
		row: fakeQueryRow(func(...any) error {
			return expected
		}),
	}
	lookup := newTelegramPeerLookup(database)

	_, failure := lookup.Lookup(context.Background(), telegram.PeerLookupRequest{
		AccountID:   uuid.New(),
		RecipientID: uuid.New(),
	})

	if !errors.Is(failure, expected) {
		t.Fatalf("expected query error to be preserved, got %v", failure)
	}
}

func TestTelegramPeerLookupGuards(t *testing.T) {
	validRequest := telegram.PeerLookupRequest{
		AccountID:   uuid.New(),
		RecipientID: uuid.New(),
	}
	validDatabase := &fakeQueryRowDatabase{
		row: fakeQueryRow(func(...any) error {
			return nil
		}),
	}

	tests := []struct {
		name string
		call func()
	}{
		{
			name: "nil context",
			call: func() {
				newTelegramPeerLookup(validDatabase).Lookup(nil, validRequest)
			},
		},
		{
			name: "nil database",
			call: func() {
				NewTelegramPeerLookup(nil).Lookup(context.Background(), validRequest)
			},
		},
		{
			name: "zero account",
			call: func() {
				request := validRequest
				request.AccountID = uuid.Nil
				newTelegramPeerLookup(validDatabase).Lookup(context.Background(), request)
			},
		},
		{
			name: "zero recipient",
			call: func() {
				request := validRequest
				request.RecipientID = uuid.Nil
				newTelegramPeerLookup(validDatabase).Lookup(context.Background(), request)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertPanics(t, test.call)
		})
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func equalInt64Pointers(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func assertPanics(t *testing.T, call func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	call()
}
