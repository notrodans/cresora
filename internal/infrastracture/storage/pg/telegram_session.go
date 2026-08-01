package pg

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/transport/telegram"
)

const (
	telegramSessionPurpose       = "cresora/telegram-session"
	telegramSessionFormatVersion = 1
	telegramSessionMaxBytes      = 1 << 20
	telegramSessionKeyIDMaxBytes = 128
)

var _ telegram.SessionStore = &telegramSessionStore{}

// telegramSessionStore stores only the encrypted session envelope. The
// current key is intentionally the only key accepted: key rotation is not
// implemented yet, but every row records keyID so a future rotation can be
// explicit rather than silently reusing an identifier.
type telegramSessionStore struct {
	database *pgxpool.Pool
	keyID    string
	aead     cipher.AEAD
}

// NewTelegramSessionStore creates a PostgreSQL-backed encrypted Telegram
// session store. key must be exactly 32 bytes and is copied by AES-GCM during
// construction; the caller remains responsible for key lifecycle.
func NewTelegramSessionStore(
	database *pgxpool.Pool,
	keyID string,
	key []byte,
) (telegram.SessionStore, error) {
	return newTelegramSessionStore(database, keyID, key)
}

func newTelegramSessionStore(
	database *pgxpool.Pool,
	keyID string,
	key []byte,
) (telegram.SessionStore, error) {
	if err := validateTelegramSessionKeyID(keyID); err != nil {
		return nil, err
	}
	if len(key) != aesKeySize {
		return nil, errors.New("create telegram session store: encryption key must be exactly 32 bytes")
	}
	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("create telegram session store: initialize encryption")
	}
	aead, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return nil, errors.New("create telegram session store: initialize authenticated encryption")
	}
	return &telegramSessionStore{
		database: database,
		keyID:    keyID,
		aead:     aead,
	}, nil
}

const aesKeySize = 32

func (store *telegramSessionStore) Load(
	context context.Context,
	scope telegram.SessionScope,
) (telegram.Session, error) {
	if !validSessionScope(scope) {
		return telegram.Session{}, fmt.Errorf("load telegram session: %w", telegram.ErrSessionInvalid)
	}

	var (
		formatVersion int4Nullable
		keyID         pgtype.Text
		nonce         bytesNullable
		ciphertext    bytesNullable
	)
	failure := store.database.QueryRow(
		context,
		`SELECT session.format_version,
		        session.key_id,
		        session.nonce,
		        session.ciphertext
		 FROM operator_accounts AS account
		 LEFT JOIN sessions AS session ON session.account_id = account.id
		 WHERE account.id = $2
		   AND account.operator_id = $1`,
		scope.OperatorID,
		scope.AccountID,
	).Scan(&formatVersion, &keyID, &nonce, &ciphertext)
	if errors.Is(failure, pgx.ErrNoRows) {
		// Unknown accounts and accounts owned by another operator intentionally
		// have the same result and error.
		return telegram.Session{}, telegram.ErrSessionUnauthorized
	}
	if failure != nil {
		return telegram.Session{}, fmt.Errorf("load telegram session: %w", failure)
	}
	if !formatVersion.Valid {
		if keyID.Valid || nonce.Valid || ciphertext.Valid {
			return telegram.Session{}, fmt.Errorf("load telegram session envelope: %w", telegram.ErrSessionInvalid)
		}
		return telegram.Session{}, nil
	}
	if !keyID.Valid || !nonce.Valid || !ciphertext.Valid {
		return telegram.Session{}, fmt.Errorf("load telegram session envelope: %w", telegram.ErrSessionInvalid)
	}

	plaintext, failure := decryptTelegramSession(
		store.aead,
		store.keyID,
		scope,
		formatVersion.Value,
		keyID.String,
		nonce.Bytes,
		ciphertext.Bytes,
	)
	if failure != nil {
		return telegram.Session{}, fmt.Errorf("load telegram session: %w", failure)
	}
	return telegram.Session{Bytes: plaintext, Present: true}, nil
}

func (store *telegramSessionStore) Store(
	context context.Context,
	scope telegram.SessionScope,
	plaintext []byte,
) error {
	if !validSessionScope(scope) {
		return fmt.Errorf("store telegram session: %w", telegram.ErrSessionInvalid)
	}
	nonce, ciphertext, failure := encryptTelegramSession(store.aead, store.keyID, scope, plaintext)
	if failure != nil {
		return fmt.Errorf("store telegram session: %w", failure)
	}

	transaction, failure := store.database.Begin(context)
	if failure != nil {
		return fmt.Errorf("store telegram session: begin transaction: %w", failure)
	}
	defer func() { _ = transaction.Rollback(context) }()

	var status string
	failure = transaction.QueryRow(
		context,
		`SELECT account.status::text
		 FROM operator_accounts AS account
		 WHERE account.id = $2
		   AND account.operator_id = $1
		 FOR UPDATE`,
		scope.OperatorID,
		scope.AccountID,
	).Scan(&status)
	if errors.Is(failure, pgx.ErrNoRows) {
		// Unknown accounts, foreign accounts, and accounts that cannot own a
		// session intentionally have the same transport error.
		return telegram.ErrSessionUnauthorized
	}
	if failure != nil {
		return fmt.Errorf("store telegram session: check account: %w", failure)
	}
	if !validTelegramSessionAccountStatus(status) {
		return telegram.ErrSessionUnauthorized
	}

	_, failure = transaction.Exec(
		context,
		`INSERT INTO sessions (
			account_id,
			format_version,
			key_id,
			nonce,
			ciphertext
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_id) DO UPDATE
		SET format_version = EXCLUDED.format_version,
		    key_id = EXCLUDED.key_id,
		    nonce = EXCLUDED.nonce,
		    ciphertext = EXCLUDED.ciphertext,
		    updated_at = CURRENT_TIMESTAMP`,
		scope.AccountID,
		telegramSessionFormatVersion,
		store.keyID,
		nonce,
		ciphertext,
	)
	if failure != nil {
		return fmt.Errorf("store telegram session: %w", failure)
	}
	if failure = transaction.Commit(context); failure != nil {
		return fmt.Errorf("store telegram session: commit transaction: %w", failure)
	}
	return nil
}

func validateTelegramSessionKeyID(keyID string) error {
	if keyID == "" || keyID != strings.TrimSpace(keyID) || len(keyID) > telegramSessionKeyIDMaxBytes {
		return errors.New("create telegram session store: invalid session key ID")
	}
	for _, character := range keyID {
		if character < 0x20 || character == 0x7f {
			return errors.New("create telegram session store: invalid session key ID")
		}
	}
	return nil
}

func validSessionScope(scope telegram.SessionScope) bool {
	return scope.OperatorID != uuid.Nil && scope.AccountID != uuid.Nil
}

func validTelegramSessionAccountStatus(status string) bool {
	return status == "authenticating" || status == "active"
}

func encryptTelegramSession(
	aead cipher.AEAD,
	keyID string,
	scope telegram.SessionScope,
	plaintext []byte,
) ([]byte, []byte, error) {
	if len(plaintext) > telegramSessionMaxBytes {
		return nil, nil, telegram.ErrSessionTooLarge
	}
	if !validSessionScope(scope) {
		return nil, nil, telegram.ErrSessionInvalid
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, errors.New("generate telegram session nonce")
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, telegramSessionAAD(keyID, scope, telegramSessionFormatVersion))
	return nonce, ciphertext, nil
}

func decryptTelegramSession(
	aead cipher.AEAD,
	currentKeyID string,
	scope telegram.SessionScope,
	formatVersion int32,
	storedKeyID string,
	nonce []byte,
	ciphertext []byte,
) ([]byte, error) {
	if !validSessionScope(scope) || formatVersion != telegramSessionFormatVersion || storedKeyID != currentKeyID {
		return nil, telegram.ErrSessionInvalid
	}
	if len(nonce) != aead.NonceSize() || len(ciphertext) < aead.Overhead() || len(ciphertext) > telegramSessionMaxBytes+aead.Overhead() {
		return nil, telegram.ErrSessionInvalid
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, telegramSessionAAD(storedKeyID, scope, formatVersion))
	if err != nil {
		return nil, telegram.ErrSessionCorrupt
	}
	if len(plaintext) > telegramSessionMaxBytes {
		return nil, telegram.ErrSessionInvalid
	}
	return plaintext, nil
}

func telegramSessionAAD(keyID string, scope telegram.SessionScope, formatVersion int32) []byte {
	// Length-prefixing the non-fixed key ID keeps the AAD unambiguous while
	// binding every identity and envelope discriminator to the ciphertext.
	aad := make([]byte, 0, len(telegramSessionPurpose)+1+4+4+len(keyID)+32)
	aad = append(aad, telegramSessionPurpose...)
	var integer [4]byte
	binary.BigEndian.PutUint32(integer[:], uint32(formatVersion))
	aad = append(aad, integer[:]...)
	binary.BigEndian.PutUint32(integer[:], uint32(len(keyID)))
	aad = append(aad, integer[:]...)
	aad = append(aad, keyID...)
	aad = append(aad, scope.OperatorID[:]...)
	aad = append(aad, scope.AccountID[:]...)
	return aad
}

// int4Nullable avoids treating a NULL format version from the ownership
// LEFT JOIN as a valid zero-value envelope.
type int4Nullable struct {
	Value int32
	Valid bool
}

type bytesNullable struct {
	Bytes []byte
	Valid bool
}

func (value *bytesNullable) Scan(source any) error {
	if source == nil {
		value.Bytes = nil
		value.Valid = false
		return nil
	}
	bytes, ok := source.([]byte)
	if !ok {
		return fmt.Errorf("scan bytea: expected bytes")
	}
	value.Bytes = append(value.Bytes[:0], bytes...)
	value.Valid = true
	return nil
}

func (value *int4Nullable) Scan(source any) error {
	if source == nil {
		value.Value = 0
		value.Valid = false
		return nil
	}
	var scanned pgtype.Int4
	if err := scanned.Scan(source); err != nil {
		return err
	}
	value.Value = scanned.Int32
	value.Valid = scanned.Valid
	return nil
}
