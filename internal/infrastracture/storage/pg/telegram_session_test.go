package pg

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/notrodans/nebula-go/internal/transport/telegram"
)

func TestEncryptTelegramSessionRandomizesNonce(t *testing.T) {
	aead := testSessionAEAD(t)
	scope := testSessionScope()
	plaintext := []byte("opaque gotd session")

	nonceA, ciphertextA, err := encryptTelegramSession(aead, "current", scope, plaintext)
	if err != nil {
		t.Fatalf("encrypt first session: %v", err)
	}
	nonceB, ciphertextB, err := encryptTelegramSession(aead, "current", scope, plaintext)
	if err != nil {
		t.Fatalf("encrypt second session: %v", err)
	}
	if bytes.Equal(nonceA, nonceB) {
		t.Fatal("two stores reused the AES-GCM nonce")
	}
	if bytes.Equal(ciphertextA, ciphertextB) {
		t.Fatal("two stores produced identical ciphertext")
	}
	for name, encrypted := range map[string]struct {
		nonce      []byte
		ciphertext []byte
	}{
		"first":  {nonce: nonceA, ciphertext: ciphertextA},
		"second": {nonce: nonceB, ciphertext: ciphertextB},
	} {
		decrypted, err := decryptTelegramSession(aead, "current", scope, telegramSessionFormatVersion, "current", encrypted.nonce, encrypted.ciphertext)
		if err != nil {
			t.Fatalf("decrypt %s session: %v", name, err)
		}
		if !bytes.Equal(decrypted, plaintext) {
			t.Fatalf("decrypted %s session = %q, want %q", name, decrypted, plaintext)
		}
	}
}

func TestDecryptTelegramSessionBindsAADToEnvelopeAndScope(t *testing.T) {
	aead := testSessionAEAD(t)
	scope := testSessionScope()
	nonce, ciphertext, err := encryptTelegramSession(aead, "current", scope, []byte("session secret"))
	if err != nil {
		t.Fatalf("encrypt session: %v", err)
	}
	tests := []struct {
		name          string
		keyID         string
		scope         telegram.SessionScope
		formatVersion int32
		wantError     error
	}{
		{
			name:      "operator",
			scope:     telegram.SessionScope{OperatorID: uuid.New(), AccountID: scope.AccountID},
			keyID:     "current",
			wantError: telegram.ErrSessionCorrupt,
		},
		{
			name:      "account",
			scope:     telegram.SessionScope{OperatorID: scope.OperatorID, AccountID: uuid.New()},
			keyID:     "current",
			wantError: telegram.ErrSessionCorrupt,
		},
		{
			name:      "key ID",
			scope:     scope,
			keyID:     "previous",
			wantError: telegram.ErrSessionInvalid,
		},
		{
			name:          "format version",
			scope:         scope,
			keyID:         "current",
			formatVersion: telegramSessionFormatVersion + 1,
			wantError:     telegram.ErrSessionInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			formatVersion := test.formatVersion
			if formatVersion == 0 {
				formatVersion = telegramSessionFormatVersion
			}
			_, err := decryptTelegramSession(aead, "current", test.scope, formatVersion, test.keyID, nonce, ciphertext)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("decrypt altered %s AAD: error = %v, want %v", test.name, err, test.wantError)
			}
		})
	}
}

func TestDecryptTelegramSessionRejectsMalformedEnvelope(t *testing.T) {
	aead := testSessionAEAD(t)
	scope := testSessionScope()
	nonce, ciphertext, err := encryptTelegramSession(aead, "current", scope, []byte("session"))
	if err != nil {
		t.Fatalf("encrypt session: %v", err)
	}
	tests := []struct {
		name       string
		nonce      []byte
		ciphertext []byte
	}{
		{name: "short nonce", nonce: nonce[:len(nonce)-1], ciphertext: ciphertext},
		{name: "short ciphertext", nonce: nonce, ciphertext: ciphertext[:aead.Overhead()-1]},
		{name: "oversize ciphertext", nonce: nonce, ciphertext: make([]byte, telegramSessionMaxBytes+aead.Overhead()+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decryptTelegramSession(aead, "current", scope, telegramSessionFormatVersion, "current", test.nonce, test.ciphertext)
			if !errors.Is(err, telegram.ErrSessionInvalid) {
				t.Fatalf("decrypt malformed envelope: error = %v, want %v", err, telegram.ErrSessionInvalid)
			}
		})
	}
}

func TestEncryptTelegramSessionEnforcesSizeBoundary(t *testing.T) {
	aead := testSessionAEAD(t)
	scope := testSessionScope()
	boundary := bytes.Repeat([]byte{'x'}, telegramSessionMaxBytes)
	if _, _, err := encryptTelegramSession(aead, "current", scope, boundary); err != nil {
		t.Fatalf("encrypt maximum-sized session: %v", err)
	}
	_, _, err := encryptTelegramSession(aead, "current", scope, append(boundary, 'x'))
	if !errors.Is(err, telegram.ErrSessionTooLarge) {
		t.Fatalf("encrypt oversized session: error = %v, want %v", err, telegram.ErrSessionTooLarge)
	}
	if strings.Contains(err.Error(), "x") {
		t.Fatalf("oversize error contains session data: %v", err)
	}
}

func TestValidateTelegramSessionKeyIDDoesNotRevealValue(t *testing.T) {
	secretKeyID := "key-id-that-must-not-be-in-error"
	err := validateTelegramSessionKeyID(" " + secretKeyID)
	if err == nil {
		t.Fatal("validate invalid key ID succeeded")
	}
	if strings.Contains(err.Error(), secretKeyID) {
		t.Fatalf("key ID error contains value: %v", err)
	}
}

func testSessionAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher([]byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatalf("create test AES cipher: %v", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("create test GCM: %v", err)
	}
	return aead
}

func testSessionScope() telegram.SessionScope {
	return telegram.SessionScope{OperatorID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), AccountID: uuid.MustParse("22222222-2222-4222-8222-222222222222")}
}
