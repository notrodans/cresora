package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestSessionEncryptionKeyUnmarshalText(t *testing.T) {
	keyBytes := []byte("01234567890123456789012345678901")
	var key SessionEncryptionKey
	if err := key.UnmarshalText([]byte(base64.StdEncoding.EncodeToString(keyBytes))); err != nil {
		t.Fatalf("unmarshal session encryption key: %v", err)
	}
	if !key.Configured() {
		t.Fatal("expected session encryption key to be configured")
	}
	if got := key.Bytes(); string(got) != string(keyBytes) {
		t.Fatalf("decoded key = %q, want %q", got, keyBytes)
	}
	got := key.Bytes()
	got[0] = 'x'
	if key.Bytes()[0] != keyBytes[0] {
		t.Fatal("Bytes returned the internal key storage")
	}
	if key.String() != "[redacted]" {
		t.Fatalf("String() = %q, want redacted value", key.String())
	}
}

func TestSessionEncryptionKeyRejectsMalformedOrWrongSizeWithoutSecret(t *testing.T) {
	secret := "this-is-not-a-key-secret"
	for _, input := range []string{
		secret,
		base64.StdEncoding.EncodeToString([]byte("too short")),
		base64.StdEncoding.EncodeToString(make([]byte, 33)),
	} {
		var key SessionEncryptionKey
		err := key.UnmarshalText([]byte(input))
		if err == nil {
			t.Fatalf("unmarshal %q succeeded, want error", input)
		}
		if strings.Contains(err.Error(), input) || strings.Contains(err.Error(), secret) {
			t.Fatalf("error %q contains secret input", err)
		}
		if !strings.Contains(err.Error(), telegramSessionEncryptionKeyEnv) {
			t.Fatalf("error %q does not name %s", err, telegramSessionEncryptionKeyEnv)
		}
	}
}

func TestLoadFromAllowsTelegramSessionConfigurationToBeAbsent(t *testing.T) {
	setRequiredEnvironment(t)
	config, err := loadFrom(t.TempDir())
	if err != nil {
		t.Fatalf("load configuration without Telegram session key: %v", err)
	}
	if config.TelegramSessionEncryptionKey.Configured() {
		t.Fatal("expected absent Telegram session key to remain unconfigured")
	}
}

func TestLoadFromRequiresTelegramSessionKeyPairWhenConfigured(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(telegramSessionKeyIDEnv, "current")

	_, err := loadFrom(t.TempDir())
	if err == nil {
		t.Fatal("load configuration succeeded without the configured session key")
	}
	if !strings.Contains(err.Error(), telegramSessionEncryptionKeyEnv) {
		t.Fatalf("error %q does not name %s", err, telegramSessionEncryptionKeyEnv)
	}
}

func TestValidateTelegramSessionConfigurationRejectsInvalidKeyID(t *testing.T) {
	key := SessionEncryptionKey{}
	if err := key.UnmarshalText([]byte(base64.StdEncoding.EncodeToString(make([]byte, 32)))); err != nil {
		t.Fatalf("create test key: %v", err)
	}
	for _, keyID := range []string{" current", strings.Repeat("x", telegramSessionKeyIDMaxLength+1), "bad\nkey"} {
		err := validateTelegramSessionConfiguration(Config{
			TelegramSessionKeyID:         keyID,
			TelegramSessionEncryptionKey: key,
		})
		if err == nil {
			t.Fatalf("validate key ID %q succeeded, want error", keyID)
		}
		if strings.Contains(err.Error(), keyID) {
			t.Fatalf("error %q contains key ID value", err)
		}
	}
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("ENV", string(Testing))
	t.Setenv("DB_URL", "postgres://example.invalid/nebula")
	t.Setenv("OPERATOR_ID", "11111111-1111-4111-8111-111111111111")
	t.Setenv("WEB_ADDR", "http://127.0.0.1:8080")
	t.Setenv("PUBLIC_ORIGIN", "http://127.0.0.1:8080")
}
