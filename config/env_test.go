package config

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"
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
	if formatted := fmt.Sprintf("%s", key); strings.Contains(formatted, string(keyBytes)) {
		t.Fatalf("formatted key exposed secret: %q", formatted)
	}
}

func TestSecretStringRedactsFormattingAndLogging(t *testing.T) {
	secretValue := "telegram-api-hash-secret"
	var secret SecretString
	if err := secret.UnmarshalText([]byte(secretValue)); err != nil {
		t.Fatalf("unmarshal secret: %v", err)
	}

	for _, formatted := range []string{
		secret.String(),
		fmt.Sprintf("%v", secret),
		fmt.Sprintf("%+v", secret),
		fmt.Sprintf("%#v", secret),
		fmt.Sprintf("%s", secret),
		fmt.Sprintf("%q", secret),
	} {
		if strings.Contains(formatted, secretValue) {
			t.Fatalf("formatted secret exposed value: %q", formatted)
		}
	}

	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	logger.Info("configuration", "telegram_api_hash", secret)
	if strings.Contains(logs.String(), secretValue) {
		t.Fatalf("structured log exposed secret: %q", logs.String())
	}
	if !strings.Contains(logs.String(), "[redacted]") {
		t.Fatalf("structured log did not use redacted representation: %q", logs.String())
	}
}

func TestSecretStringRejectsBoundaryWhitespaceAndControlCharacters(t *testing.T) {
	const secretValue = "telegram-api-hash-secret"
	for _, input := range []string{
		" " + secretValue,
		secretValue + " ",
		"\t" + secretValue,
		secretValue + "\n",
		secretValue + "\x00suffix",
		"prefix\x7fsuffix",
	} {
		t.Run(fmt.Sprintf("%q", input), func(t *testing.T) {
			var secret SecretString
			if err := secret.UnmarshalText([]byte(input)); err == nil {
				t.Fatalf("unmarshal invalid API hash %q succeeded", input)
			} else if strings.Contains(err.Error(), input) || strings.Contains(err.Error(), secretValue) {
				t.Fatalf("error %q exposed API hash value", err)
			}
			if secret.Configured() || secret.Value() != "" {
				t.Fatalf("rejected API hash retained state: configured=%t value=%q", secret.Configured(), secret.Value())
			}
		})
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
	if config.TelegramAuthEnabled {
		t.Fatal("expected Telegram auth to remain disabled by default")
	}
	if config.TelegramAPIID != 0 || config.TelegramAPIHash.Configured() {
		t.Fatalf("Telegram API settings = (%d, %s), want absent settings", config.TelegramAPIID, config.TelegramAPIHash)
	}
	if config.DeliveryReaperInterval != DefaultDeliveryReaperInterval {
		t.Fatalf("delivery reaper interval = %s, want default %s", config.DeliveryReaperInterval, DefaultDeliveryReaperInterval)
	}
	if config.DeliveryReconcilerInterval != DefaultDeliveryReconcilerInterval {
		t.Fatalf("delivery reconciler interval = %s, want default %s", config.DeliveryReconcilerInterval, DefaultDeliveryReconcilerInterval)
	}
}

func TestLoadFromValidatesDeliveryReaperInterval(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(DeliveryReaperIntervalEnv, value)

			_, err := loadFrom(t.TempDir())
			if err == nil {
				t.Fatal("load configuration succeeded, want invalid reaper interval")
			}
			if !strings.Contains(err.Error(), DeliveryReaperIntervalEnv) {
				t.Fatalf("error %q does not name %s", err, DeliveryReaperIntervalEnv)
			}
		})
	}
}

func TestValidateDeliveryReaperConfiguration(t *testing.T) {
	if err := validateDeliveryReaperConfiguration(Config{DeliveryReaperInterval: time.Second}); err != nil {
		t.Fatalf("validate positive delivery reaper interval: %v", err)
	}
	for _, interval := range []time.Duration{0, -time.Second} {
		if err := validateDeliveryReaperConfiguration(Config{DeliveryReaperInterval: interval}); err == nil {
			t.Fatalf("validate interval %s succeeded, want error", interval)
		}
	}
}

func TestLoadFromValidatesDeliveryReconcilerInterval(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(DeliveryReconcilerIntervalEnv, value)

			_, err := loadFrom(t.TempDir())
			if err == nil {
				t.Fatal("load configuration succeeded, want invalid reconciler interval")
			}
			if !strings.Contains(err.Error(), DeliveryReconcilerIntervalEnv) {
				t.Fatalf("error %q does not name %s", err, DeliveryReconcilerIntervalEnv)
			}
		})
	}
}

func TestValidateDeliveryReconcilerConfiguration(t *testing.T) {
	if err := validateDeliveryReconcilerConfiguration(Config{DeliveryReconcilerInterval: time.Second}); err != nil {
		t.Fatalf("validate positive delivery reconciler interval: %v", err)
	}
	for _, interval := range []time.Duration{0, -time.Second} {
		if err := validateDeliveryReconcilerConfiguration(Config{DeliveryReconcilerInterval: interval}); err == nil {
			t.Fatalf("validate interval %s succeeded, want error", interval)
		}
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

func TestLoadFromRequiresTelegramAuthenticationConfigurationWhenEnabled(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(telegramAuthEnabledEnv, "true")
	t.Setenv(telegramAPIIDEnv, "12345")
	t.Setenv(telegramAPIHashEnv, "telegram-api-hash-secret")

	_, err := loadFrom(t.TempDir())
	if err == nil {
		t.Fatal("load configuration succeeded without encrypted session configuration")
	}
	if !strings.Contains(err.Error(), telegramSessionKeyIDEnv) && !strings.Contains(err.Error(), telegramSessionEncryptionKeyEnv) {
		t.Fatalf("error %q does not name a required Telegram session setting", err)
	}
}

func TestLoadFromAcceptsCompleteTelegramAuthenticationConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(telegramAuthEnabledEnv, "true")
	t.Setenv(telegramAPIIDEnv, "12345")
	t.Setenv(telegramAPIHashEnv, "telegram-api-hash-secret")
	t.Setenv(telegramSessionKeyIDEnv, "current")
	t.Setenv(telegramSessionEncryptionKeyEnv, base64.StdEncoding.EncodeToString([]byte("01234567890123456789012345678901")))

	config, err := loadFrom(t.TempDir())
	if err != nil {
		t.Fatalf("load complete Telegram authentication configuration: %v", err)
	}
	if !config.TelegramAuthEnabled || config.TelegramAPIID != 12345 {
		t.Fatalf("Telegram authentication configuration = enabled:%t api ID:%d", config.TelegramAuthEnabled, config.TelegramAPIID)
	}
	if config.TelegramAPIHash.Value() != "telegram-api-hash-secret" {
		t.Fatalf("Telegram API hash value was not retained for integration use")
	}
	if !config.TelegramSessionEncryptionKey.Configured() || config.TelegramSessionKeyID != "current" {
		t.Fatal("complete Telegram session configuration was not retained")
	}
}

func TestValidateTelegramConfigurationRequiresAPISettingsWhenEnabled(t *testing.T) {
	key := configuredSessionKey(t)
	base := Config{
		TelegramAuthEnabled:          true,
		TelegramAPIID:                12345,
		TelegramAPIHash:              configuredSecret("telegram-api-hash-secret"),
		TelegramSessionKeyID:         "current",
		TelegramSessionEncryptionKey: key,
	}

	for _, test := range []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "missing API ID", cfg: Config{TelegramAuthEnabled: true, TelegramAPIHash: base.TelegramAPIHash, TelegramSessionKeyID: base.TelegramSessionKeyID, TelegramSessionEncryptionKey: key}, want: telegramAPIIDEnv},
		{name: "missing API hash", cfg: Config{TelegramAuthEnabled: true, TelegramAPIID: base.TelegramAPIID, TelegramSessionKeyID: base.TelegramSessionKeyID, TelegramSessionEncryptionKey: key}, want: telegramAPIHashEnv},
		{name: "missing session key ID", cfg: Config{TelegramAuthEnabled: true, TelegramAPIID: base.TelegramAPIID, TelegramAPIHash: base.TelegramAPIHash, TelegramSessionEncryptionKey: key}, want: telegramSessionKeyIDEnv},
		{name: "missing session key", cfg: Config{TelegramAuthEnabled: true, TelegramAPIID: base.TelegramAPIID, TelegramAPIHash: base.TelegramAPIHash, TelegramSessionKeyID: base.TelegramSessionKeyID}, want: telegramSessionEncryptionKeyEnv},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateTelegramConfiguration(test.cfg)
			if err == nil {
				t.Fatalf("validate Telegram configuration succeeded, want %s error", test.want)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error %q does not name %s", err, test.want)
			}
		})
	}
}

func configuredSecret(value string) SecretString {
	var secret SecretString
	_ = secret.UnmarshalText([]byte(value))
	return secret
}

func configuredSessionKey(t *testing.T) SessionEncryptionKey {
	t.Helper()
	var key SessionEncryptionKey
	if err := key.UnmarshalText([]byte(base64.StdEncoding.EncodeToString([]byte("01234567890123456789012345678901")))); err != nil {
		t.Fatalf("create test session key: %v", err)
	}
	return key
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

func TestValidateWebConfigurationRequiresHTTPSInProduction(t *testing.T) {
	if err := validateWebConfiguration(Config{Env: Production, PublicOrigin: mustURL(t, "http://example.test")}); err == nil {
		t.Fatal("accepted HTTP production origin")
	}
	if err := validateWebConfiguration(Config{Env: Production, PublicOrigin: mustURL(t, "https://example.test")}); err != nil {
		t.Fatalf("rejected HTTPS production origin: %v", err)
	}
	if err := validateWebConfiguration(Config{Env: Staging, PublicOrigin: mustURL(t, "http://example.test")}); err == nil {
		t.Fatal("accepted HTTP staging origin")
	}
	if err := validateWebConfiguration(Config{Env: Staging, PublicOrigin: mustURL(t, "https://example.test")}); err != nil {
		t.Fatalf("rejected HTTPS staging origin: %v", err)
	}
}

func TestValidateWebConfigurationAllowsHTTPOnlyForLocalDevelopmentAndTesting(t *testing.T) {
	tests := []struct {
		name      string
		env       EnvKind
		origin    string
		wantError bool
	}{
		{name: "development localhost", env: Development, origin: "http://localhost:8080"},
		{name: "testing localhost", env: Testing, origin: "http://localhost:8080"},
		{name: "development IPv4 loopback", env: Development, origin: "http://127.0.0.1:8080"},
		{name: "testing IPv6 loopback", env: Testing, origin: "http://[::1]:8080"},
		{name: "development nonlocal host", env: Development, origin: "http://dev.example.com:8080", wantError: true},
		{name: "testing nonlocal host", env: Testing, origin: "http://dev.example.com", wantError: true},
		{name: "development HTTPS nonlocal host", env: Development, origin: "https://dev.example.com"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateWebConfiguration(Config{Env: test.env, PublicOrigin: mustURL(t, test.origin)})
			if test.wantError && err == nil {
				t.Fatalf("accepted insecure nonlocal origin %q", test.origin)
			}
			if !test.wantError && err != nil {
				t.Fatalf("rejected origin %q: %v", test.origin, err)
			}
		})
	}
}

func TestSessionCookieModeIsExplicit(t *testing.T) {
	if got := (Config{Env: Production}).SessionCookieName(); got != ProductionSessionCookie {
		t.Fatalf("production cookie = %q, want %q", got, ProductionSessionCookie)
	}
	if got := (Config{Env: Staging}).SessionCookieName(); got != ProductionSessionCookie {
		t.Fatalf("staging cookie = %q, want %q", got, ProductionSessionCookie)
	}
	if got := (Config{Env: Development, PublicOrigin: mustURL(t, "http://localhost:8080")}).SessionCookieName(); got != DevelopmentSessionCookie {
		t.Fatalf("development cookie = %q, want %q", got, DevelopmentSessionCookie)
	}
	if got := (Config{Env: Development, PublicOrigin: mustURL(t, "https://localhost:8080")}).SessionCookieName(); got != ProductionSessionCookie {
		t.Fatalf("HTTPS development cookie = %q, want %q", got, ProductionSessionCookie)
	}
	if !(Config{Env: Testing, PublicOrigin: mustURL(t, "http://localhost:8080")}).SessionCookieAllowsInsecureLocal() {
		t.Fatal("testing HTTP mode did not explicitly allow local insecure cookies")
	}
	for _, origin := range []string{"http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
		cfg := Config{Env: Development, PublicOrigin: mustURL(t, origin)}
		if !cfg.SessionCookieAllowsInsecureLocal() || cfg.SessionCookieSecure() || cfg.SessionCookieName() != DevelopmentSessionCookie {
			t.Fatalf("local development cookie mode for %q: allow=%t secure=%t name=%q", origin, cfg.SessionCookieAllowsInsecureLocal(), cfg.SessionCookieSecure(), cfg.SessionCookieName())
		}
	}
	for _, env := range []EnvKind{Production, Staging} {
		cfg := Config{Env: env, PublicOrigin: mustURL(t, "https://example.test:8443")}
		if cfg.SessionCookieAllowsInsecureLocal() || !cfg.SessionCookieSecure() || cfg.SessionCookieName() != ProductionSessionCookie {
			t.Fatalf("secure %s cookie mode: allow=%t secure=%t name=%q", env, cfg.SessionCookieAllowsInsecureLocal(), cfg.SessionCookieSecure(), cfg.SessionCookieName())
		}
	}
	if cfg := (Config{Env: Development, PublicOrigin: mustURL(t, "http://dev.example.com:8080")}); cfg.SessionCookieAllowsInsecureLocal() || !cfg.SessionCookieSecure() || cfg.SessionCookieName() != ProductionSessionCookie {
		t.Fatalf("nonlocal development HTTP cookie mode: allow=%t secure=%t name=%q", cfg.SessionCookieAllowsInsecureLocal(), cfg.SessionCookieSecure(), cfg.SessionCookieName())
	}
	if (Config{Env: Staging, PublicOrigin: mustURL(t, "https://example.test")}).SessionCookieAllowsInsecureLocal() {
		t.Fatal("staging received local insecure cookie escape hatch")
	}
}

func mustURL(t *testing.T, value string) url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	return *parsed
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("ENV", string(Testing))
	t.Setenv("DB_URL", "postgres://example.invalid/cresora")
	t.Setenv("OPERATOR_ID", "11111111-1111-4111-8111-111111111111")
	t.Setenv("WEB_ADDR", "http://127.0.0.1:8080")
	t.Setenv("PUBLIC_ORIGIN", "http://127.0.0.1:8080")
	t.Setenv(telegramAuthEnabledEnv, "false")
	t.Setenv(telegramAPIIDEnv, "")
	t.Setenv(telegramAPIHashEnv, "")
	t.Setenv(telegramSessionKeyIDEnv, "")
	t.Setenv(telegramSessionEncryptionKeyEnv, "")
}
