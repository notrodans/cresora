package config

import (
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type EnvKind string

const (
	Production  EnvKind = "PRODUCTION"
	Development EnvKind = "DEVELOPMENT"
	Testing     EnvKind = "TESTING"
	Staging     EnvKind = "STAGING"
)

func (kind *EnvKind) UnmarshalText(text []byte) error {
	value := EnvKind(text)

	switch value {
	case Production, Development, Testing, Staging:
		*kind = value
		return nil
	default:
		return fmt.Errorf(
			"unsupported environment %q: expected %q, %q, %q, or %q",
			value,
			Production,
			Development,
			Testing,
			Staging,
		)
	}
}

type Config struct {
	Env          EnvKind `env:"ENV"`
	DbUrl        string  `env:"DB_URL"`
	WebAddr      url.URL `env:"WEB_ADDR"`
	WebOnly      bool    `env:"WEB_ONLY" envDefault:"true"`
	PublicOrigin url.URL `env:"PUBLIC_ORIGIN"`
	// DeliveryReaperInterval controls the transport-neutral lease recovery poll.
	DeliveryReaperInterval time.Duration `env:"DELIVERY_REAPER_INTERVAL" envDefault:"1m"`
	// DeliveryReconcilerInterval controls the transport-neutral terminal run
	// reconciliation poll.
	DeliveryReconcilerInterval   time.Duration        `env:"DELIVERY_RECONCILER_INTERVAL" envDefault:"1m"`
	TelegramAPIID                int                  `env:"TELEGRAM_API_ID" envDefault:"0"`
	TelegramAPIHash              SecretString         `env:"TELEGRAM_API_HASH" envDefault:""`
	TelegramSessionKeyID         string               `env:"TELEGRAM_SESSION_KEY_ID" envDefault:""`
	TelegramSessionEncryptionKey SessionEncryptionKey `env:"TELEGRAM_SESSION_ENCRYPTION_KEY" envDefault:""`
}

const (
	ProductionSessionCookie  = "__Host-cresora_session"
	DevelopmentSessionCookie = "cresora_session"
)

// SessionCookieName returns the deployment-mode-specific browser cookie name.
// Only explicitly local HTTP development/testing uses the non-__Host name;
// HTTPS local runs and all staging/production runs use the host-only policy.
func (cfg Config) SessionCookieName() string {
	if cfg.SessionCookieSecure() {
		return ProductionSessionCookie
	}
	return DevelopmentSessionCookie
}

func (cfg Config) SessionCookieSecure() bool {
	return !cfg.SessionCookieAllowsInsecureLocal()
}

// SessionCookieAllowsInsecureLocal is true only for explicitly local
// development/testing modes with an HTTP public origin. Staging and
// production never receive this escape hatch.
func (cfg Config) SessionCookieAllowsInsecureLocal() bool {
	return (cfg.Env == Development || cfg.Env == Testing) &&
		strings.EqualFold(cfg.PublicOrigin.Scheme, "http") &&
		isLocalOriginHost(cfg.PublicOrigin)
}

const (
	DeliveryReaperIntervalEnv         = "DELIVERY_REAPER_INTERVAL"
	DefaultDeliveryReaperInterval     = time.Minute
	DeliveryReconcilerIntervalEnv     = "DELIVERY_RECONCILER_INTERVAL"
	DefaultDeliveryReconcilerInterval = time.Minute
	telegramAPIIDEnv                  = "TELEGRAM_API_ID"
	telegramAPIHashEnv                = "TELEGRAM_API_HASH"
	telegramSessionKeyIDEnv           = "TELEGRAM_SESSION_KEY_ID"
	telegramSessionEncryptionKeyEnv   = "TELEGRAM_SESSION_ENCRYPTION_KEY"
	telegramSessionKeyIDMaxLength     = 128
)

// SecretString stores a deployment secret without making its value available
// through ordinary formatting or structured logging. Value is intentionally
// the explicit escape hatch for the component that must hand the secret to an
// external client.
type SecretString struct {
	value string
}

func (secret *SecretString) UnmarshalText(text []byte) error {
	*secret = SecretString{}
	value := string(text)
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not have leading or trailing whitespace", telegramAPIHashEnv)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s must not contain control characters", telegramAPIHashEnv)
	}
	secret.value = value
	return nil
}

// Configured reports whether a non-blank secret was supplied.
func (secret SecretString) Configured() bool {
	return strings.TrimSpace(secret.value) != ""
}

// Value returns the secret for the narrow integration point that needs it.
func (secret SecretString) Value() string {
	return secret.value
}

// String prevents accidental logging of the secret.
func (secret SecretString) String() string {
	if !secret.Configured() {
		return "[not configured]"
	}
	return "[redacted]"
}

// GoString prevents %#v diagnostics from exposing the secret.
func (secret SecretString) GoString() string {
	return secret.String()
}

// Format keeps string-specific formatting verbs such as %q and %s redacted.
func (secret SecretString) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, secret.String())
}

// LogValue keeps the secret redacted when passed to slog.Any.
func (secret SecretString) LogValue() slog.Value {
	return slog.StringValue(secret.String())
}

// SessionEncryptionKey keeps the key bytes out of ordinary formatted config
// output. An empty value is deliberately allowed so the existing HTTP-only
// application can still load configuration before Telegram is wired in.
type SessionEncryptionKey struct {
	bytes      [32]byte
	configured bool
}

// UnmarshalText decodes the runtime key from standard base64. The key is
// optional until the Telegram session adapter is enabled, but a configured
// value must always be exactly 32 bytes for AES-256.
func (key *SessionEncryptionKey) UnmarshalText(text []byte) error {
	*key = SessionEncryptionKey{}
	if len(text) == 0 {
		return nil
	}

	decoded, err := base64.StdEncoding.DecodeString(string(text))
	if err != nil {
		return fmt.Errorf("%s must be valid base64: %w", telegramSessionEncryptionKeyEnv, err)
	}
	if len(decoded) != len(key.bytes) {
		return fmt.Errorf("%s must decode to exactly 32 bytes", telegramSessionEncryptionKeyEnv)
	}
	copy(key.bytes[:], decoded)
	key.configured = true
	return nil
}

// Configured reports whether a non-empty runtime key was supplied.
func (key SessionEncryptionKey) Configured() bool {
	return key.configured
}

// Bytes returns a copy of the runtime key suitable for constructing the
// encryption adapter.
func (key SessionEncryptionKey) Bytes() []byte {
	if !key.configured {
		return nil
	}
	result := make([]byte, len(key.bytes))
	copy(result, key.bytes[:])
	return result
}

// String prevents accidental logging of the key material.
func (key SessionEncryptionKey) String() string {
	if !key.configured {
		return "[not configured]"
	}
	return "[redacted]"
}

// GoString prevents %#v diagnostics from exposing the unexported byte array.
func (key SessionEncryptionKey) GoString() string {
	return key.String()
}

// Format keeps string-specific formatting verbs redacted.
func (key SessionEncryptionKey) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, key.String())
}

// LogValue keeps the key redacted when passed to slog.Any.
func (key SessionEncryptionKey) LogValue() slog.Value {
	return slog.StringValue(key.String())
}

func MustLoad(root string) *Config {
	cfg, err := loadFrom(root)
	if err != nil {
		panic(err)
	}
	return &cfg
}

// LoadDatabaseURL loads only the database setting needed by maintenance
// commands. It intentionally does not require web or operator runtime
// configuration that is unrelated to local credential bootstrap.
func LoadDatabaseURL(root string) (string, error) {
	if err := loadEnvironmentFile(root); err != nil {
		return "", err
	}
	databaseURL := os.Getenv("DB_URL")
	if databaseURL == "" {
		return "", fmt.Errorf("DB_URL is required")
	}
	return databaseURL, nil
}

func loadFrom(root string) (Config, error) {
	if err := loadEnvironmentFile(root); err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{
		RequiredIfNoDef: true,
	}); err != nil {
		return Config{}, fmt.Errorf("parse configuration: %w", err)
	}
	if err := validateTelegramConfiguration(cfg); err != nil {
		return Config{}, err
	}
	if err := validateDeliveryReaperConfiguration(cfg); err != nil {
		return Config{}, err
	}
	if err := validateDeliveryReconcilerConfiguration(cfg); err != nil {
		return Config{}, err
	}
	if err := validateWebConfiguration(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func validateWebConfiguration(cfg Config) error {
	if cfg.PublicOrigin.User != nil || cfg.PublicOrigin.Host == "" || (!strings.EqualFold(cfg.PublicOrigin.Scheme, "http") && !strings.EqualFold(cfg.PublicOrigin.Scheme, "https")) {
		return fmt.Errorf("PUBLIC_ORIGIN must be an absolute HTTP(S) URL without user info")
	}
	if (cfg.PublicOrigin.Path != "" && cfg.PublicOrigin.Path != "/") || cfg.PublicOrigin.RawQuery != "" || cfg.PublicOrigin.Fragment != "" {
		return fmt.Errorf("PUBLIC_ORIGIN must contain only a scheme and host")
	}
	if (cfg.Env == Production || cfg.Env == Staging) && !strings.EqualFold(cfg.PublicOrigin.Scheme, "https") {
		return fmt.Errorf("PUBLIC_ORIGIN must use HTTPS in staging and production")
	}
	if strings.EqualFold(cfg.PublicOrigin.Scheme, "http") && !cfg.SessionCookieAllowsInsecureLocal() {
		return fmt.Errorf("PUBLIC_ORIGIN may use HTTP only for development/testing on localhost or a loopback IP")
	}
	return nil
}

func isLocalOriginHost(origin url.URL) bool {
	hostname := origin.Hostname()
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	ip := net.ParseIP(hostname)
	return ip != nil && ip.IsLoopback()
}

func validateDeliveryReaperConfiguration(cfg Config) error {
	if cfg.DeliveryReaperInterval <= 0 {
		return fmt.Errorf("%s must be positive", DeliveryReaperIntervalEnv)
	}
	return nil
}

func validateDeliveryReconcilerConfiguration(cfg Config) error {
	if cfg.DeliveryReconcilerInterval <= 0 {
		return fmt.Errorf("%s must be positive", DeliveryReconcilerIntervalEnv)
	}
	return nil
}

func validateTelegramSessionConfiguration(cfg Config) error {
	keyID := cfg.TelegramSessionKeyID
	if keyID != "" {
		if keyID != strings.TrimSpace(keyID) || strings.IndexFunc(keyID, func(r rune) bool {
			return r < 0x20 || r == 0x7f
		}) >= 0 {
			return fmt.Errorf("%s must be a non-empty printable identifier", telegramSessionKeyIDEnv)
		}
		if len(keyID) > telegramSessionKeyIDMaxLength {
			return fmt.Errorf("%s must be at most %d bytes", telegramSessionKeyIDEnv, telegramSessionKeyIDMaxLength)
		}
	}

	if cfg.TelegramSessionEncryptionKey.Configured() && keyID == "" {
		return fmt.Errorf("%s is required when %s is configured", telegramSessionKeyIDEnv, telegramSessionEncryptionKeyEnv)
	}
	if keyID != "" && !cfg.TelegramSessionEncryptionKey.Configured() {
		return fmt.Errorf("%s is required when %s is configured", telegramSessionEncryptionKeyEnv, telegramSessionKeyIDEnv)
	}
	return nil
}

func validateTelegramConfiguration(cfg Config) error {
	if err := validateTelegramSessionConfiguration(cfg); err != nil {
		return err
	}

	if cfg.TelegramAPIID <= 0 {
		return fmt.Errorf("%s must be positive", telegramAPIIDEnv)
	}
	if !cfg.TelegramAPIHash.Configured() {
		return fmt.Errorf("%s is required", telegramAPIHashEnv)
	}
	if cfg.TelegramSessionKeyID == "" {
		return fmt.Errorf("%s is required", telegramSessionKeyIDEnv)
	}
	if !cfg.TelegramSessionEncryptionKey.Configured() {
		return fmt.Errorf("%s is required", telegramSessionEncryptionKeyEnv)
	}
	return nil
}

func loadEnvironmentFile(root string) error {
	path := filepath.Join(root, ".env")
	if err := godotenv.Load(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load environment file %q: %w", path, err)
	}
	return nil
}
