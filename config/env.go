package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"
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
	Env          EnvKind   `env:"ENV"`
	DbUrl        string    `env:"DB_URL"`
	OperatorID   uuid.UUID `env:"OPERATOR_ID"`
	WebAddr      url.URL   `env:"WEB_ADDR"`
	WebOnly      bool      `env:"WEB_ONLY" envDefault:"true"`
	PublicOrigin url.URL   `env:"PUBLIC_ORIGIN"`
}

func MustLoad(root string) *Config {
	cfg, err := loadFrom(root)
	if err != nil {
		panic(err)
	}
	return &cfg
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

	return cfg, nil
}

func loadEnvironmentFile(root string) error {
	path := filepath.Join(root, ".env")
	if err := godotenv.Load(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load environment file %q: %w", path, err)
	}
	return nil
}
