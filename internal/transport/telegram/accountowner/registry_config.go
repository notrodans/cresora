package accountowner

import (
	"errors"
	"time"

	"github.com/notrodans/cresora/internal/transport/telegram/gotdclient"
)

const (
	defaultRuntimeCapacity     = 32
	defaultRuntimeIdleTimeout  = 5 * time.Minute
	defaultRuntimeDrainTimeout = 5 * time.Second
)

// RegistryConfig controls the bounded, process-local account runtime.
//
// A registry is intentionally a single-deployment component. Session bytes
// and lifecycle state remain durable elsewhere; this registry only owns the
// currently running gotd clients and their admission fences.
type RegistryConfig struct {
	Factory gotdclient.Factory
	AppID   int
	AppHash string

	// Capacity is the maximum number of admitted account owners. A zero value
	// uses the conservative default.
	Capacity int
	// IdleTimeout controls when an owner with no open handles or operations may
	// be evicted. A zero value uses the default.
	IdleTimeout time.Duration
	// DrainTimeout bounds per-account draining during Stop and eviction. A zero
	// value uses the default.
	DrainTimeout time.Duration
}

func normalizeRegistryConfig(
	config RegistryConfig,
) (RegistryConfig, error) {
	if config.Capacity == 0 {
		config.Capacity = defaultRuntimeCapacity
	}
	if config.Capacity < 0 {
		return RegistryConfig{}, errors.New(
			"telegram account runtime capacity must not be negative",
		)
	}

	if config.IdleTimeout == 0 {
		config.IdleTimeout = defaultRuntimeIdleTimeout
	}
	if config.IdleTimeout < 0 {
		return RegistryConfig{}, errors.New(
			"telegram account runtime idle timeout must not be negative",
		)
	}

	if config.DrainTimeout == 0 {
		config.DrainTimeout = defaultRuntimeDrainTimeout
	}
	if config.DrainTimeout < 0 {
		return RegistryConfig{}, errors.New(
			"telegram account runtime drain timeout must not be negative",
		)
	}

	return config, nil
}
