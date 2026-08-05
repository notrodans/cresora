package accountowner

import (
	"context"
	"testing"
	"time"

	gotdtelegram "github.com/gotd/td/telegram"
)

func TestEvictIdle_ReturnsPromptlyForNonIdleSlot(t *testing.T) {
	factory := new(registryOwnerFactory)
	registry := newFakeRegistry(t, factory, RegistryConfig{Capacity: 1, IdleTimeout: time.Hour})
	target := registryTarget()
	if err := registry.Execute(
		context.Background(),
		target,
		func(context.Context, *gotdtelegram.Client) error {
			return nil
		},
	); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		registry.evictIdle()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("evictIdle hung: slot.mu was not released when the idle condition was false")
	}
}
