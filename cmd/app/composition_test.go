package main

import (
	"context"
	"strings"
	"testing"

	telegramaccount "github.com/notrodans/cresora/internal/transport/telegram/account"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

func TestTelegramRuntimeModeMatrix(t *testing.T) {
	tests := []struct {
		name               string
		webOnly            bool
		telegramAuth       bool
		wantRuntime        bool
		wantDeliveryWorker bool
	}{
		{
			name:               "web-only/auth-disabled",
			webOnly:            true,
			telegramAuth:       false,
			wantRuntime:        false,
			wantDeliveryWorker: false,
		},
		{
			name:               "web-only/auth-enabled",
			webOnly:            true,
			telegramAuth:       true,
			wantRuntime:        true,
			wantDeliveryWorker: false,
		},
		{
			name:               "delivery/auth-enabled",
			webOnly:            false,
			telegramAuth:       true,
			wantRuntime:        true,
			wantDeliveryWorker: true,
		},
		{
			name:               "delivery/auth-disabled",
			webOnly:            false,
			telegramAuth:       false,
			wantRuntime:        true,
			wantDeliveryWorker: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validOperatorAuthConfig(t)
			cfg.WebOnly = test.webOnly
			cfg.TelegramAuthEnabled = test.telegramAuth

			if got := sharedTelegramRuntimeRequired(cfg); got != test.wantRuntime {
				t.Fatalf("shared runtime required = %t, want %t", got, test.wantRuntime)
			}
			if got := !cfg.WebOnly; got != test.wantDeliveryWorker {
				t.Fatalf("delivery worker enabled = %t, want %t", got, test.wantDeliveryWorker)
			}

			runtime, failure := composeTelegramRuntime(cfg, nil)
			if failure != nil {
				t.Fatalf("compose Telegram runtime: %v", failure)
			}
			if (runtime != nil) != test.wantRuntime {
				t.Fatalf("runtime present = %t, want %t", runtime != nil, test.wantRuntime)
			}
			if runtime != nil {
				t.Cleanup(func() {
					if failure := runtime.Stop(context.Background()); failure != nil {
						t.Errorf("stop Telegram runtime: %v", failure)
					}
				})
			}
		})
	}
}

func TestTelegramRuntimeValidationAppliesToDeliveryWithoutAuthRoute(t *testing.T) {
	cfg := validOperatorAuthConfig(t)
	cfg.WebOnly = false
	cfg.TelegramAuthEnabled = false
	cfg.TelegramAPIID = 0

	runtime, failure := composeTelegramRuntime(cfg, nil)
	if runtime != nil {
		t.Fatal("invalid delivery runtime configuration constructed a registry")
	}
	if failure == nil || !strings.Contains(failure.Error(), "TELEGRAM_API_ID") {
		t.Fatalf("delivery runtime validation error = %v, want TELEGRAM_API_ID", failure)
	}
}

func TestDeliveryAndAuthenticationUseTheSameRegistryContract(t *testing.T) {
	cfg := validOperatorAuthConfig(t)
	cfg.WebOnly = false

	runtime, failure := composeTelegramRuntime(cfg, nil)
	if failure != nil {
		t.Fatalf("compose shared Telegram runtime: %v", failure)
	}
	if runtime == nil {
		t.Fatal("delivery/authentication composition returned no shared registry")
	}
	t.Cleanup(func() {
		if failure := runtime.Stop(context.Background()); failure != nil {
			t.Errorf("stop shared Telegram runtime: %v", failure)
		}
	})

	// The root passes this one concrete registry to both boundaries. Assigning
	// it to each consumer contract keeps the identity check independent of any
	// network or database activity.
	var authRuntime operatorAuthRuntime = runtime
	var deliveryRuntime telegramaccount.Runtime = runtime
	if any(authRuntime) != any(deliveryRuntime) {
		t.Fatal("authentication and delivery received different runtime values")
	}
	var _ *accountowner.Registry = runtime
}
