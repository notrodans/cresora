package account

import (
	application "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/transport/telegram"
	targets "github.com/notrodans/nebula-go/internal/transport/telegram/targets"
)

// targetProvider binds target resolution to the route's Telegram account.
type targetProvider struct {
	lookup telegram.PeerLookup
}

var _ Targets = targetProvider{}

// NewTargets creates a provider of account-bound Telegram target resolvers.
func NewTargets(lookup telegram.PeerLookup) Targets {
	if lookup == nil {
		panic("provide Telegram targets without peer lookup")
	}
	return targetProvider{lookup: lookup}
}

func (provider targetProvider) Targets(route application.Route) telegram.Targets {
	return targets.NewResolver(route.UUID(), provider.lookup)
}
