package operatoraccountauth

import (
	"context"

	applicationroot "github.com/notrodans/cresora/internal/application"
	auth "github.com/notrodans/cresora/internal/application/operatoraccountauth"
)

// StatusAdapter adapts the merged durable/process-local status query to the
// request port.
type StatusAdapter struct{ service *auth.Service }

func NewStatus(service *auth.Service) Status {
	return StatusAdapter{service: service}
}

func (adapter StatusAdapter) Execute(ctx context.Context, actor applicationroot.Actor) (auth.Status, error) {
	return adapter.service.Status(ctx, actor)
}

var _ Status = StatusAdapter{}
