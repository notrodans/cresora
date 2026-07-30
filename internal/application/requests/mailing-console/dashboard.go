package mailingconsole

import (
	"context"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/services/mailingconsole"
)

// Dashboard loads the operator's mailing console dashboard.
type Dashboard interface {
	Execute(context.Context, application.Actor) (mailingconsole.Dashboard, error)
}

type dashboardRequest struct {
	service *mailingconsole.Service
}

// NewDashboard creates a Dashboard request backed by service.
func NewDashboard(service *mailingconsole.Service) Dashboard {
	return &dashboardRequest{service: service}
}

func (request *dashboardRequest) Execute(context context.Context, actor application.Actor) (mailingconsole.Dashboard, error) {
	return request.service.Dashboard(context, actor)
}
