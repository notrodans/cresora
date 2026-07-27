package mailingconsole

import (
	"context"

	"github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
)

// Dashboard loads the operator's mailing console dashboard.
type Dashboard interface {
	Execute(context.Context) (mailingconsole.Dashboard, error)
}

type dashboardRequest struct {
	service *mailingconsole.Service
}

// NewDashboard creates a Dashboard request backed by service.
func NewDashboard(service *mailingconsole.Service) Dashboard {
	return &dashboardRequest{service: service}
}

func (request *dashboardRequest) Execute(context context.Context) (mailingconsole.Dashboard, error) {
	return request.service.Dashboard(context)
}
