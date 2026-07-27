package mailingconsole

import (
	"context"

	"github.com/google/uuid"
	mailingconsole "github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
)

// Queue places a mailing in the send queue.
type Queue interface {
	Execute(context.Context, uuid.UUID) error
}

type queueCommand struct {
	service *mailingconsole.Service
}

// NewQueue creates a Queue command backed by service.
func NewQueue(service *mailingconsole.Service) Queue {
	return &queueCommand{service: service}
}

func (command *queueCommand) Execute(context context.Context, mailingID uuid.UUID) error {
	return command.service.Queue(context, mailingID)
}
