package mailingconsole

import (
	"context"

	application "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/application/services/mailingconsole"
	"github.com/notrodans/cresora/internal/domain/mailing"
)

// CreateDraft creates a new mailing draft.
type CreateDraft interface {
	Execute(context.Context, application.Actor, mailingconsole.CreateDraftInput) (mailing.ID, error)
}

type createDraftCommand struct {
	service *mailingconsole.Service
}

// NewCreateDraft creates a CreateDraft command backed by service.
func NewCreateDraft(service *mailingconsole.Service) CreateDraft {
	return &createDraftCommand{service: service}
}

func (command *createDraftCommand) Execute(context context.Context, actor application.Actor, input mailingconsole.CreateDraftInput) (mailing.ID, error) {
	return command.service.CreateDraft(context, actor, input)
}
