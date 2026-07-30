package mailingconsole

import (
	"context"

	application "github.com/notrodans/nebula-go/internal/application"
	"github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
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
	if service == nil {
		panic("create mailing console draft command without service")
	}
	return &createDraftCommand{service: service}
}

func (command *createDraftCommand) Execute(context context.Context, actor application.Actor, input mailingconsole.CreateDraftInput) (mailing.ID, error) {
	return command.service.CreateDraft(context, actor, input)
}
