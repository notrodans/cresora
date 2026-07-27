package coordinates

import (
	"github.com/notrodans/nebula-go/internal/domain/mailing"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

type Coordinates interface {
	Mailing() mailing.ID
	Run() mailing.RunID
	Recipient() recipient.ID
}

// Locates one mailing delivery row
type coordinates struct {
	mailing   mailing.ID
	run       mailing.RunID
	recipient recipient.ID
}

func New(mid mailing.ID, rid mailing.RunID, r recipient.ID) Coordinates {
	return coordinates{
		mailing:   mid,
		run:       rid,
		recipient: r,
	}
}

func (value coordinates) Mailing() mailing.ID     { return value.mailing }
func (value coordinates) Run() mailing.RunID      { return value.run }
func (value coordinates) Recipient() recipient.ID { return value.recipient }
