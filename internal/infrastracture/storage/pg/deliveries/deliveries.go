package pg

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/coordinates"
)

// +--------------------------------------------------+
// | Represents deliveries stored in PostgreSQL       |
// +--------------------------------------------------+
type deliveries struct {
	database *pgxpool.Pool
}

func NewDeliveries(database *pgxpool.Pool) delivery.Deliveries {
	return deliveries{database: database}
}

func (all deliveries) Delivery(
	mailingID mailing.ID,
	runID mailing.RunID,
	recipientID recipient.ID,
	token delivery.Token,
) delivery.Delivery {
	return persistentDelivery{
		database: all.database,
		identity: coordinates.New(
			mailingID,
			runID,
			recipientID,
		),
		token: token,
	}
}
