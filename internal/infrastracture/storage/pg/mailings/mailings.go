package pg

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
)

// pgMailings represents the PostgreSQL mailings table.
type pgMailings struct {
	database mailingDatabase
}

// pgOperatorMailings represents an immutable operator-scoped mailing table view.
type pgOperatorMailings struct {
	database   mailingDatabase
	operatorID uuid.UUID
}

type mailingDatabase interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

var _ mailing.Mailings = pgMailings{}
var _ mailingconsole.Mailings = pgMailings{}
var _ mailingconsole.OperatorMailings = pgOperatorMailings{}

func NewMailings(database *pgxpool.Pool) pgMailings {
	validatePool(database, "mailings")
	return pgMailings{database: database}
}

func (all pgMailings) Mailing(identity mailing.ID) mailing.Mailing {
	all.validateMailingIdentity(identity)
	return pgMailing{
		database: all.database,
		identity: identity,
		scope:    systemMailingScope{},
	}
}

func (all pgMailings) OwnedBy(operatorID uuid.UUID) mailingconsole.OperatorMailings {
	all.validateDatabase()
	validateOperatorID(operatorID, "select operator mailings")
	return pgOperatorMailings{
		database:   all.database,
		operatorID: operatorID,
	}
}

func (all pgOperatorMailings) Mailing(identity mailing.ID) mailing.Mailing {
	all.validate()
	if identity.UUID() == uuid.Nil {
		panic("select operator PostgreSQL mailing with zero identity")
	}
	return pgMailing{
		database: all.database,
		identity: identity,
		scope:    operatorMailingScope{operatorID: all.operatorID},
	}
}

func (all pgOperatorMailings) validate() {
	if all.database == nil {
		panic("use operator PostgreSQL mailings without database")
	}
	validateOperatorID(all.operatorID, "use operator PostgreSQL mailings")
}

func (all pgMailings) validateDatabase() {
	if all.database == nil {
		panic("use PostgreSQL mailings without database")
	}
}

func (all pgMailings) validateMailingIdentity(identity mailing.ID) {
	all.validateDatabase()
	if identity.UUID() == uuid.Nil {
		panic("select PostgreSQL mailing with zero identity")
	}
}

func validatePool(database *pgxpool.Pool, resource string) {
	if database == nil {
		panic("create PostgreSQL " + resource + " without database")
	}
}

func validateOperatorID(operatorID uuid.UUID, operation string) {
	if operatorID == uuid.Nil {
		panic(operation + " without operator identity")
	}
}
