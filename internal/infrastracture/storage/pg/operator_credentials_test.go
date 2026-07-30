package pg

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type fakeOperatorCredentialDatabase struct {
	query string
	args  []any
	row   fakeOperatorCredentialRow
}

func (database *fakeOperatorCredentialDatabase) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	database.query = query
	database.args = args
	return database.row
}

type fakeOperatorCredentialRow struct {
	operatorID uuid.UUID
	username   string
}

func (row fakeOperatorCredentialRow) Scan(destinations ...any) error {
	*destinations[0].(*uuid.UUID) = row.operatorID
	*destinations[1].(*string) = row.username
	return nil
}

func TestOperatorCredentialStoreUsesOneAtomicUpsertAndRevokesTokens(t *testing.T) {
	database := &fakeOperatorCredentialDatabase{row: fakeOperatorCredentialRow{
		operatorID: uuid.New(),
		username:   "admin",
	}}
	//go:ignore
	store := &OperatorCredentialStore{database: database}
	operator, err := store.BootstrapOrReset(context.Background(), "admin", canonicalOperatorCredentialPHC)
	if err != nil {
		t.Fatalf("bootstrap operator: %v", err)
	}
	if operator.Username != "admin" || operator.ID != database.row.operatorID {
		t.Fatalf("unexpected returned operator: %+v", operator)
	}
	query := strings.ToLower(database.query)
	for _, fragment := range []string{
		"insert into operators",
		"on conflict (username) do update",
		"password_hash = excluded.password_hash",
		"password_changed_at = clock_timestamp()",
		"tokens_invalid_before = greatest(operators.tokens_invalid_before, clock_timestamp())",
		"returning id, username",
		"clock_timestamp()",
		"greatest(operators.tokens_invalid_before, clock_timestamp())",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("atomic query missing %q: %s", fragment, database.query)
		}
	}
	if strings.Contains(query, "drop column password") || strings.Contains(query, "password =") {
		t.Fatalf("repository query contains legacy/plaintext password state: %s", database.query)
	}
	if len(database.args) != 2 || database.args[0] != "admin" || database.args[1] != canonicalOperatorCredentialPHC {
		t.Fatalf("unexpected query arguments: count=%d username-is-admin=%t hash-length=%d", len(database.args), len(database.args) > 0 && database.args[0] == "admin", queryArgumentLength(database.args, 1))
	}
}

func queryArgumentLength(arguments []any, index int) int {
	if index < 0 || index >= len(arguments) {
		return 0
	}
	value, ok := arguments[index].(string)
	if !ok {
		return 0
	}
	return len(value)
}
