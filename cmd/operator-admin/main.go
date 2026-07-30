// Command operator-admin bootstraps or resets one local operator credential.
// It is intentionally TTY-only and does not implement browser login.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/config"
	operatorcredentials "github.com/notrodans/cresora/internal/application/operatorcredentials"
	password "github.com/notrodans/cresora/internal/application/operatorcredentials/password"
	operatorcredentialcli "github.com/notrodans/cresora/internal/entrypoint/cli/operatorcredentials"
	"github.com/notrodans/cresora/internal/infrastracture/storage/pg"
)

var errUnexpectedArguments = errors.New("operator-admin does not accept arguments or options")

func main() {
	if failure := run(context.Background()); failure != nil {
		// No credential material is included in errors returned by the command
		// core or storage adapter.
		_, _ = fmt.Fprintln(os.Stderr, failure)
		os.Exit(1)
	}
}

func run(context context.Context) error {
	if len(os.Args) != 1 {
		return errUnexpectedArguments
	}
	terminal, failure := operatorcredentialcli.NewTerminal(os.Stdin, os.Stderr)
	if failure != nil {
		return failure
	}
	root, failure := config.ProjectRoot()
	if failure != nil {
		return fmt.Errorf("locate project root: %w", failure)
	}
	databaseURL, failure := config.LoadDatabaseURL(root)
	if failure != nil {
		return fmt.Errorf("load database configuration: %w", failure)
	}
	database, failure := pgxpool.New(context, databaseURL)
	if failure != nil {
		return fmt.Errorf("open database: %w", failure)
	}
	defer database.Close()

	store := pg.NewOperatorCredentialStore(database)
	service := operatorcredentials.NewService(store, passwordHasher{})
	return operatorcredentialcli.Run(context, terminal, service)
}

type passwordHasher struct{}

func (passwordHasher) Hash(value string) (string, error) {
	return password.Hash(value)
}
