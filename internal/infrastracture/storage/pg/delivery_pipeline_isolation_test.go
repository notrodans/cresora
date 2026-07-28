package pg_test

import (
	stdcontext "context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newIsolatedDeliveryPipelineDatabase(
	context stdcontext.Context,
	t *testing.T,
	databaseURL string,
) (*pgxpool.Pool, string, error) {
	t.Helper()
	baseConfig, failure := pgxpool.ParseConfig(databaseURL)
	if failure != nil {
		return nil, "", fmt.Errorf("parse PostgreSQL URL: %w", failure)
	}
	adminDatabase, failure := pgxpool.NewWithConfig(context, baseConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open PostgreSQL admin pool: %w", failure)
	}
	if failure = adminDatabase.Ping(context); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("ping PostgreSQL admin pool: %w", failure)
	}

	schema := "delivery_pipeline_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, failure = adminDatabase.Exec(context, "CREATE SCHEMA "+quoteDeliveryPipelineIdentifier(schema)); failure != nil {
		adminDatabase.Close()
		return nil, "", fmt.Errorf("create isolated schema: %w", failure)
	}

	var database *pgxpool.Pool
	t.Cleanup(func() {
		if database != nil {
			database.Close()
		}
		cleanupContext, cleanupCancel := stdcontext.WithTimeout(stdcontext.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupFailure := adminDatabase.Exec(
			cleanupContext,
			"DROP SCHEMA "+quoteDeliveryPipelineIdentifier(schema)+" CASCADE",
		); cleanupFailure != nil {
			t.Errorf("drop isolated schema %q: %v", schema, cleanupFailure)
		}
		adminDatabase.Close()
	})

	isolatedConfig := baseConfig.Copy()
	if isolatedConfig.ConnConfig.RuntimeParams == nil {
		isolatedConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	isolatedConfig.ConnConfig.RuntimeParams["search_path"] = schema
	options := isolatedConfig.ConnConfig.RuntimeParams["options"]
	if options != "" {
		options += " "
	}
	isolatedConfig.ConnConfig.RuntimeParams["options"] = options + "-c search_path=" + schema
	isolatedDatabaseURL, failure := deliveryPipelineDatabaseURL(databaseURL, schema)
	if failure != nil {
		return nil, "", failure
	}
	if failure = applyDeliveryPipelineMigrations(context, isolatedDatabaseURL); failure != nil {
		return nil, "", fmt.Errorf("apply migrations to isolated schema: %w", failure)
	}

	database, failure = pgxpool.NewWithConfig(context, isolatedConfig)
	if failure != nil {
		return nil, "", fmt.Errorf("open isolated PostgreSQL pool: %w", failure)
	}
	if failure = database.Ping(context); failure != nil {
		database.Close()
		return nil, "", fmt.Errorf("ping isolated PostgreSQL pool: %w", failure)
	}
	return database, schema, nil
}

func deliveryPipelineDatabaseURL(databaseURL, schema string) (string, error) {
	parsedURL, failure := url.Parse(databaseURL)
	if failure != nil {
		return "", fmt.Errorf("parse isolated PostgreSQL URL: %w", failure)
	}
	if parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql" {
		return "", fmt.Errorf("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsedURL.Query()
	query.Set("search_path", schema)
	options := query.Get("options")
	if options != "" {
		options += " "
	}
	query.Set("options", options+"-c search_path="+schema)
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String(), nil
}

func assertDeliveryPipelineIsolation(context stdcontext.Context, database *pgxpool.Pool, schema string) error {
	var currentSchema string
	if failure := database.QueryRow(context, `SELECT current_schema()`).Scan(&currentSchema); failure != nil {
		return fmt.Errorf("read current schema: %w", failure)
	}
	if currentSchema != schema {
		return fmt.Errorf("current schema is %q, want %q", currentSchema, schema)
	}

	var hasDeliveryTable bool
	if failure := database.QueryRow(
		context,
		`SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = current_schema()
			  AND relation.relname = 'mailing_deliveries'
			  AND relation.relkind = 'r'
		)`,
	).Scan(&hasDeliveryTable); failure != nil {
		return fmt.Errorf("check isolated delivery table: %w", failure)
	}
	if !hasDeliveryTable {
		return fmt.Errorf("isolated schema %q has no mailing_deliveries table", schema)
	}
	return nil
}

func quoteDeliveryPipelineIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
