package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notrodans/cresora/internal/infrastracture/storage/pg/invariants"
)

const (
	ExitClean      = 0
	ExitViolations = 1
	ExitExecution  = 2
	checkTimeout   = 30 * time.Second
)

type checkFunction func(context.Context, string, int, time.Duration) (invariants.Report, error)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(
	parent context.Context,
	arguments []string,
	output io.Writer,
	errorOutput io.Writer,
) int {
	return runWithChecker(parent, arguments, output, errorOutput, executeCheck)
}

func runWithChecker(
	parent context.Context,
	arguments []string,
	output io.Writer,
	errorOutput io.Writer,
	check checkFunction,
) int {
	defaultDatabaseURL := os.Getenv("DB_URL")
	if defaultDatabaseURL == "" {
		defaultDatabaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	flags := flag.NewFlagSet("invariantcheck", flag.ContinueOnError)
	flags.SetOutput(errorOutput)
	databaseURL := flags.String("db-url", defaultDatabaseURL, "PostgreSQL connection URL")
	sampleLimit := flags.Int("sample-limit", invariants.DefaultSampleLimit, "maximum samples per check")
	expiredLeaseGrace := flags.Duration(
		"expired-lease-grace",
		invariants.DefaultExpiredLeaseGrace,
		"grace period after a sending lease expires before reporting a warning",
	)
	if failure := flags.Parse(arguments); failure != nil {
		return ExitExecution
	}
	if *databaseURL == "" {
		_, _ = fmt.Fprintln(errorOutput, "invariant check failed: database URL is required (use -db-url or DB_URL)")
		return ExitExecution
	}
	if *sampleLimit <= 0 || *sampleLimit > invariants.MaxSampleLimit {
		_, _ = fmt.Fprintf(
			errorOutput,
			"invariant check failed: sample limit must be between 1 and %d\n",
			invariants.MaxSampleLimit,
		)
		return ExitExecution
	}
	if *expiredLeaseGrace < 0 {
		_, _ = fmt.Fprintln(errorOutput, "invariant check failed: expired lease grace must not be negative")
		return ExitExecution
	}

	context, cancel := context.WithTimeout(parent, checkTimeout)
	defer cancel()
	report, failure := check(context, *databaseURL, *sampleLimit, *expiredLeaseGrace)
	if failure != nil {
		_, _ = fmt.Fprintf(errorOutput, "invariant check failed: %v\n", failure)
		return ExitExecution
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if failure = encoder.Encode(report); failure != nil {
		_, _ = fmt.Fprintf(errorOutput, "invariant check failed: write report: %v\n", failure)
		return ExitExecution
	}
	if report.HasViolations() {
		return ExitViolations
	}
	return ExitClean
}

func executeCheck(
	context context.Context,
	databaseURL string,
	sampleLimit int,
	expiredLeaseGrace time.Duration,
) (invariants.Report, error) {
	database, failure := pgxpool.New(context, databaseURL)
	if failure != nil {
		return invariants.Report{}, fmt.Errorf("open PostgreSQL database: %w", failure)
	}
	defer database.Close()
	if failure = database.Ping(context); failure != nil {
		return invariants.Report{}, fmt.Errorf("ping PostgreSQL database: %w", failure)
	}

	checker := invariants.NewWithSampleLimitAndExpiredLeaseGrace(database, sampleLimit, expiredLeaseGrace)
	return checker.Check(context)
}
