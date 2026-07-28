package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/notrodans/nebula-go/internal/infrastracture/storage/pg/invariants"
)

func TestRunRequiresDatabaseURL(t *testing.T) {
	var output, errorOutput bytes.Buffer
	status := run(context.Background(), []string{"-db-url", ""}, &output, &errorOutput)
	if status != ExitExecution {
		t.Fatalf("run exit status = %d, want %d", status, ExitExecution)
	}
	if !strings.Contains(errorOutput.String(), "database URL is required") {
		t.Fatalf("error output %q does not explain the missing database URL", errorOutput.String())
	}
}

func TestRunRejectsUnboundedSampleLimit(t *testing.T) {
	var output, errorOutput bytes.Buffer
	status := run(
		context.Background(),
		[]string{"-db-url", "postgres://unused", "-sample-limit", "101"},
		&output,
		&errorOutput,
	)
	if status != ExitExecution {
		t.Fatalf("run exit status = %d, want %d", status, ExitExecution)
	}
	if !strings.Contains(errorOutput.String(), "sample limit must be between") {
		t.Fatalf("error output %q does not explain the sample limit", errorOutput.String())
	}
}

func TestRunReturnsExecutionFailureForMalformedDatabaseURL(t *testing.T) {
	var output, errorOutput bytes.Buffer
	status := run(
		context.Background(),
		[]string{"-db-url", "postgres://%gh&%ij"},
		&output,
		&errorOutput,
	)
	if status != ExitExecution {
		t.Fatalf("run exit status = %d, want %d", status, ExitExecution)
	}
	if !strings.Contains(errorOutput.String(), "invariant check failed") {
		t.Fatalf("error output %q does not identify an execution failure", errorOutput.String())
	}
}

func TestRunWithCheckerReturnsCleanStatusAndJSONReport(t *testing.T) {
	var output, errorOutput bytes.Buffer
	var gotURL string
	var gotSampleLimit int
	var gotGrace time.Duration
	status := runWithChecker(
		context.Background(),
		[]string{"-db-url", "postgres://injected", "-sample-limit", "7", "-expired-lease-grace", "45s"},
		&output,
		&errorOutput,
		func(_ context.Context, databaseURL string, sampleLimit int, grace time.Duration) (invariants.Report, error) {
			gotURL = databaseURL
			gotSampleLimit = sampleLimit
			gotGrace = grace
			return invariants.Report{Results: []invariants.Result{{
				Name:     invariants.CheckExpiredSendingLease,
				Severity: invariants.SeverityWarning,
			}}}, nil
		},
	)
	if status != ExitClean {
		t.Fatalf("run exit status = %d, want %d", status, ExitClean)
	}
	if errorOutput.Len() != 0 {
		t.Fatalf("run error output = %q, want empty", errorOutput.String())
	}
	if gotURL != "postgres://injected" || gotSampleLimit != 7 || gotGrace != 45*time.Second {
		t.Fatalf("checker arguments = %q/%d/%s, want %q/7/45s", gotURL, gotSampleLimit, gotGrace, "postgres://injected")
	}
	var report invariants.Report
	if failure := json.Unmarshal(output.Bytes(), &report); failure != nil {
		t.Fatalf("decode clean JSON report: %v", failure)
	}
	if report.HasViolations() {
		t.Fatal("clean report has violations")
	}
}

func TestRunWithCheckerReturnsViolationStatusAndJSONReport(t *testing.T) {
	var output, errorOutput bytes.Buffer
	want := invariants.Report{Results: []invariants.Result{{
		Name:     invariants.CheckExpiredSendingLease,
		Severity: invariants.SeverityWarning,
		Count:    1,
	}}}
	status := runWithChecker(
		context.Background(),
		[]string{"-db-url", "postgres://injected"},
		&output,
		&errorOutput,
		func(_ context.Context, _ string, _ int, _ time.Duration) (invariants.Report, error) {
			return want, nil
		},
	)
	if status != ExitViolations {
		t.Fatalf("run exit status = %d, want %d", status, ExitViolations)
	}
	if errorOutput.Len() != 0 {
		t.Fatalf("run error output = %q, want empty", errorOutput.String())
	}
	var got invariants.Report
	if failure := json.Unmarshal(output.Bytes(), &got); failure != nil {
		t.Fatalf("decode violation JSON report: %v", failure)
	}
	if !got.HasViolations() || got.Results[0].Count != 1 {
		t.Fatalf("decoded report = %#v, want one violation", got)
	}
}

func TestRunWithCheckerPreservesExecutionErrorStatus(t *testing.T) {
	var output, errorOutput bytes.Buffer
	status := runWithChecker(
		context.Background(),
		[]string{"-db-url", "postgres://injected"},
		&output,
		&errorOutput,
		func(context.Context, string, int, time.Duration) (invariants.Report, error) {
			return invariants.Report{}, errors.New("injected check failure")
		},
	)
	if status != ExitExecution {
		t.Fatalf("run exit status = %d, want %d", status, ExitExecution)
	}
	if output.Len() != 0 {
		t.Fatalf("run output = %q, want empty", output.String())
	}
	if !strings.Contains(errorOutput.String(), "invariant check failed: injected check failure") {
		t.Fatalf("error output = %q, want preserved execution error", errorOutput.String())
	}
}

func TestExitStatusesAreDistinct(t *testing.T) {
	if ExitClean == ExitViolations || ExitClean == ExitExecution || ExitViolations == ExitExecution {
		t.Fatalf("exit statuses are not distinct: clean=%d violations=%d execution=%d", ExitClean, ExitViolations, ExitExecution)
	}
}
