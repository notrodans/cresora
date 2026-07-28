package invariants

import (
	"strings"
	"testing"
	"time"
)

func TestReportHasViolationsIncludesWarnings(t *testing.T) {
	report := Report{Results: []Result{{
		Name:     CheckExpiredSendingLease,
		Severity: SeverityWarning,
		Count:    1,
	}}}
	if !report.HasViolations() {
		t.Fatal("Report.HasViolations returned false for a warning with a non-zero count")
	}
}

func TestReportHasViolationsReturnsFalseForCleanResults(t *testing.T) {
	report := Report{Results: []Result{{
		Name:     CheckSendingDeliveryWithoutLease,
		Severity: SeverityError,
		Count:    0,
	}}}
	if report.HasViolations() {
		t.Fatal("Report.HasViolations returned true for a zero-count result")
	}
}

func TestNewWithSampleLimitCapsTheBound(t *testing.T) {
	checker := NewWithSampleLimit(nil, MaxSampleLimit+1)
	if checker.sampleLimit != MaxSampleLimit {
		t.Fatalf("sample limit = %d, want %d", checker.sampleLimit, MaxSampleLimit)
	}
	if checker.expiredLeaseGrace != DefaultExpiredLeaseGrace {
		t.Fatalf("expired lease grace = %s, want %s", checker.expiredLeaseGrace, DefaultExpiredLeaseGrace)
	}
}

func TestNewWithSampleLimitAndExpiredLeaseGraceUsesConfiguredGrace(t *testing.T) {
	const grace = 45 * time.Second
	checker := NewWithSampleLimitAndExpiredLeaseGrace(nil, 3, grace)
	if checker.sampleLimit != 3 {
		t.Fatalf("sample limit = %d, want 3", checker.sampleLimit)
	}
	if checker.expiredLeaseGrace != grace {
		t.Fatalf("expired lease grace = %s, want %s", checker.expiredLeaseGrace, grace)
	}
}

func TestExpiredLeaseWarningReportsOnlyAfterGraceBoundary(t *testing.T) {
	grace := 10 * time.Second
	for _, test := range []struct {
		name       string
		expiredFor time.Duration
		want       bool
	}{
		{name: "just below", expiredFor: grace - time.Nanosecond, want: false},
		{name: "at boundary", expiredFor: grace, want: false},
		{name: "just above", expiredFor: grace + time.Nanosecond, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := leaseExpiryExceedsGrace(test.expiredFor, grace)
			if got != test.want {
				t.Fatalf("lease expiry check for %s = %t, want %t", test.expiredFor, got, test.want)
			}
		})
	}
}

func TestExpiredLeaseWarningQueryUsesStrictGraceComparison(t *testing.T) {
	var expiredLeaseCheck checkSpec
	for _, check := range checks {
		if check.name == CheckExpiredSendingLease {
			expiredLeaseCheck = check
			break
		}
	}
	if !expiredLeaseCheck.usesExpiredGrace {
		t.Fatal("expired lease check does not use the configured grace period")
	}
	if !strings.Contains(expiredLeaseCheck.query, ">") {
		t.Fatalf("expired lease query %q does not require expiry to exceed grace", expiredLeaseCheck.query)
	}
	if !strings.Contains(expiredLeaseCheck.query, "$2::double precision") {
		t.Fatalf("expired lease query %q does not use the configured grace argument", expiredLeaseCheck.query)
	}
}

func TestCheckNamesAndSeveritiesAreStable(t *testing.T) {
	want := []struct {
		name     CheckName
		severity Severity
	}{
		{CheckStoppedMailingWithClaimableDelivery, SeverityError},
		{CheckCancelledRunWithClaimableDelivery, SeverityError},
		{CheckSendingDeliveryWithoutLease, SeverityError},
		{CheckExpiredSendingLease, SeverityWarning},
		{CheckRunStatusTimestampContradiction, SeverityError},
		{CheckMailingRunStatusContradiction, SeverityError},
	}
	if len(checks) != len(want) {
		t.Fatalf("check count = %d, want %d", len(checks), len(want))
	}
	for index, expected := range want {
		if checks[index].name != expected.name || checks[index].severity != expected.severity {
			t.Fatalf("check %d = %q/%q, want %q/%q", index, checks[index].name, checks[index].severity, expected.name, expected.severity)
		}
	}
}
