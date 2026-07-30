package delivery

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestClassifyPreservesWrappedTaxonomy(t *testing.T) {
	original := errors.New("remote failure")
	tests := []struct {
		name       string
		failure    error
		kind       FailureKind
		retryAfter time.Duration
	}{
		{
			name:    "permanent",
			failure: fmt.Errorf("outer: %w", WrapPermanent(original)),
			kind:    FailurePermanent,
		},
		{
			name:    "transient",
			failure: fmt.Errorf("outer: %w", WrapTransient(original)),
			kind:    FailureTransient,
		},
		{
			name:       "flood wait",
			failure:    fmt.Errorf("outer: %w", &FloodWaitError{Duration: 7 * time.Second}),
			kind:       FailureFloodWait,
			retryAfter: 7 * time.Second,
		},
		{
			name:    "unknown",
			failure: errors.New("untyped failure"),
			kind:    FailureUnknown,
		},
		{
			name:    "invalid flood wait",
			failure: &FloodWaitError{Duration: 0},
			kind:    FailureUnknown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classification := Classify(test.failure)
			if classification.Kind != test.kind || classification.RetryAfter != test.retryAfter {
				t.Fatalf("classification = %+v, want kind %d and delay %s", classification, test.kind, test.retryAfter)
			}
		})
	}

	if !errors.Is(tests[0].failure, ErrPermanent) || !errors.Is(tests[0].failure, original) {
		t.Fatal("permanent wrapping did not preserve taxonomy and original error")
	}
	var floodWait *FloodWaitError
	if !errors.As(tests[2].failure, &floodWait) || floodWait.Duration != 7*time.Second {
		t.Fatalf("flood wait wrapping did not preserve typed error: %v", tests[2].failure)
	}
}

func TestBoundedErrorMessageUsesCodePointsAndFallback(t *testing.T) {
	message := strings.Repeat("界", MaxErrorMessageCodePoints+20)
	bounded := BoundedErrorMessage(errors.New(message))
	if len([]rune(bounded)) != MaxErrorMessageCodePoints {
		t.Fatalf("bounded message has %d code points, want %d", len([]rune(bounded)), MaxErrorMessageCodePoints)
	}
	if fallback := BoundedErrorMessage(nil); fallback == "" {
		t.Fatal("nil error did not receive a fallback diagnostic")
	}
	if fallback := BoundedErrorMessage(errors.New("")); fallback == "" {
		t.Fatal("empty error did not receive a fallback diagnostic")
	}
}

func TestBoundedErrorMessageSanitizesPostgreSQLHazards(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "NUL and surrounding whitespace", err: errors.New(" \x00telegram failure\x00 "), want: "telegram failure"},
		{name: "whitespace only", err: errors.New(" \t\n\u2003 "), want: defaultErrorMessage},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := BoundedErrorMessage(test.err)
			if got != test.want {
				t.Fatalf("bounded message = %q, want %q", got, test.want)
			}
			if strings.ContainsRune(got, '\x00') || !utf8.ValidString(got) || strings.TrimSpace(got) == "" {
				t.Fatalf("bounded message is not PostgreSQL-safe: %q", got)
			}
		})
	}
}

func TestBoundedErrorMessageRepairsInvalidUTF8AndTruncationWhitespace(t *testing.T) {
	invalid := string([]byte{'a', 0xff, 'b'})
	got := BoundedErrorMessage(errors.New(invalid))
	if got != "a�b" || !utf8.ValidString(got) {
		t.Fatalf("invalid UTF-8 message = %q, want repaired valid UTF-8", got)
	}

	truncated := BoundedErrorMessage(errors.New(strings.Repeat("x", MaxErrorMessageCodePoints-1) + " "))
	if len([]rune(truncated)) != MaxErrorMessageCodePoints-1 || strings.HasSuffix(truncated, " ") {
		t.Fatalf("truncated message = %q, want trimmed message within bound", truncated)
	}

	whitespacePrefix := BoundedErrorMessage(errors.New(strings.Repeat(" ", MaxErrorMessageCodePoints+1)))
	if whitespacePrefix != defaultErrorMessage {
		t.Fatalf("truncated whitespace message = %q, want fallback %q", whitespacePrefix, defaultErrorMessage)
	}
}
