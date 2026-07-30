package main

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestRunRejectsEveryArgumentBeforeOpeningTTYOrDatabase(t *testing.T) {
	originalArguments := os.Args
	defer func() { os.Args = originalArguments }()
	for _, argument := range []string{"--help", "reset", "-username=admin"} {
		os.Args = []string{"operator-admin", argument}
		if err := run(context.Background()); !errors.Is(err, errUnexpectedArguments) {
			t.Fatalf("argument test case should be rejected: argument-count=%d", len(os.Args)-1)
		}
	}
}
