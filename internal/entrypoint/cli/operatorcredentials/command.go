// Package operatorcredentials provides the local TTY-only operator credential
// bootstrap/reset command core. It has no network or browser-login behavior.
package operatorcredentials

import (
	"context"
	"errors"
	"fmt"

	application "github.com/notrodans/cresora/internal/application/operatorcredentials"
)

var (
	ErrTTYRequired      = errors.New("operator credential bootstrap requires a TTY")
	ErrPasswordMismatch = errors.New("password entries do not match")
)

// UI is the deliberately small prompt surface used by Run. Implementations
// must read passwords without echo and must not write them anywhere.
type UI interface {
	IsTTY() bool
	ReadUsername() (string, error)
	ReadPassword(prompt string) (string, error)
	Write(string) error
}

// Run executes the interactive bootstrap/reset flow. Passwords are compared
// only in memory and are never passed to UI.Write or the repository.
func Run(context context.Context, ui UI, service application.Service) error {
	if context == nil {
		return fmt.Errorf("run operator credential bootstrap: %w", ErrTTYRequired)
	}
	if ui == nil || !ui.IsTTY() {
		return ErrTTYRequired
	}
	username, failure := ui.ReadUsername()
	if failure != nil {
		return fmt.Errorf("read operator username: %w", failure)
	}
	// Reject terminal-spoofing input before emitting the password prompts or
	// ever allowing the username into the result output.
	if failure = application.ValidateUsername(username); failure != nil {
		return failure
	}
	first, failure := ui.ReadPassword("Password: ")
	if failure != nil {
		return fmt.Errorf("read operator password: %w", failure)
	}
	second, failure := ui.ReadPassword("Repeat password: ")
	if failure != nil {
		return fmt.Errorf("read repeated operator password: %w", failure)
	}
	if first != second {
		return ErrPasswordMismatch
	}

	operator, failure := service.BootstrapOrReset(context, username, first)
	if failure != nil {
		return failure
	}
	if failure = ui.Write(fmt.Sprintf("Operator credential configured for %s.\n", operator.Username)); failure != nil {
		return fmt.Errorf("write operator credential result: %w", failure)
	}
	return nil
}
