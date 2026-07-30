package operatorcredentials

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	application "github.com/notrodans/nebula-go/internal/application/operatorcredentials"
)

type fakeUI struct {
	tty       bool
	username  string
	passwords []string
	output    bytes.Buffer
	reads     int
}

func (ui *fakeUI) IsTTY() bool { return ui.tty }

func (ui *fakeUI) ReadUsername() (string, error) { return ui.username, nil }

func (ui *fakeUI) ReadPassword(_ string) (string, error) {
	password := ui.passwords[ui.reads]
	ui.reads++
	return password, nil
}

func (ui *fakeUI) Write(value string) error {
	_, err := ui.output.WriteString(value)
	return err
}

type fakeRepository struct {
	operator application.Operator
	username string
	hash     string
	called   int
}

func (repository *fakeRepository) BootstrapOrReset(_ context.Context, username, hash string) (application.Operator, error) {
	repository.username = username
	repository.hash = hash
	repository.called++
	return repository.operator, nil
}

type fakeHasher struct {
	password string
}

func (hasher *fakeHasher) Hash(password string) (string, error) {
	hasher.password = password
	return "redacted-test-hash", nil
}

func newTestService(repository *fakeRepository, hasher *fakeHasher) application.Service {
	return application.NewService(repository, hasher)
}

func TestRunRequiresTTY(t *testing.T) {
	ui := &fakeUI{tty: false}
	if err := Run(context.Background(), ui, application.Service{}); !errors.Is(err, ErrTTYRequired) {
		t.Fatalf("expected TTY error, got %v", err)
	}
	if ui.reads != 0 {
		t.Fatalf("non-TTY flow read a password")
	}
}

func TestRunRejectsTerminalSpoofingUsernameBeforePasswordPrompts(t *testing.T) {
	ui := &fakeUI{tty: true, username: "admin\u202eoperator", passwords: []string{"unused password", "unused password"}}
	repository := &fakeRepository{}
	hasher := &fakeHasher{}
	if err := Run(context.Background(), ui, newTestService(repository, hasher)); !errors.Is(err, application.ErrInvalidInput) {
		t.Fatalf("expected invalid username error, got %v", err)
	}
	if ui.reads != 0 || repository.called != 0 || hasher.password != "" {
		t.Fatalf("spoofing username reached credential flow: reads=%d calls=%d password-was-passed=%t", ui.reads, repository.called, hasher.password != "")
	}
}

func TestNewTerminalRejectsRedirectedInput(t *testing.T) {
	input, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open null input: %v", err)
	}
	defer input.Close()
	if _, err = NewTerminal(input, &bytes.Buffer{}); !errors.Is(err, ErrTTYRequired) {
		t.Fatalf("expected redirected input to be rejected, got %v", err)
	}
}

func TestRunRejectsPasswordMismatchWithoutPersisting(t *testing.T) {
	ui := &fakeUI{tty: true, username: "admin", passwords: []string{"first password", "second password"}}
	repository := &fakeRepository{}
	hasher := &fakeHasher{}
	if err := Run(context.Background(), ui, newTestService(repository, hasher)); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("expected mismatch error, got %v", err)
	}
	if repository.called != 0 || hasher.password != "" || ui.output.Len() != 0 {
		t.Fatalf("mismatch reached persistence or output: calls=%d password-was-passed=%t output-written=%t", repository.called, hasher.password != "", ui.output.Len() != 0)
	}
}

func TestRunUsesDoubleEntryAndDoesNotOutputSecrets(t *testing.T) {
	secret := "a very secret password"
	ui := &fakeUI{tty: true, username: "admin", passwords: []string{secret, secret}}
	repository := &fakeRepository{operator: application.Operator{Username: "admin"}}
	hasher := &fakeHasher{}
	if err := Run(context.Background(), ui, newTestService(repository, hasher)); err != nil {
		t.Fatalf("run bootstrap: %v", err)
	}
	if repository.called != 1 || repository.username != "admin" || repository.hash == secret || hasher.password != secret {
		t.Fatalf("unexpected credential flow: repository-called=%d username=%q hash-is-plaintext=%t password-was-passed=%t", repository.called, repository.username, repository.hash == secret, hasher.password == secret)
	}
	if strings.Contains(ui.output.String(), secret) || strings.Contains(ui.output.String(), repository.hash) {
		t.Fatalf("secret material appeared in UI output: plaintext-present=%t hash-present=%t", strings.Contains(ui.output.String(), secret), strings.Contains(ui.output.String(), repository.hash))
	}
	if !strings.Contains(ui.output.String(), "admin") {
		t.Fatalf("safe operator identity was not reported: %q", ui.output.String())
	}
}
