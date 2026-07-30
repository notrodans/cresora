package operatorcredentials

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// Terminal is the real TTY implementation used by cmd/operator-admin.
// Password reads use term.ReadPassword, which disables terminal echo.
type Terminal struct {
	input  *os.File
	output io.Writer
	reader *bufio.Reader
}

// NewTerminal refuses pipes and redirected input. The command therefore
// cannot accidentally consume a password from stdin or a process pipeline.
func NewTerminal(input *os.File, output io.Writer) (*Terminal, error) {
	if input == nil || !term.IsTerminal(int(input.Fd())) {
		return nil, ErrTTYRequired
	}
	if output == nil {
		return nil, errors.New("operator credential bootstrap output is required")
	}
	return &Terminal{
		input:  input,
		output: output,
		reader: bufio.NewReader(input),
	}, nil
}

func (terminal *Terminal) IsTTY() bool {
	return term.IsTerminal(int(terminal.input.Fd()))
}

func (terminal *Terminal) ReadUsername() (string, error) {
	if !terminal.IsTTY() {
		return "", ErrTTYRequired
	}
	if _, err := io.WriteString(terminal.output, "Username: "); err != nil {
		return "", err
	}
	value, err := terminal.reader.ReadString('\n')
	if err != nil && len(value) == 0 {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(value, "\n"), "\r"), nil
}

func (terminal *Terminal) ReadPassword(prompt string) (string, error) {
	if !terminal.IsTTY() {
		return "", ErrTTYRequired
	}
	if _, err := io.WriteString(terminal.output, prompt); err != nil {
		return "", err
	}
	value, err := term.ReadPassword(int(terminal.input.Fd()))
	// term.ReadPassword returns mutable bytes, so erase that buffer immediately
	// after the unavoidable conversion to a Go string. Go strings are immutable
	// and cannot be reliably zeroed; the service keeps this limitation short by
	// hashing the value immediately and never persisting or logging it.
	password := string(value)
	clear(value)
	// The newline is safe output and restores a readable prompt after the
	// terminal's echo-disabled line.
	_, newlineErr := io.WriteString(terminal.output, "\n")
	if err != nil {
		return "", fmt.Errorf("read hidden terminal input: %w", err)
	}
	if newlineErr != nil {
		return "", newlineErr
	}
	return password, nil
}

func (terminal *Terminal) Write(value string) error {
	_, err := io.WriteString(terminal.output, value)
	return err
}
