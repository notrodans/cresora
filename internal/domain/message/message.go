package message

import (
	"fmt"
	"io"
)

var _ Message = text{}

// Представляет неизменяемое содержимое сообщения рассылки
type Message interface {
	Print(io.Writer) error
}

// Хранит неизменяемое содержимое сообщения в виде обычного текста
type text struct {
	body string
}

func Text(body string) Message {
	return text{
		body: body,
	}
}

func (message text) Print(destination io.Writer) error {
	if destination == nil {
		panic("print mailing message without destination")
	}
	if message.body == "" {
		panic("print mailing message with empty body")
	}
	if _, failure := io.WriteString(destination, message.body); failure != nil {
		return fmt.Errorf("print mailing message body: %w", failure)
	}
	return nil
}
