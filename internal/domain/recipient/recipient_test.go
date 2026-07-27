package recipient

import (
	"testing"

	"github.com/google/uuid"
)

func TestIdentityUUID(t *testing.T) {
	expected := uuid.New()

	actual := Identity(expected).UUID()

	if actual != expected {
		t.Fatalf("expected recipient UUID %s, got %s", expected, actual)
	}
}
