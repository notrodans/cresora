package principal

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	application "github.com/notrodans/nebula-go/internal/application"
)

func TestStaticProviderMiddlewarePassesTrustedActor(t *testing.T) {
	operatorID := uuid.New()
	var got application.Actor
	handler := Middleware(Static(operatorID))(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var ok bool
		got, ok = FromContext(request.Context())
		if !ok {
			t.Fatal("expected request actor")
		}
		response.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "http://example.test/?operator_id="+uuid.NewString(), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("middleware status: %d", response.Code)
	}
	if got.OperatorID != operatorID {
		t.Fatalf("middleware actor: got %s, want %s", got.OperatorID, operatorID)
	}
}

func TestMiddlewareRejectsUnavailableStaticActor(t *testing.T) {
	handler := Middleware(Static(uuid.Nil))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unavailable actor status: %d", response.Code)
	}
}
