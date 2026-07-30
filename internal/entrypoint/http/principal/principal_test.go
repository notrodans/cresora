package principal

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
)

func TestProviderFuncMiddlewarePassesTrustedActor(t *testing.T) {
	operatorID := uuid.New()
	var got application.Actor
	provider := ProviderFunc(func(*http.Request) (application.Actor, error) {
		return application.Actor{OperatorID: operatorID}, nil
	})
	handler := Middleware(provider)(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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
	assertAntiFramingHeaders(t, response)
}

func TestMiddlewareRejectsUnavailableProvider(t *testing.T) {
	provider := ProviderFunc(func(*http.Request) (application.Actor, error) {
		return application.Actor{}, ErrUnavailable
	})
	handler := Middleware(provider)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unavailable actor status: %d", response.Code)
	}
	assertAntiFramingHeaders(t, response)
}

type testSessionProvider struct {
	actor application.Actor
	err   error
}

func (provider testSessionProvider) Provide(*http.Request) (application.Actor, error) {
	return provider.actor, provider.err
}

func (provider testSessionProvider) ProvideSession(*http.Request) (application.Actor, string, error) {
	return provider.actor, "", provider.err
}

func TestSessionMiddlewareSetsAntiFramingHeadersOnRedirect(t *testing.T) {
	handler := Middleware(testSessionProvider{err: errors.New("session unavailable")})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler should not run")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/console?tab=mailings", nil))

	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/login?next=%2Fconsole%3Ftab%3Dmailings" {
		t.Fatalf("session redirect: status=%d location=%q", response.Code, response.Header().Get("Location"))
	}
	assertAntiFramingHeaders(t, response)
}

func assertAntiFramingHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if got := response.Header().Get("Content-Security-Policy"); got != "frame-ancestors 'none'" {
		t.Fatalf("Content-Security-Policy = %q, want frame-ancestors policy", got)
	}
	if got := response.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q, want DENY", got)
	}
}
