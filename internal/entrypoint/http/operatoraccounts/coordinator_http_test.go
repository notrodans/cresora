package operatoraccounts

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	application "github.com/notrodans/cresora/internal/application"
	challengecoordinator "github.com/notrodans/cresora/internal/application/operatoraccountauth/challenges"
	challengefake "github.com/notrodans/cresora/internal/application/operatoraccountauth/challenges/fake"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
)

func TestCoordinatorHTTPUsesOpaqueRequestIDsAndSafeProjection(t *testing.T) {
	operator := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(operator)
	telegramFake := challengefake.New("12345")
	coordinator := challengecoordinator.NewWithProviders(telegramFake, telegramFake)
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if failure := coordinator.Shutdown(shutdownContext); failure != nil {
			t.Errorf("challenge coordinator did not shut down: %v", failure)
		}
	})
	handler := NewWithChallengeCoordinator(coordinator, provider, "http://example.test", RouteOptions{
		Mode:                     RouteDevelopmentTestMock,
		Environment:              EnvironmentTesting,
		Cookie:                   SecureCookieConfig(),
		AllowDevelopmentTestMock: true,
	})
	jar := newCookieJar()
	page := operatorPage(t, handler, jar)
	if strings.Contains(page, "fake-phone-") || strings.Contains(page, "fake-qr-") {
		t.Fatalf("provider handle leaked before a challenge: %s", page)
	}

	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone", mapValues("phone", "+15551234567")); response.Code != http.StatusSeeOther {
		t.Fatalf("phone start: status=%d", response.Code)
	}
	page = operatorPage(t, handler, jar)
	if !strings.Contains(page, `name="challenge_request_id"`) || !strings.Contains(page, `name="code"`) {
		t.Fatalf("phone page did not render opaque request ID and safe code form: %s", page)
	}
	if strings.Contains(page, "fake-phone-") {
		t.Fatalf("phone provider handle leaked to browser: %s", page)
	}

	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", mapValues("code", "12345")); response.Code != http.StatusSeeOther {
		t.Fatalf("phone completion: status=%d location=%q", response.Code, response.Header().Get("Location"))
	}
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr", mapValues()); response.Code != http.StatusSeeOther {
		t.Fatalf("QR start: status=%d", response.Code)
	}
	page = operatorPage(t, handler, jar)
	if !strings.Contains(page, `class="qr"`) || !strings.Contains(page, `name="challenge_request_id"`) {
		t.Fatalf("QR safe projection was not rendered: %s", page)
	}
	if strings.Contains(page, "fake-qr-") {
		t.Fatalf("QR provider handle leaked to browser: %s", page)
	}
}

// mapValues keeps this test independent of the url.Values literal's handling
// of a no-value form while reusing the existing authenticated HTTP helpers.
func mapValues(values ...string) url.Values {
	result := make(url.Values)
	for index := 0; index+1 < len(values); index += 2 {
		result[values[index]] = []string{values[index+1]}
	}
	return result
}

var _ principal.Provider = (*testActorProvider)(nil)
