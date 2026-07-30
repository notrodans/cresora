package authentication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	operatorsessions "github.com/notrodans/cresora/internal/application/operatorsessions"
)

type testCredentialRepository struct {
	credential operatorsessions.Credential
}

func (repository testCredentialRepository) FindCredential(context.Context, string) (operatorsessions.Credential, error) {
	return repository.credential, nil
}

type testSessionRepository struct {
	stored  operatorsessions.StoredSession
	created []byte
	creates int
	revoked []byte
}

func (repository *testSessionRepository) CreateSession(_ context.Context, operatorID uuid.UUID, _, _ string, hash []byte) (operatorsessions.StoredSession, error) {
	repository.creates++
	repository.created = append([]byte(nil), hash...)
	repository.stored.OperatorID = operatorID
	if repository.stored.ID == uuid.Nil {
		repository.stored.ID = uuid.New()
	}
	return repository.stored, nil
}

func (repository *testSessionRepository) FindValidSession(context.Context, []byte) (operatorsessions.StoredSession, error) {
	return repository.stored, nil
}

func (repository *testSessionRepository) RevokeSession(_ context.Context, hash []byte) error {
	repository.revoked = append([]byte(nil), hash...)
	return nil
}

func (*testSessionRepository) RevokeOperatorSessions(context.Context, uuid.UUID) error { return nil }

func newAuthenticationTestHandler(repository *testSessionRepository) http.Handler {
	return newAuthenticationTestHandlerWithLimiter(repository, NewLoginRateLimiter(DefaultLoginRateLimitConfig()))
}

func newAuthenticationTestHandlerWithLimiter(repository *testSessionRepository, limiter *LoginRateLimiter) http.Handler {
	service := operatorsessions.NewServiceWithVerifier(
		testCredentialRepository{credential: operatorsessions.Credential{OperatorID: uuid.New(), Username: "admin", PasswordHash: "hash", Enabled: true}},
		repository,
		operatorsessions.VerifyFunc(func(string, string) (bool, error) { return true, nil }),
	)
	cookie := CookieConfig{Name: "__Host-cresora_session", Secure: true}
	provider := NewSessionProvider(service, cookie)
	return NewWithRateLimiter(service, provider, "https://example.test", cookie, limiter)
}

func TestLoginRotatesSessionAndSetsHostCookiePolicy(t *testing.T) {
	repository := &testSessionRepository{stored: operatorsessions.StoredSession{CreatedAt: time.Now(), LastSeenAt: time.Now(), IdleExpiresAt: time.Now().Add(12 * time.Hour), AbsoluteExpiresAt: time.Now().Add(7 * 24 * time.Hour)}}
	handler := newAuthenticationTestHandler(repository)
	get := httptest.NewRequest(http.MethodGet, "https://example.test/login?next=/console", nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || getResponse.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("login GET: %d headers=%v", getResponse.Code, getResponse.Header())
	}
	assertAntiFramingHeaders(t, getResponse)
	cookies := getResponse.Result().Cookies()
	csrf := cookieValue(cookies, preAuthCSRFCookie)
	if csrf == "" {
		t.Fatal("login GET did not issue pre-auth CSRF cookie")
	}

	form := url.Values{"username": {"admin"}, "password": {"correct horse battery staple"}, csrfFormField: {csrf}, "next": {"/console"}}
	post := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader(form.Encode()))
	post.Header.Set("Origin", "https://example.test")
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	post.AddCookie(&http.Cookie{Name: preAuthCSRFCookie, Value: csrf})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, post)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/console" {
		t.Fatalf("login POST: %d location=%q body=%s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	assertAntiFramingHeaders(t, response)
	var sessionCookie *http.Cookie
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == "__Host-cresora_session" && cookie.MaxAge > 0 {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil || len(sessionCookie.Value) != 43 || !sessionCookie.Secure || !sessionCookie.HttpOnly || sessionCookie.SameSite != http.SameSiteLaxMode || sessionCookie.Path != "/" || sessionCookie.Domain != "" {
		t.Fatalf("unexpected session cookie: %#v", sessionCookie)
	}
	if len(repository.created) != 32 {
		t.Fatalf("persisted token hash length = %d", len(repository.created))
	}
}

func TestLoginErrorFlowSetsAntiFramingHeaders(t *testing.T) {
	handler := newAuthenticationTestHandler(&testSessionRepository{})
	request := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader("username=admin"))
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusSeeOther {
		t.Fatalf("login error flow status = %d, want %d", response.Code, http.StatusSeeOther)
	}
	assertAntiFramingHeaders(t, response)
}

func TestLoginRejectsForeignOriginAndOpenRedirect(t *testing.T) {
	repository := &testSessionRepository{}
	handler := newAuthenticationTestHandler(repository)
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "https://example.test/login?next=https://foreign.test/steal", nil))
	csrf := cookieValue(get.Result().Cookies(), preAuthCSRFCookie)
	form := url.Values{"username": {"admin"}, "password": {"correct horse battery staple"}, csrfFormField: {csrf}, "next": {"https://foreign.test/steal"}}
	request := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader(form.Encode()))
	request.Header.Set("Origin", "https://foreign.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.AddCookie(&http.Cookie{Name: preAuthCSRFCookie, Value: csrf})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/login?notice=refresh" || repository.created != nil {
		t.Fatalf("foreign origin: status=%d location=%q created=%v", response.Code, response.Header().Get("Location"), repository.created)
	}
}

func TestLoginRejectsMissingCSRFWithFreshGETRedirect(t *testing.T) {
	repository := &testSessionRepository{}
	handler := newAuthenticationTestHandler(repository)
	form := url.Values{"username": {"admin"}, "password": {"correct horse battery staple"}, csrfFormField: {strings.Repeat("a", 43)}, "next": {"/console"}}
	request := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader(form.Encode()))
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/login?next=%2Fconsole&notice=refresh" || repository.creates != 0 {
		t.Fatalf("missing CSRF response: status=%d location=%q creates=%d", response.Code, response.Header().Get("Location"), repository.creates)
	}
}

func TestLoginRecoveryNoticeDoesNotReflectRejectedRequest(t *testing.T) {
	handler := newAuthenticationTestHandler(&testSessionRepository{})
	request := httptest.NewRequest(http.MethodPost, "https://example.test/login?next=//foreign.test/steal", strings.NewReader(url.Values{
		"username":    {"secret-user"},
		"password":    {"secret-password"},
		"next":        {"//foreign.test/steal"},
		csrfFormField: {"rejected-csrf"},
	}.Encode()))
	request.Header.Set("Origin", "https://foreign.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	location := response.Header().Get("Location")
	parsed, failure := url.Parse(location)
	if failure != nil || parsed.Path != "/login" || parsed.Query().Get("notice") != loginRefreshNotice || parsed.Query().Get("next") != "" {
		t.Fatalf("unsafe login recovery location: %q", location)
	}
	for _, secret := range []string{"secret-user", "secret-password", "rejected-csrf", "foreign.test"} {
		if strings.Contains(location, secret) {
			t.Fatalf("rejected login value was reflected in location %q", location)
		}
	}
}

func TestLoginRateLimitIsCanonicalBoundedAndGloballyCapped(t *testing.T) {
	clock := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	limiter := NewLoginRateLimiter(LoginRateLimitConfig{
		Window:          time.Second,
		AttemptsPerKey:  2,
		GlobalAttempts:  100,
		MaxEntries:      2,
		CleanupInterval: time.Second,
	})
	limiter.now = func() time.Time { return clock }
	if !limiter.Allow(" Admin ", "203.0.113.10:1000") || !limiter.Allow("admin", "203.0.113.10:2000") {
		t.Fatal("canonical username/IP attempts were rejected too early")
	}
	if limiter.Allow("ADMIN", "203.0.113.10:3000") {
		t.Fatal("canonical username/IP was not throttled")
	}
	if !limiter.Allow("admin", "203.0.113.11:3000") {
		t.Fatal("a different direct IP was incorrectly grouped")
	}
	if limiter.Len() != 2 {
		t.Fatalf("rate limiter entries = %d, want 2", limiter.Len())
	}
	if limiter.Allow("another", "203.0.113.12:3000") {
		t.Fatal("full bounded rate limiter table accepted a new identity")
	}

	clock = clock.Add(2 * time.Second)
	allowed := limiter.Allow("another", "203.0.113.12:3000")
	if !allowed || limiter.Len() != 1 {
		t.Fatalf("expired rate limiter entries were not cleaned: allowed=%t len=%d", allowed, limiter.Len())
	}

	global := NewLoginRateLimiter(LoginRateLimitConfig{Window: time.Minute, AttemptsPerKey: 100, GlobalAttempts: 2, MaxEntries: 10, CleanupInterval: time.Minute})
	if !global.Allow("one", "192.0.2.1:1") || !global.Allow("two", "192.0.2.2:1") || global.Allow("three", "192.0.2.3:1") {
		t.Fatal("global login safety cap was not enforced")
	}
}

func TestThrottledLoginIsGenericAndDoesNotCallAuthenticator(t *testing.T) {
	repository := &testSessionRepository{}
	limiter := NewLoginRateLimiter(LoginRateLimitConfig{Window: time.Minute, AttemptsPerKey: 1, GlobalAttempts: 10, MaxEntries: 10, CleanupInterval: time.Minute})
	handler := newAuthenticationTestHandlerWithLimiter(repository, limiter)
	login := func() *httptest.ResponseRecorder {
		get := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "https://example.test/login", nil)
		handler.ServeHTTP(get, request)
		csrf := cookieValue(get.Result().Cookies(), preAuthCSRFCookie)
		form := url.Values{"username": {"admin"}, "password": {"correct horse battery staple"}, csrfFormField: {csrf}}
		post := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader(form.Encode()))
		post.RemoteAddr = "198.51.100.8:4444"
		post.Header.Set("Origin", "https://example.test")
		post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		post.AddCookie(&http.Cookie{Name: preAuthCSRFCookie, Value: csrf})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, post)
		return response
	}
	if first := login(); first.Code != http.StatusSeeOther {
		t.Fatalf("first login status = %d", first.Code)
	}
	second := login()
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `role="alert"`) || repository.creates != 1 {
		t.Fatalf("throttled login was not generic: status=%d creates=%d body=%q", second.Code, repository.creates, second.Body.String())
	}
}

func TestCookieConfigRequiresExplicitLocalInsecureModeAndHostPolicy(t *testing.T) {
	if err := validateCookieConfig(CookieConfig{Name: "__Host-cresora_session", Secure: true}); err != nil {
		t.Fatalf("valid host cookie rejected: %v", err)
	}
	if err := validateCookieConfig(CookieConfig{Name: "cresora_session", AllowInsecureLocal: true}); err != nil {
		t.Fatalf("valid explicit local cookie rejected: %v", err)
	}
	for _, cookie := range []CookieConfig{
		{Name: "cresora_session", Secure: true},
		{Name: "__Host-cresora_session"},
		{Name: "cresora_session"},
		{Name: "cresora_session", Secure: false, AllowInsecureLocal: true},
	} {
		if cookie.Name == "cresora_session" && !cookie.AllowInsecureLocal {
			if err := validateCookieConfig(cookie); err == nil {
				t.Fatalf("insecure cookie without local opt-in was accepted: %#v", cookie)
			}
			continue
		}
		if cookie.Name == "cresora_session" && cookie.AllowInsecureLocal {
			continue
		}
		if err := validateCookieConfig(cookie); err == nil {
			t.Fatalf("invalid cookie policy was accepted: %#v", cookie)
		}
	}
}

func TestLogoutRequiresSessionBoundCSRFAndRevokes(t *testing.T) {
	repository := &testSessionRepository{stored: operatorsessions.StoredSession{OperatorID: uuid.New(), ID: uuid.New(), CreatedAt: time.Now(), LastSeenAt: time.Now(), IdleExpiresAt: time.Now().Add(12 * time.Hour), AbsoluteExpiresAt: time.Now().Add(7 * 24 * time.Hour)}}
	handler := newAuthenticationTestHandler(repository)
	token := strings.Repeat("a", 43)
	csrf, ok := operatorsessions.SessionCSRFToken(token)
	if !ok {
		t.Fatal("derive CSRF token")
	}
	form := url.Values{csrfFormField: {csrf}}
	request := httptest.NewRequest(http.MethodPost, "https://example.test/logout", strings.NewReader(form.Encode()))
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.AddCookie(&http.Cookie{Name: "__Host-cresora_session", Value: token})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/login" || len(repository.revoked) != 32 {
		t.Fatalf("logout: status=%d location=%q revoked=%d", response.Code, response.Header().Get("Location"), len(repository.revoked))
	}
	if cookieValue(response.Result().Cookies(), "__Host-cresora_session") != "" {
		t.Fatal("logout did not expire session cookie")
	}
}

func TestLogoutInvalidCSRFOrOriginRecoversWithoutRevoking(t *testing.T) {
	for _, test := range []struct {
		name       string
		origin     string
		csrf       string
		wantStatus int
		wantTarget string
	}{
		{name: "invalid csrf", origin: "https://example.test", csrf: "wrong", wantStatus: http.StatusSeeOther, wantTarget: "/?notice=retry"},
		{name: "foreign origin", origin: "https://foreign.test", csrf: "wrong", wantStatus: http.StatusSeeOther, wantTarget: "/?notice=retry"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := &testSessionRepository{stored: operatorsessions.StoredSession{OperatorID: uuid.New(), ID: uuid.New(), CreatedAt: time.Now(), LastSeenAt: time.Now(), IdleExpiresAt: time.Now().Add(time.Hour), AbsoluteExpiresAt: time.Now().Add(time.Hour)}}
			handler := newAuthenticationTestHandler(repository)
			request := httptest.NewRequest(http.MethodPost, "https://example.test/logout", strings.NewReader(url.Values{csrfFormField: {test.csrf}}.Encode()))
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.AddCookie(&http.Cookie{Name: "__Host-cresora_session", Value: strings.Repeat("a", 43)})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus || response.Header().Get("Location") != test.wantTarget || repository.revoked != nil {
				t.Fatalf("logout recovery: status=%d location=%q revoked=%v", response.Code, response.Header().Get("Location"), repository.revoked)
			}
		})
	}

	unauthenticated := newAuthenticationTestHandler(&testSessionRepository{})
	request := httptest.NewRequest(http.MethodPost, "https://example.test/logout", strings.NewReader(url.Values{csrfFormField: {"wrong"}}.Encode()))
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	unauthenticated.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("logout without a valid session: status=%d body=%q", response.Code, response.Body.String())
	}
}

func cookieValue(cookies []*http.Cookie, name string) string {
	for _, cookie := range cookies {
		if cookie.Name == name && cookie.MaxAge >= 0 {
			return cookie.Value
		}
	}
	return ""
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
