package operatoraccounts

import (
	"context"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	authmock "github.com/notrodans/cresora/internal/application/operatoraccountauth/mock"
	requests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
)

type testActorProvider struct {
	actor atomic.Value
}

func newTestActorProvider(actor application.Actor) *testActorProvider {
	provider := &testActorProvider{}
	provider.actor.Store(actor)
	return provider
}

func (provider *testActorProvider) Provide(*http.Request) (application.Actor, error) {
	return provider.actor.Load().(application.Actor), nil
}

type cookieJar struct {
	mu      sync.Mutex
	cookies map[string]*http.Cookie
}

const localOperatorOrigin = "http://localhost"

func newCookieJar() *cookieJar {
	return &cookieJar{cookies: make(map[string]*http.Cookie)}
}

func (jar *cookieJar) add(response *http.Response) {
	jar.mu.Lock()
	defer jar.mu.Unlock()
	for _, cookie := range response.Cookies() {
		jar.cookies[cookie.Name] = cookie
	}
}

func (jar *cookieJar) request(request *http.Request) {
	jar.mu.Lock()
	defer jar.mu.Unlock()
	for _, cookie := range jar.cookies {
		request.AddCookie(cookie)
	}
}

func (jar *cookieJar) csrf() string {
	return jar.csrfNamed(csrfCookie)
}

func (jar *cookieJar) csrfNamed(name string) string {
	jar.mu.Lock()
	defer jar.mu.Unlock()
	return jar.cookies[name].Value
}

func operatorRequest(t *testing.T, handler http.Handler, jar *cookieJar, method, path string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return operatorRequestAtOrigin(t, handler, jar, method, path, values, "http://example.test")
}

func operatorRequestAtOrigin(t *testing.T, handler http.Handler, jar *cookieJar, method, path string, values url.Values, origin string) *httptest.ResponseRecorder {
	t.Helper()
	var body *strings.Reader
	if values == nil {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader(values.Encode())
	}
	request := httptest.NewRequest(method, origin+path, body)
	if values != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if method == http.MethodPost {
		request.Header.Set("Origin", origin)
	}
	jar.request(request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	jar.add(response.Result())
	return response
}

func operatorPageAtOrigin(t *testing.T, handler http.Handler, jar *cookieJar, origin string) string {
	t.Helper()
	response := operatorRequestAtOrigin(t, handler, jar, http.MethodGet, "/operator-accounts/authenticate", nil, origin)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticate page: expected 200, got %d", response.Code)
	}
	return response.Body.String()
}

func operatorPostAtOrigin(t *testing.T, handler http.Handler, jar *cookieJar, path string, values url.Values, origin, csrfCookieName string) *httptest.ResponseRecorder {
	t.Helper()
	values.Set("csrf_token", jar.csrfNamed(csrfCookieName))
	return operatorRequestAtOrigin(t, handler, jar, http.MethodPost, path, values, origin)
}

func operatorPage(t *testing.T, handler http.Handler, jar *cookieJar) string {
	t.Helper()
	return operatorPageAtOrigin(t, handler, jar, "http://example.test")
}

func operatorPost(t *testing.T, handler http.Handler, jar *cookieJar, path string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return operatorPostAtOrigin(t, handler, jar, path, values, "http://example.test", csrfCookie)
}

func newMockOperatorHandler(provider principal.Provider) http.Handler {
	mock := authmock.New()
	return New(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, "http://example.test")
}

func TestOperatorAccountHTTPScopesBrowserFlowByActor(t *testing.T) {
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actorA)
	handler := newMockOperatorHandler(provider)
	jar := newCookieJar()

	operatorPage(t, handler, jar)
	phoneA := "+15551230001"
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone", url.Values{"phone": {phoneA}}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor A phone start: expected redirect, got %d", response.Code)
	}
	if page := operatorPage(t, handler, jar); !phoneInputVisible(page, phoneA) {
		t.Fatalf("actor A phone challenge was not rendered: %s", page)
	}

	// Reuse the same browser cookie under a different trusted actor. The
	// browser cookie is only a flow key and must not select actor A's state.
	provider.actor.Store(actorB)
	if page := operatorPage(t, handler, jar); phoneInputVisible(page, phoneA) || strings.Contains(page, `class="qr"`) {
		t.Fatalf("actor B received actor A browser state: %s", page)
	}
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", url.Values{"code": {authmock.MockPhoneCode}}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor B foreign phone verify: expected redirect, got %d", response.Code)
	}
	phoneB := "+15551230002"
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone", url.Values{"phone": {phoneB}}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor B phone start: expected redirect, got %d", response.Code)
	}

	provider.actor.Store(actorA)
	if page := operatorPage(t, handler, jar); !phoneInputVisible(page, phoneA) || phoneInputVisible(page, phoneB) {
		t.Fatalf("actor A phone state was not isolated from actor B: %s", page)
	}
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", url.Values{"code": {authmock.MockPhoneCode}}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor A phone verification: expected redirect, got %d", response.Code)
	}
	if page := operatorPage(t, handler, jar); phoneInputVisible(page, phoneA) || strings.Contains(page, `name="code"`) {
		t.Fatalf("status-none did not clear actor A phone cache: %s", page)
	}

	provider.actor.Store(actorB)
	if page := operatorPage(t, handler, jar); !phoneInputVisible(page, phoneB) || phoneInputVisible(page, phoneA) {
		t.Fatalf("actor B phone state was lost or contaminated: %s", page)
	}
}

func phoneInputVisible(page, phone string) bool {
	return strings.Contains(page, `value="&#43;`+strings.TrimPrefix(phone, "+")+`"`) || strings.Contains(page, `>&#43;`+strings.TrimPrefix(phone, "+")+`</strong>`)
}

func TestOperatorAccountHTTPScopesQRAndClearsStatusNone(t *testing.T) {
	t.Skip("QR authentication is intentionally not part of the phone-only flow")
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actorA)
	mock := authmock.New()
	handler := New(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr", url.Values{}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor A QR start: expected redirect, got %d", response.Code)
	}
	if page := operatorPage(t, handler, jar); !strings.Contains(page, `class="qr"`) {
		t.Fatal("actor A QR challenge was not rendered")
	}

	provider.actor.Store(actorB)
	if page := operatorPage(t, handler, jar); strings.Contains(page, `class="qr"`) {
		t.Fatal("actor B rendered actor A QR challenge")
	}
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor B foreign QR refresh: expected redirect, got %d", response.Code)
	}
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr", url.Values{}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor B QR start: expected redirect, got %d", response.Code)
	}
	pageB := operatorPage(t, handler, jar)
	if !strings.Contains(pageB, `class="qr"`) {
		t.Fatal("actor B QR challenge was not rendered")
	}

	provider.actor.Store(actorA)
	pageA := operatorPage(t, handler, jar)
	if !strings.Contains(pageA, `class="qr"`) {
		t.Fatal("actor A QR challenge was not restored for its actor-scoped flow")
	}
	provider.actor.Store(actorB)
	if response := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{}); response.Code != http.StatusSeeOther {
		t.Fatalf("actor B QR refresh: expected redirect, got %d", response.Code)
	}

	// A status response without a challenge is authoritative and must remove
	// the browser's cached QR fields rather than leave a stale image visible.
	provider.actor.Store(actorA)
	if page := operatorPage(t, handler, jar); !strings.Contains(page, `class="qr"`) {
		t.Fatal("actor A QR challenge unexpectedly disappeared before status-none check")
	}
	statusNone := &fixedStatus{}
	handler = New(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, statusNone, provider, "http://example.test")
	if page := operatorPage(t, handler, jar); strings.Contains(page, `class="qr"`) {
		t.Fatal("status-none did not clear stale QR browser cache")
	}
}

type fixedStatus struct {
	status common.Status
}

func (status *fixedStatus) Execute(context.Context, application.Actor) (common.Status, error) {
	return status.status, nil
}

func TestOperatorAccountHTTPUnknownQRResponsesAreEquivalent(t *testing.T) {
	t.Skip("QR authentication is intentionally not part of the phone-only flow")
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()
	status := &fixedStatus{}
	handler := New(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	status.status = common.Status{QRChallenge: &common.QRChallenge{
		RequestID: uuid.New(), URL: "tg://login?token=foreign", ExpiresAt: time.Now().Add(time.Minute),
	}}
	operatorPage(t, handler, jar)
	foreign := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{})
	if foreign.Code != http.StatusSeeOther {
		t.Fatalf("foreign QR refresh: expected redirect, got %d", foreign.Code)
	}

	status.status = common.Status{QRChallenge: &common.QRChallenge{
		RequestID: uuid.New(), URL: "tg://login?token=random", ExpiresAt: time.Now().Add(time.Minute),
	}}
	operatorPage(t, handler, jar)
	random := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{})
	if random.Code != foreign.Code || random.Header().Get("Location") != foreign.Header().Get("Location") {
		t.Fatalf("foreign and random QR responses differ: foreign=%d/%q random=%d/%q", foreign.Code, foreign.Header().Get("Location"), random.Code, random.Header().Get("Location"))
	}
}

func TestOperatorAccountHTTPForeignRealChallengesStayActorScoped(t *testing.T) {
	store := authmock.NewStore()
	actorA := application.Actor{OperatorID: uuid.New()}
	actorB := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actorA)
	status := &fixedStatus{}
	handler := New(
		authmock.NewStartPhone(store),
		authmock.NewVerifyPhone(store),
		authmock.NewStartQR(store),
		authmock.NewRefreshQR(store),
		status,
		provider,
		"http://example.test",
	)

	phoneA, failure := authmock.NewStartPhone(store).Execute(context.Background(), actorA, "+15551230031")
	if failure != nil {
		t.Fatalf("create actor A phone challenge: %v", failure)
	}
	phoneB, failure := authmock.NewStartPhone(store).Execute(context.Background(), actorB, "+15551230032")
	if failure != nil {
		t.Fatalf("create actor B phone challenge: %v", failure)
	}
	qrA, failure := authmock.NewStartQR(store).Execute(context.Background(), actorA)
	if failure != nil {
		t.Fatalf("create actor A QR challenge: %v", failure)
	}
	qrB, failure := authmock.NewStartQR(store).Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("create actor B QR challenge: %v", failure)
	}

	assertForeignPhoneHTTP(t, handler, provider, status, actorA, phoneB)
	assertForeignPhoneHTTP(t, handler, provider, status, actorB, phoneA)
	assertForeignQRHTTP(t, handler, provider, status, actorA, qrB)
	assertForeignQRHTTP(t, handler, provider, status, actorB, qrA)

	statusReader := authmock.NewStatus(store)
	statusA, failure := statusReader.Execute(context.Background(), actorA)
	if failure != nil {
		t.Fatalf("load actor A status: %v", failure)
	}
	statusB, failure := statusReader.Execute(context.Background(), actorB)
	if failure != nil {
		t.Fatalf("load actor B status: %v", failure)
	}
	if statusA.PhoneChallenge == nil || statusA.PhoneChallenge.RequestID != phoneA.RequestID {
		t.Fatalf("actor A phone challenge was consumed or replaced: %+v", statusA.PhoneChallenge)
	}
	if statusB.PhoneChallenge == nil || statusB.PhoneChallenge.RequestID != phoneB.RequestID {
		t.Fatalf("actor B phone challenge was consumed or replaced: %+v", statusB.PhoneChallenge)
	}
	if statusA.QRChallenge == nil || statusA.QRChallenge.RequestID != qrA.RequestID {
		t.Fatalf("actor A QR challenge was consumed or replaced: %+v", statusA.QRChallenge)
	}
	if statusB.QRChallenge == nil || statusB.QRChallenge.RequestID != qrB.RequestID {
		t.Fatalf("actor B QR challenge was consumed or replaced: %+v", statusB.QRChallenge)
	}
	if len(statusA.Accounts) != len(statusB.Accounts) {
		t.Fatalf("actor account counts differ: A=%d B=%d", len(statusA.Accounts), len(statusB.Accounts))
	}
	for index := range statusA.Accounts {
		if statusA.Accounts[index].ID == statusB.Accounts[index].ID {
			t.Fatalf("actor account %d leaked the same fixture ID: %s", index, statusA.Accounts[index].ID)
		}
	}
}

func assertForeignPhoneHTTP(t *testing.T, handler http.Handler, provider *testActorProvider, status *fixedStatus, actor application.Actor, foreign common.PhoneChallenge) {
	t.Helper()
	provider.actor.Store(actor)
	jar := newCookieJar()
	status.status = common.Status{}
	operatorPage(t, handler, jar)
	foreignCopy := foreign
	status.status = common.Status{PhoneChallenge: &foreignCopy}
	operatorPage(t, handler, jar)
	status.status = common.Status{}
	foreignResponse := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", url.Values{"code": {authmock.MockPhoneCode}})

	randomCopy := foreign
	randomCopy.RequestID = uuid.New()
	status.status = common.Status{PhoneChallenge: &randomCopy}
	operatorPage(t, handler, jar)
	status.status = common.Status{}
	randomResponse := operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", url.Values{"code": {authmock.MockPhoneCode}})
	if foreignResponse.Code != randomResponse.Code || foreignResponse.Header().Get("Location") != randomResponse.Header().Get("Location") {
		t.Fatalf("foreign and random phone responses differ for actor %s: foreign=%d/%q random=%d/%q", actor.OperatorID, foreignResponse.Code, foreignResponse.Header().Get("Location"), randomResponse.Code, randomResponse.Header().Get("Location"))
	}
}

func assertForeignQRHTTP(t *testing.T, handler http.Handler, provider *testActorProvider, status *fixedStatus, actor application.Actor, foreign common.QRChallenge) {
	t.Helper()
	provider.actor.Store(actor)
	jar := newCookieJar()
	status.status = common.Status{}
	operatorPage(t, handler, jar)
	foreignCopy := foreign
	status.status = common.Status{QRChallenge: &foreignCopy}
	operatorPage(t, handler, jar)
	status.status = common.Status{}
	foreignResponse := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{})

	randomCopy := foreign
	randomCopy.RequestID = uuid.New()
	status.status = common.Status{QRChallenge: &randomCopy}
	operatorPage(t, handler, jar)
	status.status = common.Status{}
	randomResponse := operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{})
	if foreignResponse.Code != randomResponse.Code || foreignResponse.Header().Get("Location") != randomResponse.Header().Get("Location") {
		t.Fatalf("foreign and random QR responses differ for actor %s: foreign=%d/%q random=%d/%q", actor.OperatorID, foreignResponse.Code, foreignResponse.Header().Get("Location"), randomResponse.Code, randomResponse.Header().Get("Location"))
	}
}

func TestOperatorAccountHTTPConcurrentRequests(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	handler := newMockOperatorHandler(provider)
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			local := newCookieJar()
			jar.mu.Lock()
			for name, cookie := range jar.cookies {
				local.cookies[name] = cookie
			}
			jar.mu.Unlock()
			if index%2 == 0 {
				operatorPage(t, handler, local)
				return
			}
			operatorPost(t, handler, local, "/operator-accounts/authenticate/phone", url.Values{"phone": {"+1555123" + strings.Repeat("0", index%3+1)}})
		}(index)
	}
	wait.Wait()
}

func TestOperatorAccountHTTPDoesNotSerializeStaleGETAndPhoneVerify(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	store := authmock.NewStore()
	startPhone := authmock.NewStartPhone(store)
	challenge, failure := startPhone.Execute(context.Background(), actor, "+15551230021")
	if failure != nil {
		t.Fatalf("create phone challenge: %v", failure)
	}
	status := &blockingStatus{
		delegate:  authmock.NewStatus(store),
		blockCall: 2,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	verify := &observingVerify{delegate: authmock.NewVerifyPhone(store), entered: make(chan struct{})}
	handler := New(startPhone, verify, authmock.NewStartQR(store), authmock.NewRefreshQR(store), status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	staleGET := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		staleGET <- operatorRequest(t, handler, jar, http.MethodGet, "/operator-accounts/authenticate", nil)
	}()
	waitForSignal(t, status.entered, "stale GET status")

	verifyDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		verifyDone <- operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone/code", url.Values{"code": {authmock.MockPhoneCode}})
	}()
	waitForSignal(t, verify.entered, "phone verification while stale GET is blocked")

	close(status.release)
	if response := <-staleGET; response.Code != http.StatusOK {
		t.Fatalf("stale GET: expected 200, got %d", response.Code)
	}
	if response := <-verifyDone; response.Code != http.StatusSeeOther {
		t.Fatalf("phone verification: expected redirect, got %d", response.Code)
	}

	page := operatorPage(t, handler, jar)
	if phoneInputVisible(page, challenge.Phone) || strings.Contains(page, `name="code"`) {
		t.Fatalf("consumed phone challenge was restored by stale GET: %s", page)
	}
}

func TestOperatorAccountHTTPDoesNotSerializeStaleGETAndPhoneStart(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	store := authmock.NewStore()
	status := &blockingStatus{
		delegate:  authmock.NewStatus(store),
		blockCall: 2,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	start := &observingStartPhone{delegate: authmock.NewStartPhone(store), entered: make(chan struct{})}
	handler := New(start, authmock.NewVerifyPhone(store), authmock.NewStartQR(store), authmock.NewRefreshQR(store), status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	staleGET := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		staleGET <- operatorRequest(t, handler, jar, http.MethodGet, "/operator-accounts/authenticate", nil)
	}()
	waitForSignal(t, status.entered, "stale GET status")

	startDone := make(chan *httptest.ResponseRecorder, 1)
	phone := "+15551230022"
	go func() {
		startDone <- operatorPost(t, handler, jar, "/operator-accounts/authenticate/phone", url.Values{"phone": {phone}})
	}()
	waitForSignal(t, start.entered, "phone start while stale GET is blocked")

	close(status.release)
	if response := <-staleGET; response.Code != http.StatusOK {
		t.Fatalf("stale GET: expected 200, got %d", response.Code)
	}
	if response := <-startDone; response.Code != http.StatusSeeOther {
		t.Fatalf("phone start: expected redirect, got %d", response.Code)
	}
	page := operatorPage(t, handler, jar)
	if !phoneInputVisible(page, phone) {
		t.Fatalf("new phone challenge was cleared by stale GET: %s", page)
	}
}

func TestOperatorAccountHTTPSerializesStaleGETAndQRStart(t *testing.T) {
	t.Skip("QR authentication is intentionally not part of the phone-only flow")
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	store := authmock.NewStore()
	status := &blockingStatus{
		delegate:  authmock.NewStatus(store),
		blockCall: 2,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	handler := New(authmock.NewStartPhone(store), authmock.NewVerifyPhone(store), authmock.NewStartQR(store), authmock.NewRefreshQR(store), status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	staleGET := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		staleGET <- operatorRequest(t, handler, jar, http.MethodGet, "/operator-accounts/authenticate", nil)
	}()
	waitForSignal(t, status.entered, "stale GET status")

	startDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		startDone <- operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr", url.Values{})
	}()
	select {
	case response := <-startDone:
		t.Fatalf("QR start completed while stale GET was blocked: %d", response.Code)
	case <-time.After(50 * time.Millisecond):
	}

	close(status.release)
	if response := <-staleGET; response.Code != http.StatusOK {
		t.Fatalf("stale GET: expected 200, got %d", response.Code)
	}
	if response := <-startDone; response.Code != http.StatusSeeOther {
		t.Fatalf("QR start: expected redirect, got %d", response.Code)
	}
	page := operatorPage(t, handler, jar)
	if !strings.Contains(page, `class="qr"`) {
		t.Fatalf("new QR challenge was cleared by stale GET: %s", page)
	}
}

func TestOperatorAccountHTTPSerializesStaleGETAndQRRefresh(t *testing.T) {
	t.Skip("QR authentication is intentionally not part of the phone-only flow")
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	store := authmock.NewStore()
	startQR := authmock.NewStartQR(store)
	oldChallenge, failure := startQR.Execute(context.Background(), actor)
	if failure != nil {
		t.Fatalf("create QR challenge: %v", failure)
	}
	status := &blockingStatus{
		delegate:  authmock.NewStatus(store),
		blockCall: 2,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	handler := New(authmock.NewStartPhone(store), authmock.NewVerifyPhone(store), startQR, authmock.NewRefreshQR(store), status, provider, "http://example.test")
	jar := newCookieJar()
	operatorPage(t, handler, jar)

	staleGET := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		staleGET <- operatorRequest(t, handler, jar, http.MethodGet, "/operator-accounts/authenticate", nil)
	}()
	waitForSignal(t, status.entered, "stale GET status")

	refreshDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		refreshDone <- operatorPost(t, handler, jar, "/operator-accounts/authenticate/qr/refresh", url.Values{})
	}()
	select {
	case response := <-refreshDone:
		t.Fatalf("QR refresh completed while stale GET was blocked: %d", response.Code)
	case <-time.After(50 * time.Millisecond):
	}

	close(status.release)
	if response := <-staleGET; response.Code != http.StatusOK {
		t.Fatalf("stale GET: expected 200, got %d", response.Code)
	}
	if response := <-refreshDone; response.Code != http.StatusSeeOther {
		t.Fatalf("QR refresh: expected redirect, got %d", response.Code)
	}

	current, failure := authmock.NewStatus(store).Execute(context.Background(), actor)
	if failure != nil {
		t.Fatalf("load refreshed QR status: %v", failure)
	}
	if current.QRChallenge == nil || current.QRChallenge.RequestID != oldChallenge.RequestID || current.QRChallenge.URL == oldChallenge.URL {
		t.Fatalf("QR refresh state was lost or stale GET won: old=%+v current=%+v", oldChallenge, current.QRChallenge)
	}
	if page := operatorPage(t, handler, jar); !strings.Contains(page, `class="qr"`) {
		t.Fatal("refreshed QR challenge was not rendered")
	}
}

func TestOperatorAccountHTTPCreatesOneFlowCookiePerRequest(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()
	handler := New(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, "http://example.test")
	response := operatorRequest(t, handler, newCookieJar(), http.MethodGet, "/operator-accounts/authenticate", nil)

	count := 0
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == sessionCookie {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one session flow cookie, got %d cookies: %v", count, response.Result().Cookies())
	}
}

func TestOperatorAccountRouteIsRegisteredBehindPrincipalAndDisabledWithoutLiveAuth(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	authenticated := newTestActorProvider(actor)
	handler := NewWithOptions(
		nil,
		nil,
		nil,
		nil,
		nil,
		authenticated,
		"https://example.test",
		RouteOptions{Mode: RouteDisabled, Cookie: SecureCookieConfig()},
	)

	// A registered authenticated route must not fall through to 404, and the
	// disabled composition must not call a nil command port or panic.
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "https://example.test/operator-accounts/authenticate", nil))
	if get.Code != http.StatusServiceUnavailable || !strings.Contains(get.Body.String(), "СЕРВИС ВРЕМЕННО НЕДОСТУПЕН") || !strings.Contains(get.Body.String(), `href="/"`) {
		t.Fatalf("disabled authenticated GET: status=%d body=%q", get.Code, get.Body.String())
	}
	post := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "https://example.test/operator-accounts/authenticate/phone", strings.NewReader("phone=%2B15551230001"))
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.ServeHTTP(post, request)
	if post.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled authenticated POST: status=%d body=%q", post.Code, post.Body.String())
	}

	unauthenticated := principal.ProviderFunc(func(*http.Request) (application.Actor, error) {
		return application.Actor{}, principal.ErrUnavailable
	})
	unauthedHandler := NewWithOptions(nil, nil, nil, nil, nil, unauthenticated, "https://example.test", RouteOptions{Mode: RouteDisabled, Cookie: SecureCookieConfig()})
	unauthenticatedResponse := httptest.NewRecorder()
	unauthedHandler.ServeHTTP(unauthenticatedResponse, httptest.NewRequest(http.MethodGet, "https://example.test/operator-accounts/authenticate", nil))
	if unauthenticatedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated route: status=%d body=%q", unauthenticatedResponse.Code, unauthenticatedResponse.Body.String())
	}
}

func TestDisabledOperatorAccountRendersUnavailableTemplateContract(t *testing.T) {
	handler := &handler{
		disabled: true,
		tmpl:     template.Must(template.New("authenticate.html").Parse("{{.Unavailable}}|{{.UnavailableMessage}}|{{.ReturnToConsole}}|{{.ErrorFocus}}")),
	}
	response := httptest.NewRecorder()
	handler.unavailable(response)

	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "true|"+unavailableMessage+"|/|true" {
		t.Fatalf("unavailable template contract: status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestOperatorAccountProductionAndStagingCompositionsCannotRunMockFixedCode(t *testing.T) {
	for _, test := range []struct {
		name string
		env  DeploymentEnvironment
	}{
		{name: "production", env: EnvironmentProduction},
		{name: "staging", env: EnvironmentStaging},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := application.Actor{OperatorID: uuid.New()}
			provider := newTestActorProvider(actor)
			mock := authmock.New()
			handler := NewWithOptions(
				mock.StartPhone,
				mock.VerifyPhone,
				mock.StartQR,
				mock.RefreshQR,
				mock.Status,
				provider,
				"https://example.test",
				RouteOptions{Mode: RouteDisabled, Environment: test.env, Cookie: SecureCookieConfig()},
			)
			request := httptest.NewRequest(http.MethodPost, "https://example.test/operator-accounts/authenticate/phone/code", strings.NewReader("code="+authmock.MockPhoneCode))
			request.Header.Set("Origin", "https://example.test")
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), authmock.MockPhoneCode) {
				t.Fatalf("%s mock fixed-code flow executed or leaked: status=%d body=%q", test.name, response.Code, response.Body.String())
			}
		})
	}
}

func TestOperatorAccountDevelopmentTestMockModeIsExplicit(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()
	handler := NewWithOptions(
		mock.StartPhone,
		mock.VerifyPhone,
		mock.StartQR,
		mock.RefreshQR,
		mock.Status,
		provider,
		"https://example.test",
		RouteOptions{Mode: RouteDevelopmentTestMock, Environment: EnvironmentDevelopment, Cookie: SecureCookieConfig(), AllowDevelopmentTestMock: true},
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "https://example.test/operator-accounts/authenticate", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("explicit development/test mock route: status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestOperatorAccountDevelopmentTestMockRequiresExplicitOptIn(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()
	defer func() {
		if recover() == nil {
			t.Fatal("development/test mock route registered without explicit opt-in")
		}
	}()
	NewWithOptions(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, "https://example.test", RouteOptions{Mode: RouteDevelopmentTestMock, Cookie: SecureCookieConfig()})
}

func TestOperatorAccountCookiesFollowDeploymentPolicy(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()

	tests := []struct {
		name        string
		origin      string
		cookie      CookieConfig
		secure      bool
		csrfName    string
		sessionName string
	}{
		{name: "secure HTTPS", origin: "https://example.test", cookie: SecureCookieConfig(), secure: true, csrfName: productionCSRFCookie, sessionName: productionSessionCookie},
		{name: "explicit local HTTP exception", origin: "http://localhost:8080", cookie: LocalInsecureCookieConfig(), secure: false, csrfName: localCSRFCookie, sessionName: localSessionCookie},
		{name: "nonlocal HTTP remains secure", origin: "http://dev.example.test", cookie: SecureCookieConfig(), secure: true, csrfName: productionCSRFCookie, sessionName: productionSessionCookie},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewWithOptions(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, test.origin, RouteOptions{Mode: RouteDevelopmentTestMock, Environment: EnvironmentTesting, Cookie: test.cookie, AllowDevelopmentTestMock: true})
			request := httptest.NewRequest(http.MethodGet, test.origin+"/operator-accounts/authenticate", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("authenticate GET: status=%d body=%q", response.Code, response.Body.String())
			}
			cookies := response.Result().Cookies()
			assertOperatorCookie(t, cookies, test.csrfName, test.secure)
			assertOperatorCookie(t, cookies, test.sessionName, test.secure)
		})
	}
}

func TestOperatorAccountCookiePolicyRejectsImplicitInsecureAndWrongHostSettings(t *testing.T) {
	for _, cookie := range []CookieConfig{
		{CSRFCookieName: localCSRFCookie, SessionCookieName: localSessionCookie},
		{CSRFCookieName: localCSRFCookie, SessionCookieName: localSessionCookie, AllowInsecureLocal: false},
		{CSRFCookieName: productionCSRFCookie, SessionCookieName: productionSessionCookie, Secure: true, AllowInsecureLocal: true},
		{CSRFCookieName: "custom_csrf", SessionCookieName: "custom_session", Secure: true},
	} {
		if err := ValidateCookieConfig(cookie); err == nil {
			t.Fatalf("accepted invalid operator cookie policy: %#v", cookie)
		}
	}
	if err := ValidateCookieConfig(LocalInsecureCookieConfig()); err != nil {
		t.Fatalf("rejected explicit local operator cookie policy: %v", err)
	}
	actor := application.Actor{OperatorID: uuid.New()}
	provider := newTestActorProvider(actor)
	mock := authmock.New()
	defer func() {
		if recover() == nil {
			t.Fatal("registered insecure operator cookies for a nonlocal HTTP origin")
		}
	}()
	NewWithOptions(mock.StartPhone, mock.VerifyPhone, mock.StartQR, mock.RefreshQR, mock.Status, provider, "http://dev.example.test", RouteOptions{Mode: RouteDevelopmentTestMock, Environment: EnvironmentDevelopment, Cookie: LocalInsecureCookieConfig(), AllowDevelopmentTestMock: true})
}

func assertOperatorCookie(t *testing.T, cookies []*http.Cookie, name string, secure bool) {
	t.Helper()
	for _, cookie := range cookies {
		if cookie.Name != name || cookie.MaxAge <= 0 {
			continue
		}
		if cookie.Secure != secure || !cookie.HttpOnly || cookie.Path != "/" || cookie.Domain != "" || cookie.SameSite != http.SameSiteLaxMode {
			t.Fatalf("unexpected %s cookie: %#v", name, cookie)
		}
		return
	}
	t.Fatalf("cookie %q was not set: %v", name, cookies)
}

type blockingStatus struct {
	delegate  requests.Status
	blockCall int
	entered   chan struct{}
	release   chan struct{}

	mu    sync.Mutex
	calls int
}

func (status *blockingStatus) Execute(ctx context.Context, actor application.Actor) (common.Status, error) {
	current, failure := status.delegate.Execute(ctx, actor)
	status.mu.Lock()
	status.calls++
	call := status.calls
	status.mu.Unlock()
	if call == status.blockCall {
		close(status.entered)
		select {
		case <-status.release:
		case <-ctx.Done():
			return common.Status{}, ctx.Err()
		}
	}
	return current, failure
}

type observingVerify struct {
	delegate commands.VerifyPhone
	entered  chan struct{}
	once     sync.Once
}

func (verify *observingVerify) Execute(ctx context.Context, actor application.Actor, requestID uuid.UUID, code string) (common.Account, error) {
	verify.once.Do(func() { close(verify.entered) })
	return verify.delegate.Execute(ctx, actor, requestID, code)
}

type observingStartPhone struct {
	delegate commands.StartPhone
	entered  chan struct{}
	once     sync.Once
}

func (start *observingStartPhone) Execute(ctx context.Context, actor application.Actor, phone string) (common.PhoneChallenge, error) {
	start.once.Do(func() { close(start.entered) })
	return start.delegate.Execute(ctx, actor, phone)
}

func waitForSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type canonicalStartProbe struct{}

func (canonicalStartProbe) Execute(context.Context, application.Actor, string) (common.Result, error) {
	return common.Result{Account: &common.Account{Status: "active"}}, nil
}

type canonicalChallengeStartProbe struct{}

func (canonicalChallengeStartProbe) Execute(context.Context, application.Actor, string) (common.Result, error) {
	return common.Result{Challenge: &common.Challenge{RequestID: uuid.New(), Phone: "+15551234567", Stage: common.StageCode, ExpiresAt: time.Now().Add(time.Minute)}}, nil
}

type canonicalCodeProbe struct{}

func (canonicalCodeProbe) Execute(context.Context, application.Actor, uuid.UUID, string) (common.Result, error) {
	return common.Result{Account: &common.Account{Status: "active"}}, nil
}

type canonicalPasswordProbe struct{}

func (canonicalPasswordProbe) Execute(context.Context, application.Actor, uuid.UUID, string) (common.Result, error) {
	return common.Result{Account: &common.Account{Status: "active"}}, nil
}

type canonicalCancelProbe struct{}

func (canonicalCancelProbe) Execute(context.Context, application.Actor, uuid.UUID) error { return nil }

type canonicalStatusProbe struct{}

func (canonicalStatusProbe) Execute(context.Context, application.Actor) (common.Status, error) {
	return common.Status{}, nil
}

type canonicalPasswordStatusProbe struct{}

func (canonicalPasswordStatusProbe) Execute(context.Context, application.Actor) (common.Status, error) {
	return common.Status{Challenge: &common.Challenge{RequestID: uuid.New(), Phone: "+15551234567", Stage: common.StagePassword, ExpiresAt: time.Now().Add(time.Minute)}}, nil
}

func TestCanonicalPhoneStartRedirectsAlreadyActiveWithoutCodeStage(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	router := NewWithPhoneAuth(canonicalStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalStatusProbe{}, newTestActorProvider(actor), localOperatorOrigin, RouteOptions{
		Mode:                     RouteDevelopmentTestMock,
		Environment:              EnvironmentTesting,
		Cookie:                   LocalInsecureCookieConfig(),
		AllowDevelopmentTestMock: true,
	})
	jar := newCookieJar()
	page := operatorRequestAtOrigin(t, router, jar, http.MethodGet, "/operator-accounts/authenticate", nil, localOperatorOrigin)
	if page.Code != http.StatusOK {
		t.Fatalf("local HTTP canonical route: expected 200, got %d", page.Code)
	}
	assertOperatorCookie(t, page.Result().Cookies(), localCSRFCookie, false)
	assertOperatorCookie(t, page.Result().Cookies(), localSessionCookie, false)
	response := operatorPostAtOrigin(t, router, jar, "/operator-accounts/authenticate/phone", url.Values{"phone": {"+15551234567"}}, localOperatorOrigin, localCSRFCookie)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("already-active phone start: expected redirect, got %d", response.Code)
	}
	if location := response.Header().Get("Location"); location != "/operator-accounts/authenticate?notice=account-added" {
		t.Fatalf("already-active phone start redirected to %q, want account-added", location)
	}
}

func TestCanonicalPhoneStartRedirectsChallengeAsCodeSent(t *testing.T) {
	actor := application.Actor{OperatorID: uuid.New()}
	router := NewWithPhoneAuth(canonicalChallengeStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalStatusProbe{}, newTestActorProvider(actor), localOperatorOrigin, RouteOptions{
		Mode:                     RouteDevelopmentTestMock,
		Environment:              EnvironmentTesting,
		Cookie:                   LocalInsecureCookieConfig(),
		AllowDevelopmentTestMock: true,
	})
	jar := newCookieJar()
	operatorPageAtOrigin(t, router, jar, localOperatorOrigin)
	response := operatorPostAtOrigin(t, router, jar, "/operator-accounts/authenticate/phone", url.Values{"phone": {"+15551234567"}}, localOperatorOrigin, localCSRFCookie)
	if location := response.Header().Get("Location"); location != "/operator-accounts/authenticate?notice=code-sent" {
		t.Fatalf("challenge phone start redirected to %q, want code-sent", location)
	}
}

func TestCanonicalPasswordInputDisablesAutocomplete(t *testing.T) {
	router := NewWithPhoneAuth(canonicalStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalPasswordStatusProbe{}, newTestActorProvider(application.Actor{OperatorID: uuid.New()}), "https://example.test", RouteOptions{
		Mode:   RouteLive,
		Cookie: SecureCookieConfig(),
	})
	page := operatorPage(t, router, newCookieJar())
	if !strings.Contains(page, `name="password"`) || !strings.Contains(page, `autocomplete="off"`) {
		t.Fatalf("password input does not disable autocomplete: %s", page)
	}
	if strings.Contains(page, `autocomplete="current-password"`) {
		t.Fatal("password input uses a reusable credential autocomplete token")
	}
}

func TestCanonicalPhoneRouteAllowsExplicitLiveStagingWithSecureCookies(t *testing.T) {
	router := NewWithPhoneAuth(canonicalStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalStatusProbe{}, newTestActorProvider(application.Actor{OperatorID: uuid.New()}), "https://example.test", RouteOptions{
		Mode:        RouteLive,
		Environment: EnvironmentStaging,
		Cookie:      SecureCookieConfig(),
	})
	request := httptest.NewRequest(http.MethodGet, "/operator-accounts/authenticate", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code == http.StatusServiceUnavailable {
		t.Fatalf("explicit live staging route was downgraded to disabled: status=%d", response.Code)
	}
}

func TestCanonicalPhoneRouteRejectsUnknownMode(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("unknown route mode did not panic")
		}
	}()
	NewWithPhoneAuth(canonicalStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalStatusProbe{}, newTestActorProvider(application.Actor{OperatorID: uuid.New()}), "https://example.test", RouteOptions{
		Mode:   RouteMode("unexpected"),
		Cookie: SecureCookieConfig(),
	})
}

func TestCanonicalPhoneRouteDoesNotExposeQR(t *testing.T) {
	router := NewWithPhoneAuth(canonicalStartProbe{}, canonicalCodeProbe{}, canonicalPasswordProbe{}, canonicalCancelProbe{}, canonicalStatusProbe{}, newTestActorProvider(application.Actor{OperatorID: uuid.New()}), "https://example.test", RouteOptions{
		Mode:   RouteLive,
		Cookie: SecureCookieConfig(),
	})
	request := httptest.NewRequest(http.MethodPost, "/operator-accounts/authenticate/qr", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("QR endpoint is exposed: status=%d", response.Code)
	}
}
