package console

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	commands "github.com/notrodans/nebula-go/internal/application/commands/mailing-console"
	requests "github.com/notrodans/nebula-go/internal/application/requests/mailing-console"
	"github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
	"github.com/notrodans/nebula-go/internal/entrypoint/http/principal"
)

type testDependencies struct {
	dashboard           mailingconsole.Dashboard
	createErr, queueErr error
	operatorExists      bool
	created             mailingconsole.CreateDraftInput
}

type fakeConsole struct {
	dashboard      mailingconsole.Dashboard
	operatorExists bool
}

func (f *fakeConsole) OperatorExists(context.Context, uuid.UUID) (bool, error) {
	return f.operatorExists, nil
}

func (f *fakeConsole) Dashboard(_ context.Context, _ uuid.UUID) ([]mailingconsole.Account, []mailingconsole.SharedDialog, []mailingconsole.PrivateDialog, []mailingconsole.MailingSummary, error) {
	return f.dashboard.Accounts, f.dashboard.SharedDialogs, f.dashboard.PrivateDialogs, f.dashboard.Mailings, nil
}

type fakeMailings struct {
	scoped *fakeOperatorMailings
}

func (f *fakeMailings) OwnedBy(uuid.UUID) mailingconsole.OperatorMailings {
	return f.scoped
}

type fakeOperatorMailings struct {
	created   *mailingconsole.CreateDraftInput
	createErr error
	row       *fakeMailing
}

func (f *fakeOperatorMailings) CreateDraft(_ context.Context, input mailingconsole.CreateDraftInput) (mailing.ID, error) {
	if f.created != nil {
		*f.created = input
	}
	if f.createErr != nil {
		return mailing.ID{}, f.createErr
	}
	return mailing.Identity(uuid.New()), nil
}

func (f *fakeOperatorMailings) Mailing(mailing.ID) mailing.Mailing { return f.row }

type fakeMailing struct {
	mailingID uuid.UUID
	queueErr  error
}

func (f *fakeMailing) Queue(context.Context) error { return f.queueErr }

func (f *fakeMailing) Stop(context.Context) error { return nil }

type scopedHTTPConsole struct {
	dashboards map[uuid.UUID]mailingconsole.Dashboard
}

func (projection *scopedHTTPConsole) OperatorExists(_ context.Context, operatorID uuid.UUID) (bool, error) {
	_, exists := projection.dashboards[operatorID]
	return exists, nil
}

func (projection *scopedHTTPConsole) Dashboard(_ context.Context, operatorID uuid.UUID) ([]mailingconsole.Account, []mailingconsole.SharedDialog, []mailingconsole.PrivateDialog, []mailingconsole.MailingSummary, error) {
	dashboard, exists := projection.dashboards[operatorID]
	if !exists {
		return nil, nil, nil, nil, mailingconsole.ErrNotFound
	}
	return dashboard.Accounts, dashboard.SharedDialogs, dashboard.PrivateDialogs, dashboard.Mailings, nil
}

type scopedHTTPMailings struct {
	operators map[uuid.UUID]*scopedHTTPOperatorMailings
}

type scopedHTTPOperatorMailings struct {
	accountID uuid.UUID
	mailingID uuid.UUID
	row       *fakeMailing
}

func (table *scopedHTTPMailings) OwnedBy(operatorID uuid.UUID) mailingconsole.OperatorMailings {
	operator := table.operators[operatorID]
	if operator == nil {
		return nil
	}
	return operator
}

func (operator *scopedHTTPOperatorMailings) CreateDraft(_ context.Context, input mailingconsole.CreateDraftInput) (mailing.ID, error) {
	if input.AccountID != operator.accountID {
		return mailing.ID{}, mailingconsole.ErrNotFound
	}
	return mailing.Identity(operator.mailingID), nil
}

func (operator *scopedHTTPOperatorMailings) Mailing(identity mailing.ID) mailing.Mailing {
	if identity.UUID() != operator.mailingID {
		return &fakeMailing{mailingID: identity.UUID(), queueErr: mailing.ErrNotFound}
	}
	return operator.row
}

func newHandler(dependencies *testDependencies) http.Handler {
	service, operatorID := newService(dependencies)
	return New(&service, principal.Static(operatorID), "http://example.test")
}

func newService(dependencies *testDependencies) (mailingconsole.Service, uuid.UUID) {
	operatorID := uuid.New()
	projection := &fakeConsole{
		dashboard:      dependencies.dashboard,
		operatorExists: dependencies.operatorExists,
	}
	scoped := &fakeOperatorMailings{
		created:   &dependencies.created,
		createErr: dependencies.createErr,
		row:       &fakeMailing{queueErr: dependencies.queueErr},
	}
	return mailingconsole.NewService(projection, &fakeMailings{scoped: scoped}), operatorID
}

func dashboardResponse(t *testing.T, handler http.Handler) (*http.Cookie, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /: got %d", w.Code)
	}
	cookie := w.Result().Cookies()[0]
	body, _ := io.ReadAll(w.Result().Body)
	text := string(body)
	start := strings.Index(text, `name="csrf_token" value="`) + len(`name="csrf_token" value="`)
	end := strings.Index(text[start:], `"`)
	return cookie, text[start : start+end]
}

func postForm(t *testing.T, handler http.Handler, path string, values url.Values, cookie *http.Cookie, token string) *httptest.ResponseRecorder {
	t.Helper()
	if values == nil {
		values = url.Values{}
	}
	values.Set("csrf_token", token)
	r := httptest.NewRequest(http.MethodPost, "http://example.test"+path, strings.NewReader(values.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "http://example.test")
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func testDashboard() mailingconsole.Dashboard {
	a := uuid.New()
	d := uuid.New()
	return mailingconsole.Dashboard{Accounts: []mailingconsole.Account{{ID: a, TelegramUsername: "safe"}}, SharedDialogs: []mailingconsole.SharedDialog{{ID: d, AccountID: a, Title: "Team"}}, Mailings: []mailingconsole.MailingSummary{{ID: uuid.New(), Name: "Черновик", Status: "draft", RecipientCount: 1, CreatedAt: time.Now()}}}
}

func TestDashboardEscapesTemplateValues(t *testing.T) {
	repo := &testDependencies{dashboard: testDashboard(), operatorExists: true}
	repo.dashboard.Accounts[0].TelegramUsername = `<script>alert(1)</script>`
	response := httptest.NewRecorder()
	newHandler(repo).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/", nil))
	if strings.Contains(response.Body.String(), "<script>alert") || !strings.Contains(response.Body.String(), "&lt;script&gt;") {
		t.Fatal("template value was not escaped")
	}
}

func TestTwoOperatorHTTPConsoleDoesNotCrossScope(t *testing.T) {
	operatorA, operatorB := uuid.New(), uuid.New()
	accountA, accountB := uuid.New(), uuid.New()
	dialogA, dialogB := uuid.New(), uuid.New()
	draftA, draftB := uuid.New(), uuid.New()
	projection := &scopedHTTPConsole{dashboards: map[uuid.UUID]mailingconsole.Dashboard{
		operatorA: {Accounts: []mailingconsole.Account{{ID: accountA}}, SharedDialogs: []mailingconsole.SharedDialog{{ID: dialogA, AccountID: accountA}}, Mailings: []mailingconsole.MailingSummary{{ID: draftA, Status: "draft"}}},
		operatorB: {Accounts: []mailingconsole.Account{{ID: accountB}}, SharedDialogs: []mailingconsole.SharedDialog{{ID: dialogB, AccountID: accountB}}, Mailings: []mailingconsole.MailingSummary{{ID: draftB, Status: "draft"}}},
	}}
	mailingTable := &scopedHTTPMailings{operators: map[uuid.UUID]*scopedHTTPOperatorMailings{
		operatorA: {accountID: accountA, mailingID: draftA, row: &fakeMailing{mailingID: draftA}},
		operatorB: {accountID: accountB, mailingID: draftB, row: &fakeMailing{mailingID: draftB}},
	}}
	service := mailingconsole.NewService(projection, mailingTable)
	handlerA := New(
		commands.NewCreateDraft(&service),
		commands.NewQueue(&service),
		requests.NewDashboard(&service),
		principal.Static(operatorA),
		"http://example.test",
	)
	handlerB := New(
		commands.NewCreateDraft(&service),
		commands.NewQueue(&service),
		requests.NewDashboard(&service),
		principal.Static(operatorB),
		"http://example.test",
	)

	cookieA, tokenA := dashboardResponse(t, handlerA)
	cookieB, tokenB := dashboardResponse(t, handlerB)
	for _, test := range []struct {
		name       string
		handler    http.Handler
		own, other uuid.UUID
	}{
		{name: "operator A", handler: handlerA, own: accountA, other: accountB},
		{name: "operator B", handler: handlerB, own: accountB, other: accountA},
	} {
		t.Run(test.name+" dashboard", func(t *testing.T) {
			response := httptest.NewRecorder()
			test.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/", nil))
			body := response.Body.String()
			if !strings.Contains(body, test.own.String()) || strings.Contains(body, test.other.String()) {
				t.Fatalf("dashboard leaked account: %s", body)
			}
		})
	}
	if response := postForm(t, handlerA, "/mailings", url.Values{
		"name": {"foreign"}, "message": {"message"}, "account_id": {accountB.String()}, "shared_dialog_id": {dialogB.String()},
	}, cookieA, tokenA); response.Code != http.StatusNotFound {
		t.Fatalf("foreign account create: expected 404, got %d", response.Code)
	}
	foreignQueue := postForm(t, handlerA, "/mailings/"+draftB.String()+"/queue", nil, cookieA, tokenA)
	if foreignQueue.Code != http.StatusNotFound {
		t.Fatalf("foreign queue: expected 404, got %d", foreignQueue.Code)
	}
	randomQueue := postForm(t, handlerA, "/mailings/"+uuid.NewString()+"/queue", nil, cookieA, tokenA)
	if randomQueue.Code != foreignQueue.Code {
		t.Fatalf("foreign and random queue responses differ: foreign=%d random=%d", foreignQueue.Code, randomQueue.Code)
	}
	if response := postForm(t, handlerB, "/mailings", url.Values{
		"name": {"owned"}, "message": {"message"}, "account_id": {accountB.String()}, "shared_dialog_id": {dialogB.String()},
	}, cookieB, tokenB); response.Code != http.StatusSeeOther {
		t.Fatalf("owned create: expected redirect, got %d", response.Code)
	}
	if response := postForm(t, handlerB, "/mailings/"+draftB.String()+"/queue", nil, cookieB, tokenB); response.Code != http.StatusSeeOther {
		t.Fatalf("owned queue: expected redirect, got %d", response.Code)
	}
}

func TestCreateValidationPreservesForm(t *testing.T) {
	repo := &testDependencies{dashboard: testDashboard()}
	h := newHandler(repo)
	cookie, token := dashboardResponse(t, h)
	w := postForm(t, h, "/mailings", url.Values{"name": {"keep <name>"}, "message": {"keep message"}}, cookie, token)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "keep &lt;name&gt;") || !strings.Contains(w.Body.String(), "Выберите аккаунт") {
		t.Fatalf("validation render: %d %s", w.Code, w.Body.String())
	}
}

func TestCSRFRejectedAndAcceptedWithPRG(t *testing.T) {
	repo := &testDependencies{dashboard: testDashboard()}
	h := newHandler(repo)
	cookie, token := dashboardResponse(t, h)
	values := url.Values{"name": {"Name"}, "message": {"Message"}, "account_id": {repo.dashboard.Accounts[0].ID.String()}, "shared_dialog_id": {repo.dashboard.SharedDialogs[0].ID.String()}}
	if got := postForm(t, h, "/mailings", values, cookie, "wrong").Code; got != http.StatusForbidden {
		t.Fatalf("bad csrf: %d", got)
	}
	if got := postForm(t, h, "/mailings", values, cookie, token).Code; got != http.StatusSeeOther {
		t.Fatalf("valid csrf: %d", got)
	}
}

func TestOversizedBody(t *testing.T) {
	h := newHandler(&testDependencies{dashboard: testDashboard()})
	cookie, token := dashboardResponse(t, h)
	w := postForm(t, h, "/mailings", url.Values{"name": {strings.Repeat("x", maxBody)}}, cookie, token)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %d", w.Code)
	}
}

func TestUnknownLengthOversizedBody(t *testing.T) {
	for _, path := range []string{"/mailings", "/mailings/" + uuid.NewString() + "/queue"} {
		t.Run(path, func(t *testing.T) {
			h := newHandler(&testDependencies{dashboard: testDashboard()})
			cookie, token := dashboardResponse(t, h)
			r := httptest.NewRequest(http.MethodPost, "http://example.test"+path, strings.NewReader(strings.Repeat("x", maxBody+1)))
			r.ContentLength = -1
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.Header.Set("Origin", "http://example.test")
			r.AddCookie(cookie)
			// The body is deliberately oversized; the token only proves that
			// authentication is reached before body parsing.
			r.Header.Set("X-Test-CSRF", token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("unknown-length oversized body: %d", w.Code)
			}
		})
	}
}

func TestQueueErrorsAndPRG(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"not found", mailingconsole.ErrNotFound, http.StatusNotFound},
		{"conflict", mailingconsole.ErrInvalidState, http.StatusConflict},
		{"bad input", mailingconsole.ErrInvalidInput, http.StatusBadRequest},
		{"no eligible recipients", mailingconsole.ErrNoEligibleRecipients, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &testDependencies{dashboard: testDashboard(), queueErr: tc.err}
			h := newHandler(repo)
			cookie, token := dashboardResponse(t, h)
			w := postForm(t, h, "/mailings/"+uuid.NewString()+"/queue", nil, cookie, token)
			if w.Code != tc.want {
				t.Fatalf("got %d", w.Code)
			}
		})
	}
	repo := &testDependencies{dashboard: testDashboard()}
	h := newHandler(repo)
	cookie, token := dashboardResponse(t, h)
	w := postForm(t, h, "/mailings/not-a-uuid/queue", nil, cookie, token)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed id: %d", w.Code)
	}
	w = postForm(t, h, "/mailings/"+uuid.NewString()+"/queue", nil, cookie, token)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/?notice=queued" {
		t.Fatalf("queue redirect: %d %s", w.Code, w.Header().Get("Location"))
	}
}

func TestConfiguredOriginValidation(t *testing.T) {
	repo := &testDependencies{dashboard: testDashboard()}
	h := newHandler(repo)
	cookie, token := dashboardResponse(t, h)
	values := url.Values{"name": {"Name"}, "message": {"Message"}, "account_id": {repo.dashboard.Accounts[0].ID.String()}, "shared_dialog_id": {repo.dashboard.SharedDialogs[0].ID.String()}}
	tests := []struct {
		name    string
		origin  string
		referer string
		want    int
	}{
		{name: "missing", want: http.StatusForbidden},
		{name: "foreign origin", origin: "https://foreign.example", want: http.StatusForbidden},
		{name: "matching origin", origin: "http://example.test", want: http.StatusSeeOther},
		{name: "matching referer", referer: "http://example.test/form", want: http.StatusSeeOther},
		{name: "conflicting headers", origin: "http://example.test", referer: "http://foreign.example/form", want: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			form := url.Values{}
			for key, value := range values {
				form[key] = append([]string(nil), value...)
			}
			form.Set("csrf_token", token)
			r := httptest.NewRequest(http.MethodPost, "http://untrusted-host.test/mailings", strings.NewReader(form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if test.origin != "" {
				r.Header.Set("Origin", test.origin)
			}
			if test.referer != "" {
				r.Header.Set("Referer", test.referer)
			}
			r.AddCookie(cookie)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.want {
				t.Fatalf("expected %d, got %d", test.want, w.Code)
			}
		})
	}
}

func TestCSRFCookieUsesConfiguredOrigin(t *testing.T) {
	for _, test := range []struct {
		name   string
		origin string
		secure bool
	}{
		{name: "http", origin: "http://example.test", secure: false},
		{name: "https", origin: "https://example.test", secure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, operatorID := newService(&testDependencies{dashboard: testDashboard()})
			h := New(&service, principal.Static(operatorID), test.origin)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://untrusted-host.test/", nil))
			cookie := w.Result().Cookies()[0]
			if cookie.Secure != test.secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode {
				t.Fatalf("unexpected CSRF cookie: %#v", cookie)
			}
		})
	}
}

func TestGenericServiceErrorsAreNotRendered(t *testing.T) {
	secret := errors.New("database password should not be rendered")
	for _, test := range []struct {
		name string
		err  error
		path string
	}{
		{name: "create", err: secret, path: "/mailings"},
		{name: "queue", err: secret, path: "/mailings/" + uuid.NewString() + "/queue"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &testDependencies{dashboard: testDashboard(), createErr: test.err, queueErr: test.err}
			h := newHandler(repo)
			cookie, token := dashboardResponse(t, h)
			values := url.Values{"name": {"Name"}, "message": {"Message"}, "account_id": {repo.dashboard.Accounts[0].ID.String()}, "shared_dialog_id": {repo.dashboard.SharedDialogs[0].ID.String()}}
			w := postForm(t, h, test.path, values, cookie, token)
			if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), secret.Error()) {
				t.Fatalf("generic error response: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestStatusLabels(t *testing.T) {
	repo := &testDependencies{dashboard: testDashboard()}
	for _, status := range []string{"draft", "queued", "running", "paused", "stopped", "completed", "failed"} {
		repo.dashboard.Mailings = append(repo.dashboard.Mailings, mailingconsole.MailingSummary{ID: uuid.New(), Name: status, Status: status})
	}
	response := httptest.NewRecorder()
	newHandler(repo).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://example.test/", nil))
	body := response.Body.String()
	for _, test := range []struct {
		status string
		label  string
	}{
		{"draft", "Черновик"},
		{"queued", "В очереди"},
		{"running", "Отправляется"},
		{"paused", "Приостановлена"},
		{"stopped", "Остановлена"},
		{"completed", "Завершена"},
		{"failed", "Есть ошибки"},
	} {
		if got := statusLabel(test.status); got != test.label {
			t.Fatalf("status %q: expected %q, got %q", test.status, test.label, got)
		}
		if !strings.Contains(body, "status-"+test.status) || !strings.Contains(body, test.label) {
			t.Fatalf("status %q was not rendered with raw class and label", test.status)
		}
	}
}
