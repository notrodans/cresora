// Package operatoraccounts provides the operator Telegram account sign-in page.
package operatoraccounts

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	application "github.com/notrodans/nebula-go/internal/application"
	commands "github.com/notrodans/nebula-go/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
	requests "github.com/notrodans/nebula-go/internal/application/requests/operator-account-auth"
	"github.com/notrodans/nebula-go/internal/entrypoint/http/principal"
	"rsc.io/qr"
)

const (
	csrfCookie    = "nebula_operator_csrf"
	sessionCookie = "nebula_operator_session"
	stateLifetime = 10 * time.Minute
	maxStateCount = 4096
)

//go:embed templates/authenticate.html style.css authenticate.js
var assets embed.FS

type handler struct {
	startPhone  commands.StartPhone
	verifyPhone commands.VerifyPhone
	startQR     commands.StartQR
	refreshQR   commands.RefreshQR
	status      requests.Status
	tmpl        *template.Template
	mu          sync.Mutex
	states      map[browserStateKey]browserState
	actorMu     sync.Mutex
	actorLocks  map[uuid.UUID]*sync.Mutex
}

// browserStateKey keeps a browser flow separate for every trusted actor. A
// session cookie is only a flow identifier; it is never an identity source.
type browserStateKey struct {
	actorID uuid.UUID
	flowID  string
}

type browserState struct {
	Phone       string
	PhoneID     uuid.UUID
	PhoneExpiry time.Time
	QR          common.QRChallenge
	QRExpiry    time.Time
	HasQR       bool
	Touched     time.Time
}

type page struct {
	Accounts []accountRow
	Phone    string
	CodeSent bool
	QR       *qrView
	CSRF     string
	Notice   string
	Error    string
}

type browserStateSnapshot struct {
	flowID string
	state  browserState
}

// requestScope is resolved once at the HTTP boundary. The actor comes from
// the trusted principal middleware and the flow ID comes from the browser
// cookie (or is created once for this response). Keeping both values together
// prevents one request from accidentally operating on multiple browser flows.
type requestScope struct {
	actor  application.Actor
	flowID string
}

type accountRow struct {
	Name  string
	Phone string
	State string
}

type qrView struct {
	Image   string
	Expires int64
}

// New constructs the chi router for operator account authentication.
func New(startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status, provider principal.Provider) chi.Router {
	r := chi.NewRouter()
	Register(r, startPhone, verifyPhone, startQR, refreshQR, status, provider)
	return r
}

// Register adds the account authentication routes to an existing chi router.
func Register(router chi.Router, startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status, provider principal.Provider) {
	if provider == nil {
		panic("register operator account routes without principal provider")
	}
	h := &handler{
		startPhone:  startPhone,
		verifyPhone: verifyPhone,
		startQR:     startQR,
		refreshQR:   refreshQR,
		status:      status,
		states:      make(map[browserStateKey]browserState),
		actorLocks:  make(map[uuid.UUID]*sync.Mutex),
	}
	h.tmpl = template.Must(template.New("authenticate.html").ParseFS(assets, "templates/authenticate.html"))
	protected := router.With(principal.Middleware(provider))
	protected.Get("/operator-accounts/authenticate", h.authenticate)
	protected.Post("/operator-accounts/authenticate/phone", h.phone)
	protected.Post("/operator-accounts/authenticate/phone/code", h.code)
	protected.Post("/operator-accounts/authenticate/qr", h.qr)
	protected.Post("/operator-accounts/authenticate/qr/refresh", h.refresh)
	router.Get("/operator-accounts/authenticate/style.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "style.css")
	})
	router.Get("/operator-accounts/authenticate/authenticate.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "authenticate.js")
	})
}

func (h *handler) authenticate(w http.ResponseWriter, r *http.Request) {
	scope, ok := h.resolveScope(w, r)
	if !ok {
		return
	}
	csrf := h.ensureCSRF(w, r)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	snapshot := h.snapshot(scope)
	p := page{CSRF: csrf, Notice: notice(r.URL.Query().Get("notice")), Error: errorMessage(r.URL.Query().Get("error"))}

	if h.status != nil {
		current, err := h.status.Execute(r.Context(), scope.actor)
		if err != nil {
			p.Error = "Accounts are temporarily unavailable."
		} else {
			p.Accounts = mapAccounts(current.Accounts)
			snapshot = h.synchronizeStatus(scope, current)
		}
	}

	// The view model is built only after all state synchronization has happened.
	p.Phone = snapshot.state.Phone
	p.CodeSent = snapshot.state.PhoneID != uuid.Nil
	if snapshot.state.HasQR && snapshot.state.QRExpiry.After(time.Now()) {
		p.QR = makeQRView(snapshot.state.QR)
	}

	var body bytes.Buffer
	if err := h.tmpl.Execute(&body, p); err != nil {
		http.Error(w, "Unable to render page.", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(body.Bytes())
}

func (h *handler) phone(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	if !h.protectPost(w, r) {
		return
	}
	if err := parseForm(w, r); err != nil {
		redirectError(w, "invalid")
		return
	}
	phone := strings.TrimSpace(r.FormValue("phone"))
	if phone == "" || len(phone) > 32 {
		redirectError(w, "phone")
		return
	}
	scope := h.scopeForActor(w, r, actor)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	challenge, err := h.startPhone.Execute(r.Context(), scope.actor, phone)
	if err != nil {
		redirectError(w, "send-code")
		return
	}
	h.setPhone(scope, challenge)
	h.redirect(w, "/operator-accounts/authenticate?notice=code-sent")
}

func (h *handler) code(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	if !h.protectPost(w, r) {
		return
	}
	if err := parseForm(w, r); err != nil {
		redirectError(w, "invalid")
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	scope := h.scopeForActor(w, r, actor)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	snapshot := h.snapshot(scope)
	if snapshot.state.PhoneID == uuid.Nil || snapshot.state.PhoneExpiry.Before(time.Now()) || code == "" || len(code) > 16 {
		redirectError(w, "code")
		return
	}
	if _, err := h.verifyPhone.Execute(r.Context(), scope.actor, snapshot.state.PhoneID, code); err != nil {
		redirectError(w, "code")
		return
	}
	h.clearPhone(scope)
	h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
}

func (h *handler) qr(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	if !h.protectPost(w, r) {
		return
	}
	scope := h.scopeForActor(w, r, actor)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	challenge, err := h.startQR.Execute(r.Context(), scope.actor)
	if err != nil {
		redirectError(w, "qr-start")
		return
	}
	h.setQR(scope, challenge)
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
}

func (h *handler) refresh(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	if !h.protectPost(w, r) {
		return
	}
	scope := h.scopeForActor(w, r, actor)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	snapshot := h.snapshot(scope)
	if !snapshot.state.HasQR || snapshot.state.QRExpiry.Before(time.Now()) {
		redirectError(w, "qr-expired")
		return
	}
	challenge, err := h.refreshQR.Execute(r.Context(), scope.actor, snapshot.state.QR.RequestID)
	if err != nil {
		redirectError(w, "qr-refresh")
		return
	}
	h.setQR(scope, challenge)
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
}

func (h *handler) resolveScope(w http.ResponseWriter, r *http.Request) (requestScope, bool) {
	actor, ok := requestActor(w, r)
	if !ok {
		return requestScope{}, false
	}
	return h.scopeForActor(w, r, actor), true
}

func (h *handler) scopeForActor(w http.ResponseWriter, r *http.Request, actor application.Actor) requestScope {
	return requestScope{actor: actor, flowID: h.flowID(w, r)}
}

// lockActor serializes the complete backend operation and its browser-cache
// synchronization for one actor. The registry mutex is held only while
// looking up a per-actor lock; unrelated actors never wait for an external
// application operation belonging to another actor. This boundary is also
// where future external Telegram operations must remain contained: add the
// operation through these actor-scoped HTTP flows rather than introducing a
// second unsynchronized browser-state path.
func (h *handler) lockActor(actorID uuid.UUID) func() {
	h.actorMu.Lock()
	if h.actorLocks == nil {
		h.actorLocks = make(map[uuid.UUID]*sync.Mutex)
	}
	lock := h.actorLocks[actorID]
	if lock == nil {
		lock = &sync.Mutex{}
		h.actorLocks[actorID] = lock
	}
	h.actorMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func requestActor(w http.ResponseWriter, r *http.Request) (application.Actor, bool) {
	actor, ok := principal.FromContext(r.Context())
	if !ok {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
	return actor, ok
}

func (h *handler) protectPost(w http.ResponseWriter, r *http.Request) bool {
	if !sameOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	if err := parseForm(w, r); err != nil {
		http.Error(w, "Invalid form", http.StatusBadRequest)
		return false
	}
	cookie, err := r.Cookie(csrfCookie)
	if err != nil || len(cookie.Value) != 64 || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(r.FormValue("csrf_token"))) != 1 {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
}

func (h *handler) ensureCSRF(w http.ResponseWriter, r *http.Request) string {
	if cookie, err := r.Cookie(csrfCookie); err == nil && len(cookie.Value) == 64 {
		return cookie.Value
	}
	token := randomID(48) // 64 raw URL-safe characters
	http.SetCookie(w, &http.Cookie{Name: csrfCookie, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(stateLifetime.Seconds())})
	return token
}

func (h *handler) snapshot(scope requestScope) browserStateSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	h.cleanupLocked(now)
	key := browserStateKey{actorID: scope.actor.OperatorID, flowID: scope.flowID}
	state := h.states[key]
	h.expireStateLocked(&state, now)
	state.Touched = now
	h.states[key] = state
	return browserStateSnapshot{flowID: scope.flowID, state: state}
}

func (h *handler) synchronizeStatus(scope requestScope, current common.Status) browserStateSnapshot {
	// Copy the optional values before taking the browser-state lock. The
	// application status is a snapshot, but keeping that boundary explicit
	// prevents a caller-owned pointer from being retained in browser state.
	var phone *common.PhoneChallenge
	if current.PhoneChallenge != nil {
		challenge := *current.PhoneChallenge
		phone = &challenge
	}
	var qrChallenge *common.QRChallenge
	if current.QRChallenge != nil {
		challenge := *current.QRChallenge
		qrChallenge = &challenge
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	h.cleanupLocked(now)
	key := browserStateKey{actorID: scope.actor.OperatorID, flowID: scope.flowID}
	state := h.states[key]

	// A missing backend challenge is authoritative. In particular, do not
	// leave an old browser cache visible after a challenge was consumed or
	// otherwise disappeared from the actor-scoped service.
	state.Phone = ""
	state.PhoneID = uuid.Nil
	state.PhoneExpiry = time.Time{}
	if phone != nil && phone.RequestID != uuid.Nil && time.Now().Before(phone.ExpiresAt) {
		state.Phone = phone.Phone
		state.PhoneID = phone.RequestID
		state.PhoneExpiry = phone.ExpiresAt
	}
	state.QR = common.QRChallenge{}
	state.QRExpiry = time.Time{}
	state.HasQR = false
	if qrChallenge != nil && qrChallenge.RequestID != uuid.Nil && time.Now().Before(qrChallenge.ExpiresAt) {
		state.QR = *qrChallenge
		state.QRExpiry = qrChallenge.ExpiresAt
		state.HasQR = true
	}
	state.Touched = now
	h.states[key] = state
	return browserStateSnapshot{flowID: scope.flowID, state: state}
}

func (h *handler) setPhone(scope requestScope, challenge common.PhoneChallenge) {
	h.updateState(scope, func(state *browserState) {
		state.Phone = challenge.Phone
		state.PhoneID = challenge.RequestID
		state.PhoneExpiry = challenge.ExpiresAt
	})
}

func (h *handler) clearPhone(scope requestScope) {
	h.updateState(scope, func(state *browserState) {
		state.Phone = ""
		state.PhoneID = uuid.Nil
		state.PhoneExpiry = time.Time{}
	})
}

func (h *handler) setQR(scope requestScope, challenge common.QRChallenge) {
	h.updateState(scope, func(state *browserState) {
		state.QR = challenge
		state.QRExpiry = challenge.ExpiresAt
		state.HasQR = true
	})
}

func (h *handler) updateState(scope requestScope, update func(*browserState)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	h.cleanupLocked(now)
	key := browserStateKey{actorID: scope.actor.OperatorID, flowID: scope.flowID}
	state := h.states[key]
	h.expireStateLocked(&state, now)
	update(&state)
	state.Touched = now
	h.states[key] = state
}

func (h *handler) flowID(w http.ResponseWriter, r *http.Request) string {
	cookie, err := r.Cookie(sessionCookie)
	if err == nil && cookie.Value != "" {
		return cookie.Value
	}
	flowID := randomID(24)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: flowID, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(stateLifetime.Seconds())})
	return flowID
}

func (h *handler) cleanupLocked(now time.Time) {
	for key, state := range h.states {
		if state.Touched.Add(stateLifetime).Before(now) {
			delete(h.states, key)
			continue
		}
		hadChallenge := state.PhoneID != uuid.Nil || state.HasQR
		h.expireStateLocked(&state, now)
		if state.PhoneID == uuid.Nil {
			state.Phone = ""
			state.PhoneExpiry = time.Time{}
		}
		if !state.HasQR {
			state.QR = common.QRChallenge{}
			state.QRExpiry = time.Time{}
		}
		h.states[key] = state
		if hadChallenge && state.PhoneID == uuid.Nil && !state.HasQR {
			delete(h.states, key)
			continue
		}
		if state.PhoneID == uuid.Nil && !state.HasQR {
			continue
		}
		phoneExpired := state.PhoneID == uuid.Nil || !now.Before(state.PhoneExpiry)
		qrExpired := !state.HasQR || !now.Before(state.QRExpiry)
		if phoneExpired && qrExpired {
			delete(h.states, key)
		}
	}
	for len(h.states) > maxStateCount {
		var oldest browserStateKey
		var oldestTouched time.Time
		for key, state := range h.states {
			if oldestTouched.IsZero() || state.Touched.Before(oldestTouched) {
				oldest, oldestTouched = key, state.Touched
			}
		}
		delete(h.states, oldest)
	}
}

func (h *handler) expireStateLocked(state *browserState, now time.Time) {
	if state.PhoneID != uuid.Nil && !now.Before(state.PhoneExpiry) {
		state.Phone = ""
		state.PhoneID = uuid.Nil
		state.PhoneExpiry = time.Time{}
	}
	if state.HasQR && !now.Before(state.QRExpiry) {
		state.QR = common.QRChallenge{}
		state.QRExpiry = time.Time{}
		state.HasQR = false
	}
}

func (h *handler) redirect(w http.ResponseWriter, target string) {
	http.Redirect(w, &http.Request{URL: &url.URL{}}, target, http.StatusSeeOther)
}
func redirectError(w http.ResponseWriter, code string) {
	http.Redirect(w, &http.Request{URL: &url.URL{}}, "/operator-accounts/authenticate?error="+url.QueryEscape(code), http.StatusSeeOther)
}

func parseForm(w http.ResponseWriter, r *http.Request) error {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	return r.ParseForm()
}
func sameOrigin(r *http.Request) bool {
	seen := false
	for _, raw := range []string{r.Header.Get("Origin"), r.Header.Get("Referer")} {
		if raw == "" {
			continue
		}
		seen = true
		u, err := url.Parse(raw)
		if err != nil || u.Host != r.Host {
			return false
		}
		if u.Scheme != "" && r.URL.Scheme != "" && u.Scheme != r.URL.Scheme {
			return false
		}
		break
	}
	return seen
}
func randomID(size int) string {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic("operator authentication entropy unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func notice(code string) string {
	switch code {
	case "code-sent":
		return "A Telegram code was sent."
	case "account-added":
		return "Telegram account connected."
	case "qr-ready":
		return "Scan the QR code with Telegram."
	}
	return ""
}
func errorMessage(code string) string {
	switch code {
	case "phone":
		return "Please enter a valid phone number."
	case "code":
		return "That code was not accepted or has expired."
	case "send-code":
		return "We could not send a code. Try again."
	case "qr-start", "qr-refresh", "qr-expired":
		return "QR sign-in is unavailable right now."
	case "invalid":
		return "Please check the form and try again."
	}
	return ""
}

func mapAccounts(accounts []common.Account) []accountRow {
	rows := make([]accountRow, 0, len(accounts))
	for _, account := range accounts {
		name := strings.TrimSpace(strings.TrimSpace(account.TelegramFirstName + " " + account.TelegramLastName))
		if account.TelegramUsername != "" {
			if name != "" {
				name += " · "
			}
			name += "@" + account.TelegramUsername
		}
		if name == "" {
			name = "Telegram account"
		}
		rows = append(rows, accountRow{Name: name, Phone: account.Phone, State: "Connected"})
	}
	return rows
}
func makeQRView(challenge common.QRChallenge) *qrView {
	image, err := qr.Encode(challenge.URL, qr.M)
	if err != nil {
		return nil
	}
	return &qrView{Image: "data:image/png;base64," + base64.StdEncoding.EncodeToString(image.PNG()), Expires: challenge.ExpiresAt.UnixMilli()}
}

var _ common.Account
var _ common.PhoneChallenge
