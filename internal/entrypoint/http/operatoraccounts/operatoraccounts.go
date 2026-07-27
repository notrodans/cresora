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
	commands "github.com/notrodans/nebula-go/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/nebula-go/internal/application/operatoraccountauth"
	requests "github.com/notrodans/nebula-go/internal/application/requests/operator-account-auth"
	"rsc.io/qr"
)

const (
	csrfCookie    = "nebula_operator_csrf"
	sessionCookie = "nebula_operator_session"
	stateLifetime = 10 * time.Minute
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
	states      map[string]*browserState
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
func New(startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status) chi.Router {
	r := chi.NewRouter()
	Register(r, startPhone, verifyPhone, startQR, refreshQR, status)
	return r
}

// Register adds the account authentication routes to an existing chi router.
func Register(router chi.Router, startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status) {
	h := &handler{
		startPhone:  startPhone,
		verifyPhone: verifyPhone,
		startQR:     startQR,
		refreshQR:   refreshQR,
		status:      status,
		states:      make(map[string]*browserState),
	}
	h.tmpl = template.Must(template.New("authenticate.html").ParseFS(assets, "templates/authenticate.html"))
	router.Get("/operator-accounts/authenticate", h.authenticate)
	router.Post("/operator-accounts/authenticate/phone", h.phone)
	router.Post("/operator-accounts/authenticate/phone/code", h.code)
	router.Post("/operator-accounts/authenticate/qr", h.qr)
	router.Post("/operator-accounts/authenticate/qr/refresh", h.refresh)
	router.Get("/operator-accounts/authenticate/style.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "style.css")
	})
	router.Get("/operator-accounts/authenticate/authenticate.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "authenticate.js")
	})
}

func (h *handler) authenticate(w http.ResponseWriter, r *http.Request) {
	csrf := h.ensureCSRF(w, r)
	state := h.state(w, r)
	p := page{CSRF: csrf, Phone: state.Phone, CodeSent: state.PhoneID != uuid.Nil, Notice: notice(r.URL.Query().Get("notice")), Error: errorMessage(r.URL.Query().Get("error"))}

	if h.status != nil {
		current, err := h.status.Execute(r.Context())
		if err != nil {
			p.Error = "Accounts are temporarily unavailable."
		} else {
			p.Accounts = mapAccounts(current.Accounts)
			if current.PhoneChallenge != nil && !current.PhoneChallenge.ExpiresAt.Before(time.Now()) {
				state.Phone, state.PhoneID, state.PhoneExpiry = current.PhoneChallenge.Phone, current.PhoneChallenge.RequestID, current.PhoneChallenge.ExpiresAt
			}
			if current.QRChallenge != nil && !current.QRChallenge.ExpiresAt.Before(time.Now()) {
				state.QR, state.QRExpiry, state.HasQR = *current.QRChallenge, current.QRChallenge.ExpiresAt, true
			}
		}
	}
	if state.HasQR && state.QRExpiry.After(time.Now()) {
		p.QR = makeQRView(state.QR)
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
	challenge, err := h.startPhone.Execute(r.Context(), phone)
	if err != nil {
		redirectError(w, "send-code")
		return
	}
	state := h.state(w, r)
	state.Phone, state.PhoneID, state.PhoneExpiry, state.Touched = phone, challenge.RequestID, challenge.ExpiresAt, time.Now()
	h.redirect(w, "/operator-accounts/authenticate?notice=code-sent")
}

func (h *handler) code(w http.ResponseWriter, r *http.Request) {
	if !h.protectPost(w, r) {
		return
	}
	if err := parseForm(w, r); err != nil {
		redirectError(w, "invalid")
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	state := h.state(w, r)
	if state.PhoneID == uuid.Nil || state.PhoneExpiry.Before(time.Now()) || code == "" || len(code) > 16 {
		redirectError(w, "code")
		return
	}
	if _, err := h.verifyPhone.Execute(r.Context(), state.PhoneID, code); err != nil {
		redirectError(w, "code")
		return
	}
	state.Phone, state.PhoneID = "", uuid.Nil
	h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
}

func (h *handler) qr(w http.ResponseWriter, r *http.Request) {
	if !h.protectPost(w, r) {
		return
	}
	challenge, err := h.startQR.Execute(r.Context())
	if err != nil {
		redirectError(w, "qr-start")
		return
	}
	state := h.state(w, r)
	state.QR, state.QRExpiry, state.HasQR, state.Touched = challenge, challenge.ExpiresAt, true, time.Now()
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
}

func (h *handler) refresh(w http.ResponseWriter, r *http.Request) {
	if !h.protectPost(w, r) {
		return
	}
	state := h.state(w, r)
	if !state.HasQR || state.QRExpiry.Before(time.Now()) {
		redirectError(w, "qr-expired")
		return
	}
	challenge, err := h.refreshQR.Execute(r.Context(), state.QR.RequestID)
	if err != nil {
		redirectError(w, "qr-refresh")
		return
	}
	state.QR, state.QRExpiry, state.Touched = challenge, challenge.ExpiresAt, time.Now()
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
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
	token := randomID(32)
	http.SetCookie(w, &http.Cookie{Name: csrfCookie, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(stateLifetime.Seconds())})
	return token
}

func (h *handler) state(w http.ResponseWriter, r *http.Request) *browserState {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for key, value := range h.states {
		if value.Touched.Add(stateLifetime).Before(now) || (!value.QRExpiry.IsZero() && value.QRExpiry.Before(now) && value.PhoneExpiry.Before(now)) {
			delete(h.states, key)
		}
	}
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || cookie.Value == "" {
		key := randomID(24)
		h.states[key] = &browserState{Touched: now}
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: key, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(stateLifetime.Seconds())})
		return h.states[key]
	}
	if state := h.states[cookie.Value]; state != nil {
		state.Touched = now
		return state
	}
	h.states[cookie.Value] = &browserState{Touched: now}
	return h.states[cookie.Value]
}

func (h *handler) redirect(w http.ResponseWriter, target string) {
	http.Redirect(w, nil, target, http.StatusSeeOther)
}
func redirectError(w http.ResponseWriter, code string) {
	http.Redirect(w, &http.Request{}, "/operator-accounts/authenticate?error="+url.QueryEscape(code), http.StatusSeeOther)
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
