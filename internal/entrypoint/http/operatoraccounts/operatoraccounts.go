// Package operatoraccounts provides the operator Telegram account sign-in page.
package operatoraccounts

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"errors"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	challengecoordinator "github.com/notrodans/cresora/internal/application/operatoraccountauth/challenges"
	requests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	"rsc.io/qr"
)

const (
	productionCSRFCookie    = "__Host-cresora_operator_csrf"
	productionSessionCookie = "__Host-cresora_operator_session"
	localCSRFCookie         = "cresora_operator_csrf"
	localSessionCookie      = "cresora_operator_session"
	// These aliases describe the secure-by-default route used by the existing
	// tests. Runtime handlers use the names from CookieConfig instead.
	csrfCookie    = productionCSRFCookie
	sessionCookie = productionSessionCookie
	stateLifetime = 10 * time.Minute
)

// CookieConfig is the deployment policy for the operator-account browser
// flow. Secure cookies use the __Host- prefix, which requires Secure, Path=/,
// and no Domain attribute. The only supported insecure policy is an explicit
// local development/testing exception; callers must not infer it from an
// incoming request.
type CookieConfig struct {
	CSRFCookieName     string
	SessionCookieName  string
	Secure             bool
	AllowInsecureLocal bool
}

// SecureCookieConfig returns the policy used by HTTPS, staging, and
// production deployments.
func SecureCookieConfig() CookieConfig {
	return CookieConfig{
		CSRFCookieName:    productionCSRFCookie,
		SessionCookieName: productionSessionCookie,
		Secure:            true,
	}
}

// LocalInsecureCookieConfig is deliberately explicit and is only suitable for
// development/testing on a local HTTP origin. The composition root must use
// this only when its session configuration grants the same exception.
func LocalInsecureCookieConfig() CookieConfig {
	return CookieConfig{
		CSRFCookieName:     localCSRFCookie,
		SessionCookieName:  localSessionCookie,
		AllowInsecureLocal: true,
	}
}

// NewCookieConfig selects the operator-account cookie names from the same
// secure/local decision used by the authenticated session cookie.
func NewCookieConfig(secure, allowInsecureLocal bool) CookieConfig {
	if secure {
		return SecureCookieConfig()
	}
	if allowInsecureLocal {
		return LocalInsecureCookieConfig()
	}
	return CookieConfig{}
}

// ValidateCookieConfig checks the complete browser cookie policy before any
// routes are registered. It is exported for composition-level tests and for
// callers that construct the HTTP graph outside cmd/app.
func ValidateCookieConfig(cookie CookieConfig) error {
	if cookie.CSRFCookieName == "" || cookie.SessionCookieName == "" {
		return errors.New("operator-account cookie names are required")
	}
	hostCSRF := strings.HasPrefix(cookie.CSRFCookieName, "__Host-")
	hostSession := strings.HasPrefix(cookie.SessionCookieName, "__Host-")
	if cookie.Secure != hostCSRF || cookie.Secure != hostSession {
		return errors.New("Secure operator-account cookies must use the __Host- policy")
	}
	if cookie.Secure {
		if cookie.CSRFCookieName != productionCSRFCookie || cookie.SessionCookieName != productionSessionCookie {
			return errors.New("Secure operator-account cookies must use the configured host-only names")
		}
		if cookie.AllowInsecureLocal {
			return errors.New("local insecure operator-account cookies cannot be combined with Secure cookies")
		}
		return nil
	}
	if !cookie.AllowInsecureLocal {
		return errors.New("insecure operator-account cookies require explicit local development/testing configuration")
	}
	if cookie.CSRFCookieName != localCSRFCookie || cookie.SessionCookieName != localSessionCookie {
		return errors.New("insecure operator-account cookies must use local names")
	}
	return nil
}

// RouteMode controls whether the account-authentication commands are exposed.
// An empty mode is normalized to RouteDisabled, the safe choice for a
// composition root without live Telegram adapters. DevelopmentTestMock is an
// explicit test composition only; it is never selected by production code.
type RouteMode string

const (
	RouteDisabled            RouteMode = "disabled"
	RouteLive                RouteMode = "live"
	RouteDevelopmentTestMock RouteMode = "development-test-mock"
)

// DeploymentEnvironment is kept at the HTTP composition boundary so a mock
// route cannot be accidentally selected for a production or staging graph.
type DeploymentEnvironment string

const (
	EnvironmentProduction  DeploymentEnvironment = "PRODUCTION"
	EnvironmentDevelopment DeploymentEnvironment = "DEVELOPMENT"
	EnvironmentTesting     DeploymentEnvironment = "TESTING"
	EnvironmentStaging     DeploymentEnvironment = "STAGING"
)

// RouteOptions contains the non-request-derived route policy. In particular,
// cookie security, deployment environment, and command availability are fixed
// at composition time.
type RouteOptions struct {
	Mode                     RouteMode
	Environment              DeploymentEnvironment
	Cookie                   CookieConfig
	AllowDevelopmentTestMock bool
}

const unavailableMessage = "Раздел управления аккаунтами временно недоступен. Попробуйте позже."

//go:embed templates/authenticate.html style.css authenticate.js
var assets embed.FS

type handler struct {
	startPhone   commands.StartPhone
	verifyPhone  commands.VerifyPhone
	startQR      commands.StartQR
	refreshQR    commands.RefreshQR
	status       requests.Status
	tmpl         *template.Template
	actorMu      sync.Mutex
	actorLocks   map[uuid.UUID]*sync.Mutex
	publicOrigin *url.URL
	disabled     bool
	cookie       CookieConfig
}

type page struct {
	Accounts           []accountRow
	Phone              string
	PhoneRequestID     string
	CodeSent           bool
	QR                 *qrView
	QRRequestID        string
	CSRF               string
	Notice             string
	Error              string
	ErrorFocus         bool
	Unavailable        bool
	UnavailableMessage string
	ReturnToConsole    string
}

// requestScope is resolved once at the HTTP boundary. The actor comes from
// the trusted principal middleware. The legacy flow cookie is retained only
// for the existing browser-session contract; challenge state and ownership
// come exclusively from the application coordinator.
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
func New(startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status, provider principal.Provider, publicOrigin ...string) chi.Router {
	r := chi.NewRouter()
	Register(r, startPhone, verifyPhone, startQR, refreshQR, status, provider, publicOrigin...)
	return r
}

// NewWithOptions constructs a router with an explicit deployment policy. Use
// RouteDevelopmentTestMock only from an intentionally isolated development or
// test composition. Production/staging composition should use RouteDisabled
// until live Telegram adapters are available.
func NewWithOptions(startPhone commands.StartPhone, verifyPhone commands.VerifyPhone, startQR commands.StartQR, refreshQR commands.RefreshQR, status requests.Status, provider principal.Provider, publicOrigin string, options RouteOptions) chi.Router {
	r := chi.NewRouter()
	RegisterWithOptions(r, startPhone, verifyPhone, startQR, refreshQR, status, provider, publicOrigin, options)
	return r
}

// NewWithChallengeCoordinator composes the application-only coordinator into
// an explicitly supplied HTTP route. It is intended for development/testing
// with challenges/fake; production and staging must continue to use the
// authenticated RouteDisabled composition until a real adapter is approved.
// Account rows are intentionally empty because the coordinator does not read
// or write account persistence.
func NewWithChallengeCoordinator(
	coordinator *challengecoordinator.Coordinator,
	provider principal.Provider,
	publicOrigin string,
	options RouteOptions,
) chi.Router {
	if coordinator == nil {
		panic("register operator account routes without challenge coordinator")
	}
	ports := coordinator.CQS()
	return NewWithOptions(ports.StartPhone, ports.VerifyPhone, ports.StartQR, ports.RefreshQR, ports.Status, provider, publicOrigin, options)
}

// Register adds the account authentication routes to an existing chi router.
func Register(
	router chi.Router,
	startPhone commands.StartPhone,
	verifyPhone commands.VerifyPhone,
	startQR commands.StartQR,
	refreshQR commands.RefreshQR,
	status requests.Status,
	provider principal.Provider,
	configuredOrigin ...string,
) {
	originValue := "http://example.test"
	if len(configuredOrigin) > 0 && configuredOrigin[0] != "" {
		originValue = configuredOrigin[0]
	}
	// The legacy constructor remains available for the explicitly composed
	// in-memory development/test handlers. The application composition root
	// uses RegisterWithOptions with RouteDisabled instead.
	RegisterWithOptions(router, startPhone, verifyPhone, startQR, refreshQR, status, provider, originValue, RouteOptions{
		Mode:   RouteLive,
		Cookie: SecureCookieConfig(),
	})
}

// RegisterWithOptions adds the account authentication routes to an existing
// chi router with an explicit command and cookie policy.
func RegisterWithOptions(
	router chi.Router,
	startPhone commands.StartPhone,
	verifyPhone commands.VerifyPhone,
	startQR commands.StartQR,
	refreshQR commands.RefreshQR,
	status requests.Status,
	provider principal.Provider,
	configuredOrigin string,
	options RouteOptions,
) {
	if router == nil {
		panic("register operator account routes with missing router")
	}
	if provider == nil {
		panic("register operator account routes without principal provider")
	}
	if configuredOrigin == "" {
		configuredOrigin = "http://example.test"
	}
	origin, failure := parsePublicOrigin(configuredOrigin)
	if failure != nil {
		panic(failure)
	}
	if options.Mode == "" {
		options.Mode = RouteDisabled
	}
	if options.Cookie == (CookieConfig{}) {
		options.Cookie = SecureCookieConfig()
	}
	if failure := ValidateCookieConfig(options.Cookie); failure != nil {
		panic(failure)
	}
	if !options.Cookie.Secure && (!strings.EqualFold(origin.Scheme, "http") || !isLocalOriginHost(origin)) {
		panic("insecure operator-account cookies require a local HTTP origin")
	}
	if options.Mode == RouteLive && (options.Environment == EnvironmentProduction || options.Environment == EnvironmentStaging) {
		// Until live Telegram auth is explicitly approved, production and
		// staging must use the same inert authenticated 503 as RouteDisabled.
		options.Mode = RouteDisabled
	}
	switch options.Mode {
	case RouteDisabled:
		// A disabled route deliberately accepts nil command ports. This keeps
		// the unavailable composition unable to call a mock or a partially
		// initialized Telegram adapter.
	case RouteLive:
		if startPhone == nil || verifyPhone == nil || startQR == nil || refreshQR == nil || status == nil {
			panic("register enabled operator account routes with missing command")
		}
	case RouteDevelopmentTestMock:
		if !options.AllowDevelopmentTestMock {
			panic("development/test mock route requires explicit opt-in")
		}
		if options.Environment != EnvironmentDevelopment && options.Environment != EnvironmentTesting {
			panic("development/test mock route requires DEVELOPMENT or TESTING environment")
		}
		if startPhone == nil || verifyPhone == nil || startQR == nil || refreshQR == nil || status == nil {
			panic("register enabled operator account routes with missing command")
		}
	default:
		panic("register operator account routes with unknown mode")
	}
	h := &handler{
		startPhone:   startPhone,
		verifyPhone:  verifyPhone,
		startQR:      startQR,
		refreshQR:    refreshQR,
		status:       status,
		actorLocks:   make(map[uuid.UUID]*sync.Mutex),
		publicOrigin: origin,
		disabled:     options.Mode == RouteDisabled,
		cookie:       options.Cookie,
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
	noStore(w)
	if h.unavailable(w) {
		return
	}
	scope, ok := h.resolveScope(w, r)
	if !ok {
		return
	}
	csrf := h.ensureCSRF(w, r)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	p := page{CSRF: csrf, Notice: notice(r.URL.Query().Get("notice")), Error: errorMessage(r.URL.Query().Get("error"))}
	p.ErrorFocus = p.Error != ""

	if h.status != nil {
		current, err := h.status.Execute(r.Context(), scope.actor)
		if err != nil {
			p.Error = "Accounts are temporarily unavailable."
			p.ErrorFocus = true
		} else {
			p.Accounts = mapAccounts(current.Accounts)
			if current.PhoneChallenge != nil && time.Now().Before(current.PhoneChallenge.ExpiresAt) {
				p.Phone = current.PhoneChallenge.Phone
				p.PhoneRequestID = current.PhoneChallenge.RequestID.String()
				p.CodeSent = true
			}
			if current.QRChallenge != nil && time.Now().Before(current.QRChallenge.ExpiresAt) {
				p.QR = makeQRView(*current.QRChallenge)
				p.QRRequestID = current.QRChallenge.RequestID.String()
			}
		}
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
	noStore(w)
	if h.unavailable(w) {
		return
	}
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
	_, err := h.startPhone.Execute(r.Context(), scope.actor, phone)
	if err != nil {
		redirectError(w, "send-code")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=code-sent")
}

func (h *handler) code(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
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
	requestIDValue := strings.TrimSpace(r.FormValue("challenge_request_id"))
	scope := h.scopeForActor(w, r, actor)
	unlock := h.lockActor(scope.actor.OperatorID)
	defer unlock()
	current, err := h.status.Execute(r.Context(), scope.actor)
	requestID := uuid.Nil
	if requestIDValue != "" {
		requestID, err = uuid.Parse(requestIDValue)
		if err != nil {
			redirectError(w, "code")
			return
		}
	} else if current.PhoneChallenge != nil {
		requestID = current.PhoneChallenge.RequestID
	}
	if (err != nil && requestIDValue == "") || requestID == uuid.Nil || (requestIDValue == "" && (current.PhoneChallenge == nil || !time.Now().Before(current.PhoneChallenge.ExpiresAt))) || code == "" || len(code) > 16 {
		redirectError(w, "code")
		return
	}
	if _, err := h.verifyPhone.Execute(r.Context(), scope.actor, requestID, code); err != nil {
		redirectError(w, "code")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
}

func (h *handler) qr(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
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
	_, err := h.startQR.Execute(r.Context(), scope.actor)
	if err != nil {
		redirectError(w, "qr-start")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
}

func (h *handler) refresh(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
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
	current, err := h.status.Execute(r.Context(), scope.actor)
	requestIDValue := strings.TrimSpace(r.FormValue("challenge_request_id"))
	requestID := uuid.Nil
	if requestIDValue != "" {
		requestID, err = uuid.Parse(requestIDValue)
		if err != nil {
			redirectError(w, "qr-refresh")
			return
		}
	} else if current.QRChallenge != nil {
		requestID = current.QRChallenge.RequestID
	}
	if (err != nil && requestIDValue == "") || requestID == uuid.Nil || (requestIDValue == "" && (current.QRChallenge == nil || !time.Now().Before(current.QRChallenge.ExpiresAt))) {
		redirectError(w, "qr-expired")
		return
	}
	_, err = h.refreshQR.Execute(r.Context(), scope.actor, requestID)
	if err != nil {
		redirectError(w, "qr-refresh")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=qr-ready")
}

func (h *handler) resolveScope(w http.ResponseWriter, r *http.Request) (requestScope, bool) {
	actor, ok := requestActor(w, r)
	if !ok {
		return requestScope{}, false
	}
	return h.scopeForActor(w, r, actor), true
}

func (h *handler) unavailable(w http.ResponseWriter) bool {
	if h == nil || !h.disabled {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	page := page{
		// Keep this state explicit in the view model so the designer can replace
		// the normal sign-in controls with a recovery alert and a safe local
		// return action without inferring availability from empty fields.
		Unavailable:        true,
		UnavailableMessage: unavailableMessage,
		ReturnToConsole:    "/",
		// Keep the normal error field populated with the same fixed,
		// non-sensitive message for alternate templates that use it as their
		// generic alert fallback.
		Error:      unavailableMessage,
		ErrorFocus: true,
	}
	var body bytes.Buffer
	if err := h.tmpl.Execute(&body, page); err != nil {
		http.Error(w, "Unable to render page.", http.StatusInternalServerError)
		return true
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(body.Bytes())
	return true
}

func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
}

func (h *handler) scopeForActor(w http.ResponseWriter, r *http.Request, actor application.Actor) requestScope {
	return requestScope{actor: actor, flowID: h.flowID(w, r)}
}

// lockActor serializes the complete HTTP operation for one actor. The registry
// mutex is held only while looking up a per-actor lock; unrelated actors never
// wait for an external application operation belonging to another actor. The
// lock is not the challenge store: coordinator state remains the sole source
// of truth and its mutex is never held across provider calls.
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
	if !h.sameOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	if err := parseForm(w, r); err != nil {
		http.Error(w, "Invalid form", http.StatusBadRequest)
		return false
	}
	cookie, err := r.Cookie(h.cookie.CSRFCookieName)
	if err != nil || len(cookie.Value) != 64 || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(r.FormValue("csrf_token"))) != 1 {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
}

func (h *handler) ensureCSRF(w http.ResponseWriter, r *http.Request) string {
	if cookie, err := r.Cookie(h.cookie.CSRFCookieName); err == nil && len(cookie.Value) == 64 {
		return cookie.Value
	}
	token := randomID(48) // 64 raw URL-safe characters
	http.SetCookie(w, h.newCookie(h.cookie.CSRFCookieName, token, int(stateLifetime.Seconds())))
	return token
}

func (h *handler) flowID(w http.ResponseWriter, r *http.Request) string {
	cookie, err := r.Cookie(h.cookie.SessionCookieName)
	if err == nil && cookie.Value != "" {
		return cookie.Value
	}
	flowID := randomID(24)
	http.SetCookie(w, h.newCookie(h.cookie.SessionCookieName, flowID, int(stateLifetime.Seconds())))
	return flowID
}

func (h *handler) newCookie(name, value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.cookie.Secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
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
func (h *handler) sameOrigin(r *http.Request) bool {
	seen := false
	for index, raw := range []string{r.Header.Get("Origin"), r.Header.Get("Referer")} {
		if raw == "" {
			continue
		}
		seen = true
		u, err := url.Parse(raw)
		if err != nil || u.User != nil {
			return false
		}
		if h.publicOrigin != nil {
			if !strings.EqualFold(u.Scheme, h.publicOrigin.Scheme) || !strings.EqualFold(u.Host, h.publicOrigin.Host) {
				return false
			}
			if index == 0 && (u.Path != "" || u.RawQuery != "" || u.Fragment != "") {
				return false
			}
		}
	}
	return seen
}

func parsePublicOrigin(value string) (*url.URL, error) {
	parsed, failure := url.Parse(value)
	if failure != nil || parsed.User != nil || parsed.Host == "" || (!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return nil, errors.New("public origin must be an absolute HTTP(S) URL")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("public origin must contain only a scheme and host")
	}
	parsed.Path = ""
	return parsed, nil
}

func isLocalOriginHost(origin *url.URL) bool {
	if origin == nil {
		return false
	}
	hostname := origin.Hostname()
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	address := net.ParseIP(hostname)
	return address != nil && address.IsLoopback()
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
