// Package authentication exposes the local operator login boundary. It does
// not know how credentials or sessions are stored; that contract belongs to
// the application and PostgreSQL adapters.
package authentication

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"html/template"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	application "github.com/notrodans/cresora/internal/application"
	operatorsessions "github.com/notrodans/cresora/internal/application/operatorsessions"
	"github.com/notrodans/cresora/internal/entrypoint/http/operatorlogin"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
)

const (
	preAuthCSRFCookie  = "cresora_login_csrf"
	preAuthFlowCookie  = "cresora_login_flow"
	csrfFormField      = "csrf"
	maxLoginBody       = 8 << 10
	loginRefreshNotice = "refresh"
)

// Authenticator is the transport-facing subset of the application service.
type Authenticator interface {
	Login(context.Context, string, string) (operatorsessions.Session, error)
	Revoke(context.Context, string) error
}

// CookieConfig is intentionally small so cookie policy is configured at the
// composition root, not inferred from request headers. Insecure cookies
// require the explicit local-development/testing opt-in.
type CookieConfig struct {
	Name               string
	Secure             bool
	AllowInsecureLocal bool
}

// SessionProvider validates the configured session cookie against the server-
// side repository. It implements principal.SessionProvider, making the raw
// value available only to the HTTP context for CSRF derivation.
type SessionProvider struct {
	service operatorsessions.Service
	config  CookieConfig
}

func NewSessionProvider(service operatorsessions.Service, cookie CookieConfig) *SessionProvider {
	if failure := validateCookieConfig(cookie); failure != nil {
		panic(failure)
	}
	return &SessionProvider{service: service, config: cookie}
}

func (provider *SessionProvider) Provide(request *http.Request) (application.Actor, error) {
	actor, _, failure := provider.ProvideSession(request)
	return actor, failure
}

func (provider *SessionProvider) ProvideSession(request *http.Request) (application.Actor, string, error) {
	if provider == nil || request == nil {
		return application.Actor{}, "", operatorsessions.ErrSessionInvalid
	}
	cookie, failure := request.Cookie(provider.config.Name)
	if failure != nil || cookie.Value == "" {
		return application.Actor{}, "", operatorsessions.ErrSessionInvalid
	}
	session, failure := provider.service.Validate(request.Context(), cookie.Value)
	if failure != nil {
		return application.Actor{}, "", operatorsessions.ErrSessionInvalid
	}
	return application.Actor{OperatorID: session.OperatorID}, cookie.Value, nil
}

type Handler struct {
	service      Authenticator
	provider     principal.Provider
	publicOrigin *url.URL
	cookie       CookieConfig
	rateLimiter  *LoginRateLimiter
	template     *template.Template
}

// LoginRateLimitConfig bounds both the per-identity attempt stream and the
// process-local accounting table. RemoteAddr is deliberately the only network
// identity accepted by LoginRateLimiter; forwarded headers are never trusted.
type LoginRateLimitConfig struct {
	Window          time.Duration
	AttemptsPerKey  int
	GlobalAttempts  int
	MaxEntries      int
	CleanupInterval time.Duration
}

const (
	defaultLoginRateLimitWindow        = time.Minute
	defaultLoginAttemptsPerKey         = 5
	defaultLoginGlobalAttempts         = 300
	defaultLoginRateLimitMaxEntries    = 4096
	defaultLoginRateLimitCleanupPeriod = 30 * time.Second
)

// DefaultLoginRateLimitConfig is intentionally conservative. It is an
// application-process safety net, not a claim that an upstream edge/WAF or
// reverse proxy rate-limits this endpoint.
func DefaultLoginRateLimitConfig() LoginRateLimitConfig {
	return LoginRateLimitConfig{
		Window:          defaultLoginRateLimitWindow,
		AttemptsPerKey:  defaultLoginAttemptsPerKey,
		GlobalAttempts:  defaultLoginGlobalAttempts,
		MaxEntries:      defaultLoginRateLimitMaxEntries,
		CleanupInterval: defaultLoginRateLimitCleanupPeriod,
	}
}

var processLoginRateLimiter = NewLoginRateLimiter(DefaultLoginRateLimitConfig())

type loginRateLimitEntry struct {
	windowStarted time.Time
	lastSeen      time.Time
	attempts      int
}

// LoginRateLimiter is safe for concurrent requests and has a hard upper
// bound on its identity table. Expired entries are cleaned periodically and a
// full table rejects new identities rather than growing without bound.
type LoginRateLimiter struct {
	mu          sync.Mutex
	config      LoginRateLimitConfig
	entries     map[string]loginRateLimitEntry
	global      loginRateLimitEntry
	lastCleanup time.Time
	now         func() time.Time
}

func NewLoginRateLimiter(config LoginRateLimitConfig) *LoginRateLimiter {
	defaults := DefaultLoginRateLimitConfig()
	if config.Window <= 0 {
		config.Window = defaults.Window
	}
	if config.AttemptsPerKey <= 0 {
		config.AttemptsPerKey = defaults.AttemptsPerKey
	}
	if config.GlobalAttempts <= 0 {
		config.GlobalAttempts = defaults.GlobalAttempts
	}
	if config.MaxEntries <= 0 {
		config.MaxEntries = defaults.MaxEntries
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = defaults.CleanupInterval
	}
	return &LoginRateLimiter{
		config:  config,
		entries: make(map[string]loginRateLimitEntry),
		now:     time.Now,
	}
}

// Allow records one login attempt. A false result is intentionally
// indistinguishable from bad credentials to the HTTP caller.
func (limiter *LoginRateLimiter) Allow(username, remoteAddr string) bool {
	now := limiter.now()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	if limiter.lastCleanup.IsZero() || now.Sub(limiter.lastCleanup) >= limiter.config.CleanupInterval {
		for key, entry := range limiter.entries {
			if now.Sub(entry.lastSeen) >= limiter.config.Window {
				delete(limiter.entries, key)
			}
		}
		limiter.lastCleanup = now
	}
	if now.Sub(limiter.global.windowStarted) >= limiter.config.Window {
		limiter.global = loginRateLimitEntry{windowStarted: now}
	}
	if limiter.global.windowStarted.IsZero() {
		limiter.global.windowStarted = now
	}
	if limiter.global.attempts >= limiter.config.GlobalAttempts {
		return false
	}
	limiter.global.attempts++

	key := CanonicalLoginUsername(username) + "\x00" + directRemoteIP(remoteAddr)
	entry, exists := limiter.entries[key]
	if !exists {
		if len(limiter.entries) >= limiter.config.MaxEntries {
			return false
		}
		entry = loginRateLimitEntry{windowStarted: now}
	}
	if now.Sub(entry.windowStarted) >= limiter.config.Window {
		entry = loginRateLimitEntry{windowStarted: now}
	}
	entry.lastSeen = now
	if entry.attempts >= limiter.config.AttemptsPerKey {
		limiter.entries[key] = entry
		return false
	}
	entry.attempts++
	limiter.entries[key] = entry
	return true
}

// Len is a diagnostic/test seam for the bounded accounting guarantee.
func (limiter *LoginRateLimiter) Len() int {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return len(limiter.entries)
}

// CanonicalLoginUsername groups equivalent case/outer-whitespace spellings
// for throttling without changing the username sent to credential storage.
func CanonicalLoginUsername(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

func directRemoteIP(remoteAddr string) string {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if host, _, failure := net.SplitHostPort(remoteAddr); failure == nil {
		remoteAddr = host
	}
	remoteAddr = strings.Trim(remoteAddr, "[]")
	if address, failure := netip.ParseAddr(remoteAddr); failure == nil {
		return address.Unmap().String()
	}
	if remoteAddr == "" {
		return "<unknown>"
	}
	return strings.ToLower(remoteAddr)
}

type loginPage struct {
	CSRFToken  string
	Next       string
	Username   string
	Notice     string
	Error      string
	ErrorFocus bool
}

// New returns an independent authentication router using the designer-owned
// package-local login template and stylesheet.
func New(service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig) chi.Router {
	router := chi.NewRouter()
	Register(router, service, provider, publicOrigin, cookie)
	return router
}

// NewWithRateLimiter is a deterministic composition seam for tests and
// explicitly isolated deployments. Production should normally use New, which
// shares the bounded process-local default across registered routers.
func NewWithRateLimiter(service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig, limiter *LoginRateLimiter) chi.Router {
	router := chi.NewRouter()
	RegisterWithRateLimiter(router, service, provider, publicOrigin, cookie, limiter)
	return router
}

func Register(router chi.Router, service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig) {
	RegisterWithRateLimiter(router, service, provider, publicOrigin, cookie, processLoginRateLimiter)
}

func RegisterWithRateLimiter(router chi.Router, service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig, limiter *LoginRateLimiter) {
	origin, failure := parsePublicOrigin(publicOrigin)
	if failure != nil {
		panic(failure)
	}
	if failure := validateCookieConfig(cookie); failure != nil {
		panic(failure)
	}
	handler := &Handler{
		service:      service,
		provider:     provider,
		publicOrigin: origin,
		cookie:       cookie,
		rateLimiter:  limiter,
		template:     template.Must(template.New("login.html").ParseFS(operatorlogin.Assets, "templates/login.html")),
	}
	router.HandleFunc("/login", handler.loginDispatch)
	router.Get("/login/style.css", func(response http.ResponseWriter, request *http.Request) {
		http.ServeFileFS(response, request, operatorlogin.Assets, "static/style.css")
	})
	protected := router.With(principal.Middleware(provider))
	protected.Post("/logout", handler.logout)
}

// NewWithTemplate is the same production handler with an explicitly supplied
// parsed template, useful for a composition root with a different design.
func NewWithTemplate(service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig, loginTemplate *template.Template) chi.Router {
	return NewWithTemplateAndRateLimiter(service, provider, publicOrigin, cookie, loginTemplate, processLoginRateLimiter)
}

func NewWithTemplateAndRateLimiter(service Authenticator, provider principal.Provider, publicOrigin string, cookie CookieConfig, loginTemplate *template.Template, limiter *LoginRateLimiter) chi.Router {
	router := chi.NewRouter()
	origin, failure := parsePublicOrigin(publicOrigin)
	if failure != nil {
		panic(failure)
	}
	if failure := validateCookieConfig(cookie); failure != nil {
		panic(failure)
	}
	handler := &Handler{service: service, provider: provider, publicOrigin: origin, cookie: cookie, rateLimiter: limiter, template: loginTemplate}
	router.HandleFunc("/login", handler.loginDispatch)
	router.Get("/login/style.css", func(response http.ResponseWriter, request *http.Request) {
		http.ServeFileFS(response, request, operatorlogin.Assets, "static/style.css")
	})
	router.With(principal.Middleware(provider)).Post("/logout", handler.logout)
	return router
}

func (h *Handler) login(response http.ResponseWriter, request *http.Request) {
	h.setSecurityHeaders(response)
	next := safeLocalRedirect(request.URL.Query().Get("next"))
	csrf := h.ensurePreAuthCookie(response, request, preAuthCSRFCookie)
	_ = h.ensurePreAuthCookie(response, request, preAuthFlowCookie)
	page := loginPage{CSRFToken: csrf, Next: next, Notice: loginNotice(request.URL.Query().Get("notice"))}
	if request.Method != http.MethodGet {
		response.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	h.render(response, page, http.StatusOK)
}

func (h *Handler) loginDispatch(response http.ResponseWriter, request *http.Request) {
	h.setSecurityHeaders(response)
	if request.Method == http.MethodGet {
		h.login(response, request)
		return
	}
	if request.Method != http.MethodPost {
		response.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.sameOrigin(request) {
		h.redirectLogin(response, safeLocalRedirect(request.URL.Query().Get("next")))
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxLoginBody)
	if request.ContentLength > maxLoginBody {
		response.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	if failure := request.ParseForm(); failure != nil {
		response.WriteHeader(http.StatusBadRequest)
		return
	}
	nextValue := request.PostForm.Get("next")
	if nextValue == "" {
		nextValue = request.URL.Query().Get("next")
	}
	next := safeLocalRedirect(nextValue)
	csrfCookie, csrfFailure := request.Cookie(preAuthCSRFCookie)
	csrfValue := ""
	if csrfFailure == nil && csrfCookie != nil && validRandomValue(csrfCookie.Value) {
		csrfValue = csrfCookie.Value
	}
	csrfForm := request.PostForm.Get(csrfFormField)
	if csrfForm == "" {
		// Keep the transport tolerant of older designer data while the canonical
		// login template uses the shorter field name.
		csrfForm = request.PostForm.Get("csrf_token")
	}
	if csrfFailure != nil || csrfValue == "" || csrfCookie == nil || csrfValue != csrfCookie.Value || len(csrfForm) != len(csrfValue) || subtle.ConstantTimeCompare([]byte(csrfForm), []byte(csrfValue)) != 1 {
		h.redirectLogin(response, next)
		return
	}
	username := request.PostForm.Get("username")
	if !h.rateLimiter.Allow(username, request.RemoteAddr) {
		h.render(response, loginPage{CSRFToken: csrfValue, Next: next, Username: username, Error: genericLoginError, ErrorFocus: true}, http.StatusOK)
		return
	}
	plaintext := request.PostForm.Get("password")
	session, failure := h.service.Login(request.Context(), username, plaintext)
	if failure != nil {
		h.render(response, loginPage{CSRFToken: csrfValue, Next: next, Username: username, Error: genericLoginError, ErrorFocus: true}, http.StatusOK)
		return
	}
	h.setSessionCookie(response, session.Token)
	h.expireCookie(response, preAuthCSRFCookie)
	h.expireCookie(response, preAuthFlowCookie)
	h.redirect(response, next)
}

func (h *Handler) logout(response http.ResponseWriter, request *http.Request) {
	h.setSecurityHeaders(response)
	rawToken, tokenOK := principal.SessionTokenFromContext(request.Context())
	if request.Method != http.MethodPost {
		response.WriteHeader(http.StatusForbidden)
		return
	}
	if !h.sameOrigin(request) {
		if tokenOK {
			h.redirectConsole(response)
			return
		}
		response.WriteHeader(http.StatusForbidden)
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxLoginBody)
	if failure := request.ParseForm(); failure != nil {
		response.WriteHeader(http.StatusBadRequest)
		return
	}
	csrf, csrfOK := operatorsessions.SessionCSRFToken(rawToken)
	csrfForm := request.PostForm.Get(csrfFormField)
	if csrfForm == "" {
		csrfForm = request.PostForm.Get("csrf_token")
	}
	if !tokenOK || !csrfOK || len(csrf) != len(csrfForm) || subtle.ConstantTimeCompare([]byte(csrf), []byte(csrfForm)) != 1 {
		if tokenOK {
			h.redirectConsole(response)
			return
		}
		response.WriteHeader(http.StatusForbidden)
		return
	}
	if failure := h.service.Revoke(request.Context(), rawToken); failure != nil {
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.expireCookie(response, h.cookie.Name)
	h.redirect(response, "/login")
}

func (h *Handler) render(response http.ResponseWriter, page loginPage, status int) {
	if page.CSRFToken == "" {
		// Error responses still need a fresh token for a retry.
		page.CSRFToken = randomValue(32)
	}
	var body bytes.Buffer
	if failure := h.template.Execute(&body, page); failure != nil {
		response.WriteHeader(http.StatusInternalServerError)
		return
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	response.WriteHeader(status)
	_, _ = response.Write(body.Bytes())
}

func (h *Handler) ensurePreAuthCookie(response http.ResponseWriter, request *http.Request, name string) string {
	if cookie, failure := request.Cookie(name); failure == nil && validRandomValue(cookie.Value) {
		return cookie.Value
	}
	value := randomValue(32)
	http.SetCookie(response, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.cookie.Secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   600,
	})
	return value
}

func (h *Handler) setSessionCookie(response http.ResponseWriter, value string) {
	http.SetCookie(response, &http.Cookie{
		Name:     h.cookie.Name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.cookie.Secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   7 * 24 * 60 * 60,
	})
}

func (h *Handler) expireCookie(response http.ResponseWriter, name string) {
	http.SetCookie(response, &http.Cookie{Name: name, Value: "", Path: "/", HttpOnly: true, Secure: h.cookie.Secure, SameSite: http.SameSiteLaxMode, MaxAge: -1})
}

func (h *Handler) redirect(response http.ResponseWriter, location string) {
	if location == "" {
		location = "/"
	}
	http.Redirect(response, &http.Request{URL: &url.URL{}}, location, http.StatusSeeOther)
}

func (h *Handler) redirectLogin(response http.ResponseWriter, next string) {
	// Rotate the pre-authentication material before sending the browser back to
	// a GET. The rejected POST must not be replayable with the same form state,
	// and the following GET will issue a fresh synchronizer value.
	h.expireCookie(response, preAuthCSRFCookie)
	h.expireCookie(response, preAuthFlowCookie)
	query := url.Values{"notice": {loginRefreshNotice}}
	if next = safeLocalRedirect(next); next != "/" {
		query.Set("next", next)
	}
	location := "/login?" + query.Encode()
	h.redirect(response, location)
}

func (h *Handler) redirectConsole(response http.ResponseWriter) {
	// This location is a fixed local route. In particular, no request query,
	// form value, CSRF token, or error text is copied into it.
	h.redirect(response, "/?notice=retry")
}

func (h *Handler) setSecurityHeaders(response http.ResponseWriter) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Pragma", "no-cache")
	response.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("X-Frame-Options", "DENY")
	response.Header().Set("Referrer-Policy", "no-referrer")
}

func (h *Handler) sameOrigin(request *http.Request) bool {
	seen := false
	for _, raw := range []string{request.Header.Get("Origin"), request.Header.Get("Referer")} {
		if raw == "" {
			continue
		}
		seen = true
		parsed, failure := url.Parse(raw)
		if failure != nil || parsed.User != nil || !strings.EqualFold(parsed.Scheme, h.publicOrigin.Scheme) || !strings.EqualFold(parsed.Host, h.publicOrigin.Host) {
			return false
		}
		if raw == request.Header.Get("Origin") && (parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "") {
			return false
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

func validateCookieConfig(cookie CookieConfig) error {
	if cookie.Name == "" {
		return errors.New("session cookie name is required")
	}
	hostCookie := strings.HasPrefix(cookie.Name, "__Host-")
	if cookie.Secure != hostCookie {
		return errors.New("Secure session cookies must use the __Host- policy")
	}
	if !cookie.Secure && !cookie.AllowInsecureLocal {
		return errors.New("insecure session cookies require explicit local development/testing configuration")
	}
	if cookie.AllowInsecureLocal && cookie.Secure {
		return errors.New("local insecure cookie mode cannot be combined with Secure cookies")
	}
	return nil
}

func safeLocalRedirect(value string) string {
	if value == "" {
		return "/"
	}
	if strings.ContainsAny(value, "\\\r\n") || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") {
		return "/"
	}
	parsed, failure := url.Parse(value)
	if failure != nil || parsed.IsAbs() || parsed.Host != "" || parsed.User != nil || parsed.Path == "" || !strings.HasPrefix(parsed.Path, "/") || strings.HasPrefix(parsed.Path, "//") {
		return "/"
	}
	return value
}

func randomValue(size int) string {
	raw := make([]byte, size)
	rand.Read(raw)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func validRandomValue(value string) bool {
	if len(value) != base64.RawURLEncoding.EncodedLen(32) {
		return false
	}
	_, failure := base64.RawURLEncoding.DecodeString(value)
	return failure == nil
}

const genericLoginError = "The username or password is incorrect."

func loginNotice(code string) string {
	if code == loginRefreshNotice {
		return code
	}
	return ""
}
