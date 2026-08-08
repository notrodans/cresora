// Package operatoraccounts provides the operator Telegram account sign-in page.
package operatoraccounts

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"errors"
	"html/template"
	"io"
	slogger "log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/operator-account-auth"
	common "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	disconnect "github.com/notrodans/cresora/internal/application/operatoraccounts"
	requests "github.com/notrodans/cresora/internal/application/requests/operator-account-auth"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	"github.com/notrodans/cresora/internal/infrastracture/logger/slog"
)

const (
	productionCSRFCookie    = "__Host-cresora_operator_csrf"
	productionSessionCookie = "__Host-cresora_operator_session"
	localCSRFCookie         = "cresora_operator_csrf"
	localSessionCookie      = "cresora_operator_session"
	// These aliases describe the secure-by-default route used by the existing
	// tests. Runtime handlers use the names from CookieConfig instead.
	csrfCookie        = productionCSRFCookie
	sessionCookie     = productionSessionCookie
	stateLifetime     = 10 * time.Minute
	disconnectTimeout = 10 * time.Second
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

const (
	canonicalOperationSendCode = "send_code"
	canonicalOperationSignIn   = "sign_in"
	canonicalOperationPassword = "password"
	canonicalOperationCancel   = "cancel"

	canonicalFailureApplication      = "application_failure"
	canonicalFailureInvalidResult    = "invalid_application_result"
	canonicalFailureRequestCancelled = "request_cancelled"

	disconnectOperation = "disconnect"
)

//go:embed templates/authenticate.html style.css authenticate.js
var assets embed.FS

type handler struct {
	start                          commands.Start
	codeCommand                    commands.Code
	password                       commands.Password
	cancel                         commands.Cancel
	status                         requests.Status
	disconnect                     disconnectCommand
	tmpl                           *template.Template
	publicOrigin                   *url.URL
	disabled                       bool
	disconnectDisabled             bool
	cookie                         CookieConfig
	allowLocalNullOriginNativeForm bool
}

type page struct {
	Accounts           []accountRow
	Phone              string
	CodeSent           bool
	ChallengeRequestID string
	ChallengeStage     string
	Delivery           string
	ChallengeExpires   int64
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
	ID                       string
	Name                     string
	Phone                    string
	State                    string
	CanDisconnect            bool
	DisconnectDisabledReason string
}

// disconnectCommand is the narrow application port consumed by the HTTP
// handler. It keeps the handler independent of the concrete service and of
// persistence or transport types.
type disconnectCommand interface {
	Execute(context.Context, application.Actor, operatoraccount.ID) (disconnect.DisconnectResult, error)
}

// NewWithPhoneAuth constructs the approved runtime phone-auth flow. It accepts
// only the canonical command ports; QR ports are deliberately not part of this
// composition and cannot be called by the live HTTP handler.
func NewWithPhoneAuth(start commands.Start, code commands.Code, password commands.Password, cancel commands.Cancel, status requests.Status, provider principal.Provider, publicOrigin string, options RouteOptions) chi.Router {
	r := chi.NewRouter()
	registerPhoneAuth(r, start, code, password, cancel, status, nil, provider, publicOrigin, options)
	return r
}

// NewWithPhoneAuthAndDisconnect constructs the live phone-auth router with the
// authenticated account disconnect command. A nil command is intentionally
// fail-closed; the route remains unavailable rather than becoming a partial
// live composition.
func NewWithPhoneAuthAndDisconnect(start commands.Start, code commands.Code, password commands.Password, cancel commands.Cancel, status requests.Status, disconnectCommand disconnectCommand, provider principal.Provider, publicOrigin string, options RouteOptions) chi.Router {
	r := chi.NewRouter()
	registerPhoneAuth(r, start, code, password, cancel, status, disconnectCommand, provider, publicOrigin, options)
	return r
}

func registerPhoneAuth(router chi.Router, start commands.Start, code commands.Code, password commands.Password, cancel commands.Cancel, status requests.Status, disconnectCommand disconnectCommand, provider principal.Provider, configuredOrigin string, options RouteOptions) {
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
	switch options.Mode {
	case RouteDisabled:
	case RouteLive:
	case RouteDevelopmentTestMock:
		if !options.AllowDevelopmentTestMock {
			panic("development/test mock route requires explicit opt-in")
		}
		if options.Environment != EnvironmentDevelopment && options.Environment != EnvironmentTesting {
			panic("development/test mock route requires DEVELOPMENT or TESTING environment")
		}
	default:
		panic("register operator account routes with unknown mode")
	}
	if options.Mode != RouteDisabled && (start == nil || code == nil || password == nil || cancel == nil || status == nil) {
		panic("register enabled operator account routes with missing command")
	}
	if options.Mode == RouteLive && (options.Environment == EnvironmentProduction || options.Environment == EnvironmentStaging) && !options.Cookie.Secure {
		panic("live operator account routes require Secure cookies in production and staging")
	}
	allowLocalNullOriginNativeForm := options.Mode == RouteLive &&
		options.Environment == EnvironmentDevelopment &&
		!options.Cookie.Secure &&
		options.Cookie.AllowInsecureLocal &&
		options.Cookie.CSRFCookieName == localCSRFCookie &&
		options.Cookie.SessionCookieName == localSessionCookie &&
		strings.EqualFold(origin.Scheme, "http") &&
		isLocalOriginHost(origin)
	h := &handler{
		start:                          start,
		codeCommand:                    code,
		password:                       password,
		cancel:                         cancel,
		status:                         status,
		disconnect:                     disconnectCommand,
		publicOrigin:                   origin,
		disabled:                       options.Mode == RouteDisabled,
		disconnectDisabled:             options.Mode != RouteLive || disconnectCommand == nil,
		cookie:                         options.Cookie,
		allowLocalNullOriginNativeForm: allowLocalNullOriginNativeForm,
	}
	h.tmpl = template.Must(template.New("authenticate.html").ParseFS(assets, "templates/authenticate.html"))
	protected := router.With(principal.Middleware(provider))
	protected.Get("/operator-accounts/authenticate", h.authenticate)
	protected.Post("/operator-accounts/authenticate/phone", h.phoneCanonical)
	protected.Post("/operator-accounts/authenticate/phone/code", h.codeCanonical)
	protected.Post("/operator-accounts/authenticate/phone/password", h.passwordStep)
	protected.Post("/operator-accounts/authenticate/phone/cancel", h.cancelStep)
	registerDisconnectRoutes(protected, h)
	router.Get("/operator-accounts/authenticate/style.css", func(w http.ResponseWriter, r *http.Request) { http.ServeFileFS(w, r, assets, "style.css") })
	router.Get("/operator-accounts/authenticate/authenticate.js", func(w http.ResponseWriter, r *http.Request) { http.ServeFileFS(w, r, assets, "authenticate.js") })
}

func registerDisconnectRoutes(router chi.Router, h *handler) {
	router.Post("/operator-accounts/disconnect", h.disconnectAccount)
	router.Post("/operator-accounts/{accountID}/disconnect", h.disconnectAccount)
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
	p := page{CSRF: csrf, Notice: notice(r.URL.Query().Get("notice")), Error: errorMessage(r.URL.Query().Get("error"))}
	p.ErrorFocus = p.Error != ""

	if h.status != nil {
		current, err := h.status.Status(r.Context(), scope.actor)
		if err != nil {
			p.Error = "Accounts are temporarily unavailable."
			p.ErrorFocus = true
		} else {
			p.Accounts = mapAccounts(current.Accounts)
			if current.Challenge != nil && time.Now().Before(current.Challenge.ExpiresAt) {
				p.Phone = current.Challenge.Phone
				p.ChallengeRequestID = current.Challenge.RequestID.String()
				p.ChallengeStage = string(current.Challenge.Stage)
				p.Delivery = current.Challenge.Delivery
				p.ChallengeExpires = current.Challenge.ExpiresAt.UnixMilli()
				p.CodeSent = current.Challenge.Stage == common.StageCode
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

func (h *handler) phoneCanonical(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
	actor, ok := requestActor(w, r)
	if !ok || !h.protectPost(w, r) {
		return
	}
	phone := strings.TrimSpace(r.FormValue("phone"))
	if phone == "" || len(phone) > 32 {
		logCanonicalFailure(r, canonicalOperationSendCode, common.ErrInvalidInput)
		redirectError(w, "phone")
		return
	}
	result, err := h.start.Start(r.Context(), actor, phone)
	if err != nil {
		logCanonicalFailure(r, canonicalOperationSendCode, err)
		if isFloodWait(err) {
			redirectError(w, "flood-wait")
			return
		}
		redirectError(w, "send-code")
		return
	}
	if result.Validate() != nil {
		logCanonicalFailure(r, canonicalOperationSendCode, common.ErrInvalidResult)
		redirectError(w, "send-code")
		return
	}
	if result.Account != nil {
		h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=code-sent")
}

func (h *handler) codeCanonical(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
	actor, ok := requestActor(w, r)
	if !ok || !h.protectPost(w, r) {
		return
	}
	requestID, err := uuid.Parse(strings.TrimSpace(r.FormValue("challenge_request_id")))
	code := strings.TrimSpace(r.FormValue("code"))
	if err != nil || requestID == uuid.Nil || code == "" || len(code) > 16 {
		logCanonicalFailure(r, canonicalOperationSignIn, common.ErrInvalidInput)
		redirectError(w, "code")
		return
	}
	result, err := h.codeCommand.Code(r.Context(), actor, requestID, code)
	if err != nil {
		if errors.Is(err, common.ErrPasswordRequired) {
			h.redirect(w, "/operator-accounts/authenticate?notice=password-required")
			return
		}
		logCanonicalFailure(r, canonicalOperationSignIn, err)
		if isFloodWait(err) {
			redirectError(w, "flood-wait")
			return
		}
		redirectError(w, "code")
		return
	}
	if result.Validate() != nil {
		logCanonicalFailure(r, canonicalOperationSignIn, common.ErrInvalidResult)
		redirectError(w, "code")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
}

func (h *handler) passwordStep(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
	actor, ok := requestActor(w, r)
	if !ok || !h.protectPost(w, r) {
		return
	}
	requestID, err := uuid.Parse(strings.TrimSpace(r.FormValue("challenge_request_id")))
	password := r.FormValue("password")
	if err != nil || requestID == uuid.Nil || password == "" || len(password) > 256 {
		logCanonicalFailure(r, canonicalOperationPassword, common.ErrInvalidInput)
		redirectError(w, "password")
		return
	}
	result, err := h.password.Password(r.Context(), actor, requestID, password)
	if err != nil {
		logCanonicalFailure(r, canonicalOperationPassword, err)
		if isFloodWait(err) {
			redirectError(w, "flood-wait")
			return
		}
		redirectError(w, "password")
		return
	}
	if result.Validate() != nil {
		logCanonicalFailure(r, canonicalOperationPassword, common.ErrInvalidResult)
		redirectError(w, "password")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=account-added")
}

func (h *handler) cancelStep(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.unavailable(w) {
		return
	}
	actor, ok := requestActor(w, r)
	if !ok || !h.protectPost(w, r) {
		return
	}
	requestID, err := uuid.Parse(strings.TrimSpace(r.FormValue("challenge_request_id")))
	if err != nil || requestID == uuid.Nil {
		logCanonicalFailure(r, canonicalOperationCancel, common.ErrInvalidInput)
		redirectError(w, "invalid")
		return
	}
	if err := h.cancel.Cancel(r.Context(), actor, requestID); err != nil {
		logCanonicalFailure(r, canonicalOperationCancel, err)
		redirectError(w, "cancel")
		return
	}
	h.redirect(w, "/operator-accounts/authenticate?notice=cancelled")
}

func (h *handler) disconnectAccount(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	if h.disconnectUnavailable(w) {
		return
	}
	actor, ok := requestActor(w, r)
	if !ok || !h.protectPost(w, r) {
		return
	}

	accountIDValue := strings.TrimSpace(chi.URLParam(r, "accountID"))
	if accountIDValue == "" {
		accountIDValue = strings.TrimSpace(r.FormValue("account_id"))
	}
	accountID, err := uuid.Parse(accountIDValue)
	if err != nil || accountID == uuid.Nil {
		logDisconnectFailure(r, disconnect.ErrInvalidInput)
		redirectError(w, "disconnect")
		return
	}

	disconnectContext, cancel := context.WithTimeout(r.Context(), disconnectTimeout)
	defer cancel()
	result, err := h.disconnect.Execute(disconnectContext, actor, operatoraccount.Identity(accountID))
	if err != nil {
		logDisconnectFailure(r, err)
		if result.Outcome == disconnect.DisconnectPending || errors.Is(err, disconnect.ErrRemoteLogoutNotConverged) {
			redirectError(w, "disconnect-pending")
			return
		}
		redirectError(w, "disconnect")
		return
	}

	switch result.Outcome {
	case disconnect.DisconnectCompleted:
		h.redirect(w, "/operator-accounts/authenticate?notice=account-disconnected")
	case disconnect.DisconnectAlreadyDisconnected:
		h.redirect(w, "/operator-accounts/authenticate?notice=account-already-disconnected")
	default:
		logDisconnectFailure(r, disconnect.ErrInvalidRemoteLogoutFailure)
		redirectError(w, "disconnect")
	}
}

func (h *handler) resolveScope(w http.ResponseWriter, r *http.Request) (requestScope, bool) {
	actor, ok := requestActor(w, r)
	if !ok {
		return requestScope{}, false
	}
	return h.scopeForActor(w, r, actor), true
}

func (h *handler) unavailable(w http.ResponseWriter) bool {
	if !h.disabled {
		return false
	}
	h.renderUnavailable(w)
	return true
}

func (h *handler) disconnectUnavailable(w http.ResponseWriter) bool {
	if !h.disconnectDisabled {
		return false
	}
	h.renderUnavailable(w)
	return true
}

func (h *handler) renderUnavailable(w http.ResponseWriter) {
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
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(body.Bytes())
}

func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
}

func (h *handler) scopeForActor(w http.ResponseWriter, r *http.Request, actor application.Actor) requestScope {
	return requestScope{actor: actor, flowID: h.flowID(w, r)}
}

func requestActor(w http.ResponseWriter, r *http.Request) (application.Actor, bool) {
	actor, ok := principal.FromContext(r.Context())
	if !ok {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
	return actor, ok
}

func (h *handler) protectPost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return false
	}
	if !h.sameOrigin(r) && !h.localNullOriginNativeForm(r) {
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

func logCanonicalFailure(r *http.Request, operation string, failure error) {
	failureClass := canonicalFailureApplication
	if errors.Is(failure, common.ErrInvalidResult) {
		failureClass = canonicalFailureInvalidResult
	} else {
		var providerFailure *common.ProviderFailureError
		if errors.As(failure, &providerFailure) && providerFailure != nil {
			if kind := providerFailure.Kind(); kind != common.ProviderFailureUnknown {
				failureClass = string(kind)
			}
		}
	}

	level := slogger.LevelError
	if errors.Is(failure, context.Canceled) {
		if failureClass == canonicalFailureApplication {
			failureClass = canonicalFailureRequestCancelled
		}
		level = slogger.LevelInfo
	}
	sloggerForRequest := slog.LoggerOr(r.Context(), slogger.Default())
	sloggerForRequest.LogAttrs(
		r.Context(),
		level,
		"operator account authentication failed",
		slogger.String("operation", operation),
		slogger.String("failure_class", failureClass),
	)
}

func logDisconnectFailure(r *http.Request, failure error) {
	failureClass := canonicalFailureApplication
	switch {
	case errors.Is(failure, disconnect.ErrInvalidInput):
		failureClass = "invalid_input"
	case errors.Is(failure, disconnect.ErrAccountNotFound):
		failureClass = "account_not_found"
	case errors.Is(failure, disconnect.ErrAccountStateConflict):
		failureClass = "state_conflict"
	case errors.Is(failure, disconnect.ErrAccountVersionConflict):
		failureClass = "version_conflict"
	case errors.Is(failure, disconnect.ErrSessionNotFound):
		failureClass = "session_not_found"
	case errors.Is(failure, disconnect.ErrRemoteLogoutFloodWait):
		failureClass = string(disconnect.RemoteLogoutFailureFloodWait)
	case errors.Is(failure, disconnect.ErrRemoteLogoutTransient):
		failureClass = string(disconnect.RemoteLogoutFailureTransient)
	case errors.Is(failure, disconnect.ErrRemoteLogoutAmbiguous):
		failureClass = string(disconnect.RemoteLogoutFailureAmbiguous)
	case errors.Is(failure, disconnect.ErrRemoteLogoutPermanent):
		failureClass = string(disconnect.RemoteLogoutFailurePermanent)
	case errors.Is(failure, disconnect.ErrRuntimeUnavailable):
		failureClass = string(disconnect.RemoteLogoutFailureUnavailable)
	}

	level := slogger.LevelError
	if errors.Is(failure, context.Canceled) {
		failureClass = canonicalFailureRequestCancelled
		level = slogger.LevelInfo
	}
	sloggerForRequest := slog.LoggerOr(r.Context(), slogger.Default())
	sloggerForRequest.LogAttrs(
		r.Context(),
		level,
		"operator account disconnect failed",
		slogger.String("operation", disconnectOperation),
		slogger.String("failure_class", failureClass),
	)
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

func (h *handler) localNullOriginNativeForm(r *http.Request) bool {
	if !h.allowLocalNullOriginNativeForm || r == nil || r.URL == nil || h.publicOrigin == nil {
		return false
	}
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return false
	}
	switch r.URL.EscapedPath() {
	case "/operator-accounts/authenticate/phone",
		"/operator-accounts/authenticate/phone/code",
		"/operator-accounts/authenticate/phone/password",
		"/operator-accounts/authenticate/phone/cancel":
	case "/operator-accounts/disconnect":
		return false
	default:
		const (
			prefix = "/operator-accounts/"
			suffix = "/disconnect"
		)
		path := r.URL.EscapedPath()
		if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
			return false
		}
		rawAccountID := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
		accountID, failure := uuid.Parse(rawAccountID)
		if failure != nil || accountID == uuid.Nil {
			return false
		}
	}
	if r.Host != h.publicOrigin.Host {
		return false
	}
	if values := r.Header.Values("Origin"); len(values) != 1 || values[0] != "null" {
		return false
	}
	if len(r.Header.Values("Referer")) != 0 {
		return false
	}
	if values := r.Header.Values("Sec-Fetch-Site"); len(values) != 1 || values[0] != "same-origin" {
		return false
	}
	if values := r.Header.Values("Sec-Fetch-Mode"); len(values) != 1 || values[0] != "navigate" {
		return false
	}
	if values := r.Header.Values("Sec-Fetch-Dest"); len(values) != 1 || values[0] != "document" {
		return false
	}
	return true
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
	io.ReadFull(rand.Reader, b)
	return base64.RawURLEncoding.EncodeToString(b)
}
func notice(code string) string {
	switch code {
	case "code-sent":
		return "A Telegram code was sent."
	case "account-added":
		return "Telegram account connected."
	case "password-required":
		return "Authentication password required."
	case "cancelled":
		return "Challenge cancelled."
	case "account-disconnected":
		return "Telegram account disconnected."
	case "account-already-disconnected":
		return "Telegram account is already disconnected."
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
	case "password":
		return "The password was not accepted."
	case "cancel":
		return "The sign-in attempt could not be cancelled."
	case "flood-wait":
		return "Telegram временно ограничил попытки входа. Подождите немного и повторите попытку."
	case "invalid":
		return "Please check the form and try again."
	case "disconnect":
		return "We could not disconnect that account. Try again."
	case "disconnect-pending":
		return "The account could not be disconnected yet. Try again later."
	}
	return ""
}

func isFloodWait(err error) bool {
	var retryAfter *common.RetryAfterError
	return errors.As(err, &retryAfter)
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
		canDisconnect, disabledReason := disconnectAvailability(account.Status)
		rows = append(rows, accountRow{
			ID:                       account.ID.String(),
			Name:                     name,
			Phone:                    account.Phone,
			State:                    accountState(account.Status),
			CanDisconnect:            canDisconnect,
			DisconnectDisabledReason: disabledReason,
		})
	}
	return rows
}

func disconnectAvailability(status operatoraccount.Status) (bool, string) {
	switch status {
	case operatoraccount.StatusActive, operatoraccount.StatusReauthRequired, operatoraccount.StatusDisconnecting:
		return true, ""
	case operatoraccount.StatusAuthenticating:
		return false, "authentication_in_progress"
	case operatoraccount.StatusDisconnected:
		return false, "already_disconnected"
	default:
		return false, "unavailable"
	}
}

func accountState(status operatoraccount.Status) string {
	switch status {
	case operatoraccount.StatusActive:
		return "active"
	case operatoraccount.StatusAuthenticating:
		return "authenticating"
	case operatoraccount.StatusReauthRequired:
		return "reauth_required"
	case operatoraccount.StatusDisconnecting:
		return "disconnecting"
	case operatoraccount.StatusDisconnected:
		return "disconnected"
	default:
		return string(status)
	}
}

var _ common.Account
