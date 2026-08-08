// Package console contains the small, operator-scoped HTML mailing console.
package console

import (
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"errors"
	"fmt"
	"html/template"
	slogger "log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	applicationactor "github.com/notrodans/cresora/internal/application"
	commands "github.com/notrodans/cresora/internal/application/commands/mailing-console"
	operatorsessions "github.com/notrodans/cresora/internal/application/operatorsessions"
	requests "github.com/notrodans/cresora/internal/application/requests/mailing-console"
	application "github.com/notrodans/cresora/internal/application/services/mailingconsole"
	"github.com/notrodans/cresora/internal/entrypoint/http/principal"
	"github.com/notrodans/cresora/internal/infrastracture/logger/slog"
)

const maxBody = 64 << 10
const csrfCookie = "cresora_console_csrf"

//go:embed templates/index.html style.css console.js
var assets embed.FS

type Handler struct {
	createDraft  commands.CreateDraft
	queue        commands.Queue
	dashboardReq requests.Dashboard
	logger       *slogger.Logger
	tmpl         *template.Template
	publicOrigin *url.URL
}

// Register adds the mailing console routes to an existing chi router.
func Register(router chi.Router, createDraft commands.CreateDraft, queue commands.Queue, dashboard requests.Dashboard, provider principal.Provider, publicOrigin string, logger *slogger.Logger) {
	origin, failure := parsePublicOrigin(publicOrigin)
	if failure != nil {
		panic(fmt.Sprintf("create mailing console handler with invalid public origin: %v", failure))
	}
	tmpl := template.Must(template.New("index.html").Funcs(template.FuncMap{
		"contains":    contains,
		"statusLabel": statusLabel,
	}).ParseFS(assets, "templates/index.html"))
	handler := &Handler{
		createDraft:  createDraft,
		queue:        queue,
		dashboardReq: dashboard,
		logger:       logger,
		tmpl:         tmpl,
		publicOrigin: origin,
	}
	protected := router.With(principal.Middleware(provider))
	protected.Get("/", func(w http.ResponseWriter, r *http.Request) {
		handler.dashboard(w, r, safeNotice(r.URL.Query().Get("notice")))
	})
	protected.Post("/mailings", handler.create)
	protected.Post("/mailings/{mailingID}/queue", handler.queueMailing)
	router.Get("/style.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "style.css")
	})
	router.Get("/console.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "console.js")
	})
}

type pageData struct {
	Dashboard  application.Dashboard
	Form       formData
	Notice     string
	Error      string
	ErrorFocus bool
}
type formData struct {
	Name        string
	Message     string
	AccountID   string
	Shared      []string
	Private     []string
	FieldErrors map[string]string
}

func (h *Handler) dashboard(w http.ResponseWriter, r *http.Request, notice string) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	token := h.csrfToken(w, r)
	dashboard, err := h.dashboardReq.Execute(r.Context(), actor)
	if err != nil {
		http.Error(w, "Не удалось загрузить данные консоли.", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.Execute(w, struct {
		pageData
		CSRF string
	}{pageData: pageData{Dashboard: dashboard, Notice: notice}, CSRF: token}); err != nil {
		return
	}
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	values, accepted := h.parsePost(w, r)
	if !accepted {
		return
	}

	form := formData{
		Name:        values.Get("name"),
		Message:     values.Get("message"),
		AccountID:   values.Get("account_id"),
		Shared:      values["shared_dialog_id"],
		Private:     values["private_target"],
		FieldErrors: make(map[string]string),
	}

	logger := slog.LoggerOr(r.Context(), h.logger).With(
		slogger.String("operation", "mailing.create_draft"),
		slogger.String("account_id", form.AccountID),
	)

	logger.DebugContext(
		r.Context(),
		"draft form received",
		slogger.Int("name_length", len(form.Name)),
		slogger.Int("message_length", len(form.Message)),
		slogger.Int("shared_dialog_count", len(form.Shared)),
		slogger.Int("private_target_count", len(form.Private)),
	)

	input, err := inputFromForm(form)
	if err != nil {
		field, message := formError(err)
		form.FieldErrors[field] = message

		logger.WarnContext(
			r.Context(),
			"draft form rejected",
			slogger.String("field", field),
			slogger.Any("error", err),
		)

		h.renderForm(
			w,
			r,
			form,
			"Проверьте выделенные поля.",
			http.StatusBadRequest,
		)
		return
	}

	draft, err := h.createDraft.Execute(r.Context(), actor, input)
	if err != nil {
		logger.ErrorContext(
			r.Context(),
			"draft creation failed",
			slogger.Any("error", err),
		)

		h.renderServiceError(w, r, form, err)
		return
	}

	logger.InfoContext(
		r.Context(),
		"draft created",
		slogger.Any("draft_id", draft.UUID().ID()),
	)

	h.redirect(w, r, "/?notice=draft-created")
}

func (h *Handler) queueMailing(w http.ResponseWriter, r *http.Request) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	if _, accepted := h.parsePost(w, r); !accepted {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "mailingID"))
	if err != nil {
		http.Error(w, "Не удалось загрузить данные консоли.", http.StatusBadRequest)
		return
	}
	if err = h.queue.Execute(r.Context(), actor, id); err != nil {
		status, message := serviceError(err)
		http.Error(w, message, status)
		return
	}
	h.redirect(w, r, "/?notice=queued")
}

func (h *Handler) renderForm(w http.ResponseWriter, r *http.Request, f formData, message string, status int) {
	actor, ok := requestActor(w, r)
	if !ok {
		return
	}
	d, err := h.dashboardReq.Execute(r.Context(), actor)
	if err != nil {
		http.Error(w, "Не удалось загрузить данные консоли.", http.StatusInternalServerError)
		return
	}
	token := h.csrfToken(w, r)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = h.tmpl.Execute(w, struct {
		pageData
		CSRF string
	}{pageData: pageData{Dashboard: d, Form: f, Error: message, ErrorFocus: message != ""}, CSRF: token})
}

func requestActor(w http.ResponseWriter, r *http.Request) (applicationactor.Actor, bool) {
	actor, ok := principal.FromContext(r.Context())
	if !ok {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
	return actor, ok
}

func (h *Handler) renderServiceError(w http.ResponseWriter, r *http.Request, f formData, err error) {
	status, message := serviceError(err)
	if status == http.StatusBadRequest {
		h.renderForm(w, r, f, message, status)
		return
	}
	http.Error(w, message, status)
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request, location string) {
	http.Redirect(w, r, location, http.StatusSeeOther)
}

func inputFromForm(f formData) (application.CreateDraftInput, error) {
	var out application.CreateDraftInput
	out.Name, out.MessageText = f.Name, f.Message
	var err error
	out.AccountID, err = uuid.Parse(f.AccountID)
	if err != nil {
		return out, fmt.Errorf("account")
	}
	for _, raw := range f.Shared {
		id, e := uuid.Parse(raw)
		if e != nil {
			return out, fmt.Errorf("recipient")
		}
		out.SharedDialogIDs = append(out.SharedDialogIDs, id)
	}
	for _, raw := range f.Private {
		parts := strings.SplitN(raw, ":", 2)
		if len(parts) != 2 {
			return out, fmt.Errorf("recipient")
		}
		peer, e := strconv.ParseInt(parts[1], 10, 64)
		if e != nil {
			return out, fmt.Errorf("recipient")
		}
		out.PrivateTargets = append(out.PrivateTargets, application.PrivateTarget{
			PeerType: application.PeerType(parts[0]),
			PeerID:   peer,
		})
	}
	if strings.TrimSpace(out.Name) == "" {
		return out, fmt.Errorf("name")
	}
	if strings.TrimSpace(out.MessageText) == "" {
		return out, fmt.Errorf("message")
	}
	if len(out.SharedDialogIDs)+len(out.PrivateTargets) == 0 {
		return out, fmt.Errorf("recipient")
	}
	return out, nil
}

func formError(err error) (string, string) {
	switch err.Error() {
	case "name":
		return "name", "Укажите название рассылки"
	case "message":
		return "message", "Введите текст сообщения"
	case "account":
		return "account", "Выберите аккаунт отправителя"
	default:
		return "recipients", "Выберите хотя бы одного получателя"
	}
}

func serviceError(err error) (int, string) {
	switch {
	case errors.Is(err, application.ErrInvalidInput):
		return http.StatusBadRequest, "Проверьте данные рассылки."
	case errors.Is(err, application.ErrNoEligibleRecipients):
		return http.StatusConflict, "Рассылка не имеет доступных получателей."
	case errors.Is(err, application.ErrNotFound):
		return http.StatusNotFound, "Рассылка не найдена."
	case errors.Is(err, application.ErrInvalidState):
		return http.StatusConflict, "Рассылка уже не может быть изменена."
	default:
		return http.StatusInternalServerError, "Не удалось выполнить действие. Попробуйте ещё раз."
	}
}

func (h *Handler) parsePost(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается.", http.StatusMethodNotAllowed)
		return nil, false
	}
	if !h.sameOrigin(r) && !h.localNullOriginNativeForm(r) {
		if h.redirectRecoverableSession(w, r) {
			return nil, false
		}
		http.Error(w, "Запрос отклонён.", http.StatusForbidden)
		return nil, false
	}
	cookie, failure := r.Cookie(csrfCookie)
	rawSessionToken, sessionBound := principal.SessionTokenFromContext(r.Context())
	if failure != nil && !sessionBound {
		http.Error(w, "Запрос отклонён.", http.StatusForbidden)
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	if r.ContentLength > maxBody {
		http.Error(w, "Запрос слишком большой.", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	if failure = r.ParseForm(); failure != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](failure); ok {
			http.Error(w, "Запрос слишком большой.", http.StatusRequestEntityTooLarge)
			return nil, false
		}
		http.Error(w, "Не удалось прочитать форму.", http.StatusBadRequest)
		return nil, false
	}
	token := r.PostForm.Get("csrf_token")
	expected := ""
	if sessionBound {
		expected, sessionBound = operatorsessions.SessionCSRFToken(rawSessionToken)
	}
	if !sessionBound {
		expected = cookie.Value
	}
	if len(token) != len(expected) || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		if h.redirectRecoverableSession(w, r) {
			return nil, false
		}
		http.Error(w, "Запрос отклонён", http.StatusForbidden)
		return nil, false
	}
	return r.PostForm, true
}

func (h *Handler) localNullOriginNativeForm(r *http.Request) bool {
	if r == nil || r.URL == nil || h.publicOrigin == nil {
		return false
	}
	if !strings.EqualFold(h.publicOrigin.Scheme, "http") || !isLoopbackHost(h.publicOrigin.Hostname()) {
		return false
	}
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return false
	}
	switch path := r.URL.Path; {
	case path == "/mailings":
	case strings.HasPrefix(path, "/mailings/") && strings.HasSuffix(path, "/queue"):
		rawID := strings.TrimSuffix(strings.TrimPrefix(path, "/mailings/"), "/queue")
		if rawID == "" || strings.Contains(rawID, "/") {
			return false
		}
		if _, failure := uuid.Parse(rawID); failure != nil {
			return false
		}
	default:
		return false
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

func (h *Handler) redirectRecoverableSession(w http.ResponseWriter, r *http.Request) bool {
	if _, ok := principal.SessionTokenFromContext(r.Context()); !ok {
		return false
	}
	// The target and notice are fixed server-owned values. Never reflect the
	// rejected request's query, form fields, or validation reason here.
	h.redirect(w, r, "/?notice=retry")
	return true
}

func safeNotice(value string) string {
	switch value {
	case "draft-created", "queued", "retry":
		return value
	default:
		return ""
	}
}

func contains(values []string, value string) bool {
	return slices.Contains(values, value)
}

func statusLabel(status string) string {
	switch strings.ToLower(status) {
	case "draft":
		return "Черновик"
	case "queued":
		return "В очереди"
	case "running", "sending", "in_progress":
		return "Отправляется"
	case "paused":
		return "Приостановлена"
	case "stopped":
		return "Остановлена"
	case "completed", "done", "sent":
		return "Завершена"
	case "failed", "error":
		return "Есть ошибки"
	default:
		return status
	}
}

func (h *Handler) sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	referer := r.Header.Get("Referer")
	if origin == "" && referer == "" {
		return false
	}
	if origin != "" && !h.matchesOrigin(origin, false) {
		return false
	}
	if referer != "" && !h.matchesOrigin(referer, true) {
		return false
	}
	return true
}

// Проверяем, чтобы Origin был равен заданному PUBLIC_ORIGIN из ENV
func (h *Handler) matchesOrigin(raw string, referer bool) bool {
	parsed, failure := url.Parse(raw)
	if failure != nil || parsed.User != nil || !strings.EqualFold(parsed.Scheme, h.publicOrigin.Scheme) || !strings.EqualFold(parsed.Host, h.publicOrigin.Host) {
		return false
	}
	if !referer && (parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "") {
		return false
	}
	return true
}

func isLoopbackHost(hostname string) bool {
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	address := net.ParseIP(hostname)
	return address != nil && address.IsLoopback()
}

func (h *Handler) csrfToken(w http.ResponseWriter, r *http.Request) string {
	if rawSessionToken, ok := principal.SessionTokenFromContext(r.Context()); ok {
		if token, valid := operatorsessions.SessionCSRFToken(rawSessionToken); valid {
			return token
		}
	}
	if c, err := r.Cookie(csrfCookie); err == nil && c.Value != "" {
		return c.Value
	}
	// Allocate a 32-byte buffer for a 256-bit CSRF token.
	b := make([]byte, 32)
	// crypto/rand.Read uses the operating system's cryptographically secure
	// source and fills the complete buffer.
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	token := fmt.Sprintf("%x", b)
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.publicOrigin.Scheme == "https",
	})
	return token
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
