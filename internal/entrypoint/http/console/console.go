// Package console contains the small, operator-scoped HTML mailing console.
package console

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	slogger "log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	commands "github.com/notrodans/nebula-go/internal/application/commands/mailing-console"
	requests "github.com/notrodans/nebula-go/internal/application/requests/mailing-console"
	application "github.com/notrodans/nebula-go/internal/application/services/mailingconsole"
	"github.com/notrodans/nebula-go/internal/domain/mailing"
	"github.com/notrodans/nebula-go/internal/infrastracture/logger/slog"
)

const maxBody = 64 << 10
const csrfCookie = "nebula_console_csrf"

//go:embed templates/index.html style.css console.js
var assets embed.FS

type Handler struct {
	createDraft  commands.CreateDraft
	queue        commands.Queue
	dashboardReq requests.Dashboard
	tmpl         *template.Template
	publicOrigin *url.URL
}

// New returns an independent chi router for the operator-scoped mailing
// console. Its arguments are a CreateDraft command, Queue command, Dashboard
// request, public origin, and optional logger.
//
// The two-argument service form is retained for callers of the pre-CQS
// constructor. It only adapts the service to the three ports; Handler itself
// depends exclusively on those ports.
func New(first any, arguments ...any) chi.Router {
	createDraft, queue, dashboard, publicOrigin := newDependencies(first, arguments...)
	router := chi.NewRouter()
	Register(router, createDraft, queue, dashboard, publicOrigin)
	return router
}

// Register adds the mailing console routes to an existing chi router.
func Register(router chi.Router, createDraft commands.CreateDraft, queue commands.Queue, dashboard requests.Dashboard, publicOrigin string) {
	if router == nil {
		panic("register mailing console routes on nil router")
	}
	origin, failure := parsePublicOrigin(publicOrigin)
	if failure != nil {
		panic(fmt.Sprintf("create mailing console handler with invalid public origin: %v", failure))
	}
	tmpl := template.Must(template.New("index.html").Funcs(template.FuncMap{
		"contains":    contains,
		"statusLabel": statusLabel,
	}).ParseFS(assets, "templates/index.html"))
	handler := &Handler{createDraft: createDraft, queue: queue, dashboardReq: dashboard, tmpl: tmpl, publicOrigin: origin}
	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		handler.dashboard(w, r, r.URL.Query().Get("notice"))
	})
	router.Post("/mailings", handler.create)
	router.Post("/mailings/{mailingID}/queue", handler.queueMailing)
	router.Get("/style.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "style.css")
	})
	router.Get("/console.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, assets, "console.js")
	})
}

func newDependencies(first any, arguments ...any) (commands.CreateDraft, commands.Queue, requests.Dashboard, string) {
	if len(arguments) == 1 || len(arguments) == 2 {
		publicOrigin, ok := arguments[0].(string)
		if !ok {
			panic("create mailing console handler without public origin")
		}
		legacyCreateDraft, createOK := first.(legacyCreateDraftOperation)
		legacyQueue, queueOK := first.(legacyQueueOperation)
		legacyDashboard, dashboardOK := first.(legacyDashboardOperation)
		if !createOK || !queueOK || !dashboardOK {
			panic("create mailing console handler without CQS ports")
		}
		return legacyCreateDraftCommand{operation: legacyCreateDraft}, legacyQueueCommand{operation: legacyQueue}, legacyDashboardRequest{operation: legacyDashboard}, publicOrigin
	}
	if len(arguments) != 3 && len(arguments) != 4 {
		panic("create mailing console handler with invalid arguments")
	}
	queue, ok := arguments[0].(commands.Queue)
	if !ok {
		panic("create mailing console handler without queue command")
	}
	dashboard, ok := arguments[1].(requests.Dashboard)
	if !ok {
		panic("create mailing console handler without dashboard request")
	}
	publicOrigin, ok := arguments[2].(string)
	if !ok {
		panic("create mailing console handler without public origin")
	}
	createDraft, ok := first.(commands.CreateDraft)
	if !ok {
		panic("create mailing console handler without create draft command")
	}
	return createDraft, queue, dashboard, publicOrigin
}

// These adapters keep the old New(service, origin[, logger]) form source
// compatible while new composition roots use Register with explicit CQS
// ports. They are intentionally outside Handler, which only knows the ports.
type legacyCreateDraftOperation interface {
	CreateDraft(context.Context, application.CreateDraftInput) (mailing.ID, error)
}

type legacyQueueOperation interface {
	Queue(context.Context, uuid.UUID) error
}

type legacyDashboardOperation interface {
	Dashboard(context.Context) (application.Dashboard, error)
}

type legacyCreateDraftCommand struct {
	operation legacyCreateDraftOperation
}

func (command legacyCreateDraftCommand) Execute(context context.Context, input application.CreateDraftInput) (mailing.ID, error) {
	return command.operation.CreateDraft(context, input)
}

type legacyQueueCommand struct {
	operation legacyQueueOperation
}

func (command legacyQueueCommand) Execute(context context.Context, mailingID uuid.UUID) error {
	return command.operation.Queue(context, mailingID)
}

type legacyDashboardRequest struct {
	operation legacyDashboardOperation
}

func (request legacyDashboardRequest) Execute(context context.Context) (application.Dashboard, error) {
	return request.operation.Dashboard(context)
}

type pageData struct {
	Dashboard application.Dashboard
	Form      formData
	Notice    string
	Error     string
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
	token := h.csrfToken(w, r)
	dashboard, err := h.dashboardReq.Execute(r.Context())
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

	logger := slog.LoggerFrom(r.Context()).With(
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

	draft, err := h.createDraft.Execute(r.Context(), input)
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
	if _, accepted := h.parsePost(w, r); !accepted {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "mailingID"))
	if err != nil {
		http.Error(w, "Не удалось загрузить данные консоли.", http.StatusBadRequest)
		return
	}
	if err = h.queue.Execute(r.Context(), id); err != nil {
		status, message := serviceError(err)
		http.Error(w, message, status)
		return
	}
	h.redirect(w, r, "/?notice=queued")
}

func (h *Handler) renderForm(w http.ResponseWriter, r *http.Request, f formData, message string, status int) {
	d, err := h.dashboardReq.Execute(r.Context())
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
	}{pageData: pageData{Dashboard: d, Form: f, Error: message}, CSRF: token})
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
	if !h.sameOrigin(r) {
		http.Error(w, "Запрос отклонён.", http.StatusForbidden)
		return nil, false
	}
	cookie, failure := r.Cookie(csrfCookie)
	if failure != nil {
		http.Error(w, "Запрос отклонён.", http.StatusForbidden)
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	if r.ContentLength > maxBody {
		http.Error(w, "Запрос слишком большой.", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	if failure = r.ParseForm(); failure != nil {
		var maxError *http.MaxBytesError
		if errors.As(failure, &maxError) {
			http.Error(w, "Запрос слишком большой.", http.StatusRequestEntityTooLarge)
			return nil, false
		}
		http.Error(w, "Не удалось прочитать форму.", http.StatusBadRequest)
		return nil, false
	}
	token := r.PostForm.Get("csrf_token")
	if len(token) != len(cookie.Value) || subtle.ConstantTimeCompare([]byte(token), []byte(cookie.Value)) != 1 {
		http.Error(w, "Запрос отклонён", http.StatusForbidden)
		return nil, false
	}
	return r.PostForm, true
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

func (h *Handler) csrfToken(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie(csrfCookie); err == nil && c.Value != "" {
		return c.Value
	}
	// Аллоцируем срез из 32 байтов
	// Он будет наполнен случайными криптографическими данными
	// 32 байта - это 256 случайности
	b := make([]byte, 32)
	// Считываем 32 криптографически случайных байта из rand.Reader в буфер b.
	// rand.Reader — это источник данных. Мы говорим ему: "Дай мне 32 случайных байта." и эти байты мы помещаем в буфер (b)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
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
	if failure != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("public origin must be an absolute HTTP(S) URL")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("public origin must contain only a scheme and host")
	}
	parsed.Path = ""
	return parsed, nil
}
