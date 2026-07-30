// Package principal contains the narrow HTTP seam between a trusted
// principal provider and application handlers.
package principal

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	application "github.com/notrodans/cresora/internal/application"
)

var ErrUnavailable = errors.New("request principal is unavailable")

// Provider supplies the trusted principal for an HTTP request. Providers are
// entrypoint concerns; application services and commands receive the returned
// actor explicitly and never read it from context.
type Provider interface {
	Provide(*http.Request) (application.Actor, error)
}

// ProviderFunc adapts a function to Provider for entrypoint composition and
// tests. The function is trusted infrastructure, not browser input.
type ProviderFunc func(*http.Request) (application.Actor, error)

func (provider ProviderFunc) Provide(request *http.Request) (application.Actor, error) {
	return provider(request)
}

// SessionProvider is an optional stronger provider seam. The raw token is
// carried only for deriving a session-bound CSRF value at the HTTP boundary;
// application handlers never receive it.
type SessionProvider interface {
	Provider
	ProvideSession(*http.Request) (application.Actor, string, error)
}

type contextKey struct{}
type sessionTokenKey struct{}

// Middleware obtains the actor from the trusted provider and makes it
// available only to HTTP handlers through the request context seam.
func Middleware(provider Provider) func(http.Handler) http.Handler {
	if provider == nil {
		panic("create principal middleware without provider")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			response.Header().Set("Cache-Control", "no-store")
			response.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
			response.Header().Set("X-Content-Type-Options", "nosniff")
			response.Header().Set("X-Frame-Options", "DENY")
			response.Header().Set("Referrer-Policy", "no-referrer")
			var (
				actor       application.Actor
				rawToken    string
				failure     error
				sessionAuth bool
			)
			if sessionProvider, ok := provider.(SessionProvider); ok {
				actor, rawToken, failure = sessionProvider.ProvideSession(request)
				sessionAuth = true
			} else {
				actor, failure = provider.Provide(request)
			}
			if failure != nil || actor.OperatorID == uuid.Nil {
				if sessionAuth && request.Method == http.MethodGet {
					location := "/login"
					if request.URL.Path != "/" && request.URL.Path != "/login" {
						next := request.URL.RequestURI()
						if next != "" && next[0] == '/' && !strings.HasPrefix(next, "//") {
							location += "?next=" + url.QueryEscape(next)
						}
					}
					http.Redirect(response, request, location, http.StatusSeeOther)
					return
				}
				http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
			requestContext := context.WithValue(request.Context(), contextKey{}, actor)
			if rawToken != "" {
				requestContext = context.WithValue(requestContext, sessionTokenKey{}, rawToken)
			}
			next.ServeHTTP(response, request.WithContext(requestContext))
		})
	}
}

// FromContext is deliberately kept in the entrypoint package. It is the only
// context extraction point; callers must pass the returned actor explicitly to
// application operations.
func FromContext(context context.Context) (application.Actor, bool) {
	if context == nil {
		return application.Actor{}, false
	}
	actor, ok := context.Value(contextKey{}).(application.Actor)
	if !ok || actor.OperatorID == uuid.Nil {
		return application.Actor{}, false
	}
	return actor, true
}

// SessionTokenFromContext is restricted to HTTP entrypoint code that needs to
// derive a CSRF synchronizer value. It is never an identity source.
func SessionTokenFromContext(context context.Context) (string, bool) {
	if context == nil {
		return "", false
	}
	token, ok := context.Value(sessionTokenKey{}).(string)
	return token, ok && token != ""
}
