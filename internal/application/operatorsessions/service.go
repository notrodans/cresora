// Package operatorsessions owns the browser authentication contract.  It
// deliberately keeps the bearer value in the HTTP boundary: persistence only
// receives its SHA-256 digest.
package operatorsessions

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/notrodans/nebula-go/internal/application/operatorcredentials/password"
)

var (
	// ErrAuthentication is intentionally shared by every login failure.  HTTP
	// callers must not be able to distinguish an unknown, disabled, or
	// unprovisioned operator from a bad password.
	ErrAuthentication     = errors.New("operator authentication failed")
	ErrSessionInvalid     = errors.New("operator session is invalid")
	ErrCredentialNotFound = errors.New("operator credential not found")
)

// Credential is the minimum credential projection needed by the login use
// case. PasswordHash is never returned from this package to an HTTP handler.
type Credential struct {
	OperatorID   uuid.UUID
	Username     string
	PasswordHash string
	Enabled      bool
}

// CredentialRepository looks up a credential by username. Implementations
// should return ErrCredentialNotFound for a missing username.
type CredentialRepository interface {
	FindCredential(context.Context, string) (Credential, error)
}

// Session is the server-side session projection. Token is populated only by a
// successful Login call and is intended for one response cookie; it is never a
// persistence field.
type Session struct {
	ID                uuid.UUID
	OperatorID        uuid.UUID
	Token             string
	CreatedAt         time.Time
	LastSeenAt        time.Time
	IdleExpiresAt     time.Time
	AbsoluteExpiresAt time.Time
}

// StoredSession is the database projection. There is deliberately no token
// or request metadata here.
type StoredSession struct {
	ID                uuid.UUID
	OperatorID        uuid.UUID
	CreatedAt         time.Time
	LastSeenAt        time.Time
	IdleExpiresAt     time.Time
	AbsoluteExpiresAt time.Time
}

// SessionRepository is implemented by the PostgreSQL adapter. The tokenHash
// argument is always exactly 32 bytes of SHA-256 output.
type SessionRepository interface {
	// CreateSession must lock the operator row and compare the supplied
	// credential identity and hash with the current row before inserting a
	// session. The values are the exact credential projection verified by Login.
	CreateSession(context.Context, uuid.UUID, string, string, []byte) (StoredSession, error)
	FindValidSession(context.Context, []byte) (StoredSession, error)
	RevokeSession(context.Context, []byte) error
	RevokeOperatorSessions(context.Context, uuid.UUID) error
}

// PasswordVerifier is a narrow seam so application tests can avoid a memory-
// hard operation while production uses the Argon2id implementation.
type PasswordVerifier interface {
	Verify(string, string) (bool, error)
}

type passwordVerifier struct{}

func (passwordVerifier) Verify(plaintext, encoded string) (bool, error) {
	return password.Verify(plaintext, encoded)
}

// VerifyFunc adapts a test or explicitly configured verifier.
type VerifyFunc func(string, string) (bool, error)

func (function VerifyFunc) Verify(plaintext, encoded string) (bool, error) {
	return function(plaintext, encoded)
}

// Service coordinates credential verification and opaque session creation.
type Service struct {
	credentials CredentialRepository
	sessions    SessionRepository
	verifier    PasswordVerifier
}

// Password verification is deliberately non-blocking at the process boundary:
// requests that arrive after all KDF slots are occupied fail generically
// instead of queueing unbounded memory and CPU work. This is process-wide so
// multiple services/routers in one process cannot multiply the KDF budget.
const passwordVerificationConcurrency = 8

var passwordVerificationSlots = make(chan struct{}, passwordVerificationConcurrency)

// NewService constructs the production authentication service.
func NewService(credentials CredentialRepository, sessions SessionRepository) Service {
	return Service{credentials: credentials, sessions: sessions, verifier: passwordVerifier{}}
}

// NewServiceWithVerifier is useful for server-side tests and controlled
// adapters. The default production constructor always uses Argon2id.
func NewServiceWithVerifier(credentials CredentialRepository, sessions SessionRepository, verifier PasswordVerifier) Service {
	if verifier == nil {
		panic("create operator session service without password verifier")
	}
	return Service{credentials: credentials, sessions: sessions, verifier: verifier}
}

// Login verifies credentials and creates a new random bearer token. A token
// is never reused, including when the caller supplied a pre-authentication
// cookie. The repository receives only the digest.
func (service Service) Login(context context.Context, username, plaintext string) (Session, error) {
	if context == nil || service.credentials == nil || service.sessions == nil || service.verifier == nil {
		return Session{}, ErrAuthentication
	}

	credential, lookupFailure := service.credentials.FindCredential(context, username)
	if lookupFailure != nil {
		service.dummyVerify()
		return Session{}, ErrAuthentication
	}

	// Invalid-length inputs must still perform a verifier operation. This
	// prevents short invalid passwords from creating a useful timing oracle,
	// but the original input validity must remain independent of the candidate
	// passed to the verifier.
	inputValid := len(plaintext) >= password.MinPasswordLength && len(plaintext) <= password.MaxPasswordBytes
	candidate := plaintext
	if !inputValid {
		candidate = dummyPassword
	}
	verifyHash := credential.PasswordHash
	if verifyHash == "" {
		verifyHash = dummyPasswordHash
	}
	verified, verifyFailure := service.verifyPassword(candidate, verifyHash)
	if verifyFailure != nil || !verified || !inputValid || !credential.Enabled || credential.OperatorID == uuid.Nil || credential.PasswordHash == "" {
		return Session{}, ErrAuthentication
	}
	if credential.Username == "" || credential.Username != username {
		return Session{}, ErrAuthentication
	}

	rawToken := make([]byte, 32)
	if _, failure := rand.Read(rawToken); failure != nil {
		return Session{}, ErrAuthentication
	}
	defer clear(rawToken)
	hash := sha256.Sum256(rawToken)
	stored, createFailure := service.sessions.CreateSession(context, credential.OperatorID, credential.Username, credential.PasswordHash, hash[:])
	if createFailure != nil || stored.OperatorID != credential.OperatorID {
		return Session{}, ErrAuthentication
	}
	return Session{
		ID:                stored.ID,
		OperatorID:        stored.OperatorID,
		Token:             base64.RawURLEncoding.EncodeToString(rawToken),
		CreatedAt:         stored.CreatedAt,
		LastSeenAt:        stored.LastSeenAt,
		IdleExpiresAt:     stored.IdleExpiresAt,
		AbsoluteExpiresAt: stored.AbsoluteExpiresAt,
	}, nil
}

// Validate authenticates an opaque cookie value against the server-side
// session row. Invalid encodings never reach persistence.
func (service Service) Validate(context context.Context, token string) (Session, error) {
	if context == nil || service.sessions == nil {
		return Session{}, ErrSessionInvalid
	}
	raw, ok := decodeToken(token)
	if !ok {
		return Session{}, ErrSessionInvalid
	}
	hash := sha256.Sum256(raw)
	stored, failure := service.sessions.FindValidSession(context, hash[:])
	if failure != nil || stored.OperatorID == uuid.Nil {
		return Session{}, ErrSessionInvalid
	}
	return Session{
		ID:                stored.ID,
		OperatorID:        stored.OperatorID,
		CreatedAt:         stored.CreatedAt,
		LastSeenAt:        stored.LastSeenAt,
		IdleExpiresAt:     stored.IdleExpiresAt,
		AbsoluteExpiresAt: stored.AbsoluteExpiresAt,
	}, nil
}

// Revoke invalidates the server-side row. A malformed value is treated as
// already logged out and does not produce a database query.
func (service Service) Revoke(context context.Context, token string) error {
	if context == nil || service.sessions == nil {
		return nil
	}
	raw, ok := decodeToken(token)
	if !ok {
		return nil
	}
	hash := sha256.Sum256(raw)
	return service.sessions.RevokeSession(context, hash[:])
}

// RevokeOperatorSessions is used by administrative credential reset paths in
// addition to the database tokens_invalid_before boundary.
func (service Service) RevokeOperatorSessions(context context.Context, operatorID uuid.UUID) error {
	if context == nil || service.sessions == nil || operatorID == uuid.Nil {
		return nil
	}
	return service.sessions.RevokeOperatorSessions(context, operatorID)
}

// TokenHash is the only supported persistence representation of a bearer
// token. The returned slice is a copy and can be passed to a database driver.
func TokenHash(token string) ([]byte, bool) {
	raw, ok := decodeToken(token)
	if !ok {
		return nil, false
	}
	hash := sha256.Sum256(raw)
	return append([]byte(nil), hash[:]...), true
}

// SessionCSRFToken derives a synchronizer value from the authenticated bearer
// token. It is not a second cookie and changes with every rotated session.
func SessionCSRFToken(token string) (string, bool) {
	if _, ok := decodeToken(token); !ok {
		return "", false
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte("nebula/operator-session/csrf/v1"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), true
}

func (service Service) dummyVerify() {
	_, _ = service.verifyPassword(dummyPassword, dummyPasswordHash)
}

func (service Service) verifyPassword(plaintext, encoded string) (bool, error) {
	select {
	case passwordVerificationSlots <- struct{}{}:
		defer func() { <-passwordVerificationSlots }()
		return service.verifier.Verify(plaintext, encoded)
	default:
		return false, ErrAuthentication
	}
}

func decodeToken(token string) ([]byte, bool) {
	if len(token) != base64.RawURLEncoding.EncodedLen(32) {
		return nil, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return nil, false
	}
	return raw, true
}

const (
	dummyPassword = "nebula-invalid-login-dummy"
	// This is a valid Argon2id PHC string using exactly DefaultParameters and
	// Argon2Version. It is used solely to consume comparable verification work
	// for an unknown username and contains no live secret. The parameter
	// equality test in this package must be kept with the immutable password
	// defaults if those defaults are ever intentionally changed.
	dummyPasswordHash = "$argon2id$v=19$m=65536,t=3,p=1$QkJCQkJCQkJCQkJCQkJCQg$zcS15uZIKyLKK8fEVrDvkFnAJRpTIpduyDeW4MTQScQ"
)
