package operatorsessions

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/notrodans/nebula-go/internal/application/operatorcredentials/password"
)

type fakeCredentials struct {
	credential Credential
	err        error
}

func (repository *fakeCredentials) FindCredential(context.Context, string) (Credential, error) {
	if repository.err != nil {
		return Credential{}, repository.err
	}
	return repository.credential, nil
}

type fakeSessions struct {
	createdHash         []byte
	createdFor          uuid.UUID
	createdUsername     string
	createdPasswordHash string
	stored              StoredSession
	findHash            []byte
	revokeHash          []byte
}

func (repository *fakeSessions) CreateSession(_ context.Context, operatorID uuid.UUID, username, passwordHash string, tokenHash []byte) (StoredSession, error) {
	repository.createdFor = operatorID
	repository.createdUsername = username
	repository.createdPasswordHash = passwordHash
	repository.createdHash = append([]byte(nil), tokenHash...)
	if repository.stored.ID == uuid.Nil {
		repository.stored = StoredSession{ID: uuid.New(), OperatorID: operatorID, CreatedAt: time.Now(), LastSeenAt: time.Now(), IdleExpiresAt: time.Now().Add(12 * time.Hour), AbsoluteExpiresAt: time.Now().Add(7 * 24 * time.Hour)}
	}
	return repository.stored, nil
}

func (repository *fakeSessions) FindValidSession(_ context.Context, tokenHash []byte) (StoredSession, error) {
	repository.findHash = append([]byte(nil), tokenHash...)
	return repository.stored, nil
}

func (repository *fakeSessions) RevokeSession(_ context.Context, tokenHash []byte) error {
	repository.revokeHash = append([]byte(nil), tokenHash...)
	return nil
}

func (*fakeSessions) RevokeOperatorSessions(context.Context, uuid.UUID) error { return nil }

type countingVerifier struct {
	calls []string
	valid bool
}

func (verifier *countingVerifier) Verify(plaintext, encoded string) (bool, error) {
	verifier.calls = append(verifier.calls, plaintext+"|"+encoded)
	return verifier.valid, nil
}

func TestLoginUsesGenericDummyVerificationForUnknownUsername(t *testing.T) {
	verifier := &countingVerifier{}
	service := NewServiceWithVerifier(&fakeCredentials{err: ErrCredentialNotFound}, &fakeSessions{}, verifier)
	if _, err := service.Login(context.Background(), "missing", "correct horse battery staple"); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("missing login error = %v", err)
	}
	if len(verifier.calls) != 1 || !strings.HasPrefix(verifier.calls[0], dummyPassword+"|") {
		t.Fatalf("dummy verification calls = %#v", verifier.calls)
	}
}

func TestDummyHashMatchesCurrentArgon2idDefaults(t *testing.T) {
	parsed, err := password.ParsePHC(dummyPasswordHash)
	if err != nil {
		t.Fatalf("parse dummy password hash: %v", err)
	}
	defaults := password.DefaultParameters()
	if parsed.Version != password.Argon2Version ||
		parsed.MemoryKiB != defaults.MemoryKiB ||
		parsed.Iterations != defaults.Iterations ||
		parsed.Parallelism != defaults.Parallelism ||
		parsed.SaltLength() != int(defaults.SaltLength) ||
		parsed.HashLength() != int(defaults.KeyLength) {
		t.Fatalf("dummy hash parameters drifted: version=%d memory=%d iterations=%d parallelism=%d salt-length=%d key-length=%d", parsed.Version, parsed.MemoryKiB, parsed.Iterations, parsed.Parallelism, parsed.SaltLength(), parsed.HashLength())
	}
	verified, err := password.Verify(dummyPassword, dummyPasswordHash)
	if err != nil || !verified {
		t.Fatalf("dummy hash does not verify with its dummy password: verified=%v err=%v", verified, err)
	}
}

func TestLoginRejectsInvalidInputWhenOperatorPasswordIsDummyPassword(t *testing.T) {
	operatorID := uuid.New()
	invalidInputs := []struct {
		name      string
		plaintext string
	}{
		{name: "empty", plaintext: ""},
		{name: "short", plaintext: strings.Repeat("x", password.MinPasswordLength-1)},
		{name: "oversized", plaintext: strings.Repeat("x", password.MaxPasswordBytes+1)},
	}

	for _, input := range invalidInputs {
		t.Run(input.name, func(t *testing.T) {
			credentials := &fakeCredentials{credential: Credential{
				OperatorID:   operatorID,
				Username:     "admin",
				PasswordHash: dummyPasswordHash,
				Enabled:      true,
			}}
			sessions := &fakeSessions{}
			service := NewService(credentials, sessions)

			if _, err := service.Login(context.Background(), "admin", input.plaintext); !errors.Is(err, ErrAuthentication) {
				t.Fatalf("invalid input length %d error = %v", len(input.plaintext), err)
			}
			if len(sessions.createdHash) != 0 {
				t.Fatal("invalid input issued a session")
			}
		})
	}
}

type blockingVerifier struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (verifier blockingVerifier) Verify(string, string) (bool, error) {
	verifier.started <- struct{}{}
	<-verifier.release
	return false, nil
}

func TestPasswordVerificationSaturationFailsWithoutStartingAnotherKDF(t *testing.T) {
	started := make(chan struct{}, passwordVerificationConcurrency)
	release := make(chan struct{})
	verifier := blockingVerifier{started: started, release: release}
	credentials := &fakeCredentials{credential: Credential{OperatorID: uuid.New(), Username: "admin", PasswordHash: "hash", Enabled: true}}
	service := NewServiceWithVerifier(credentials, &fakeSessions{}, verifier)

	results := make(chan error, passwordVerificationConcurrency)
	for index := 0; index < passwordVerificationConcurrency; index++ {
		go func() {
			_, err := service.Login(context.Background(), "admin", "correct horse battery staple")
			results <- err
		}()
	}
	for index := 0; index < passwordVerificationConcurrency; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("password verification did not occupy every KDF slot")
		}
	}

	missing := NewServiceWithVerifier(&fakeCredentials{err: ErrCredentialNotFound}, &fakeSessions{}, verifier)
	if _, err := missing.Login(context.Background(), "missing", "correct horse battery staple"); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("saturated dummy login error = %v", err)
	}
	if len(started) != 0 {
		t.Fatalf("saturated dummy login started KDF work: pending verifier calls=%d", len(started))
	}

	close(release)
	for index := 0; index < passwordVerificationConcurrency; index++ {
		if err := <-results; !errors.Is(err, ErrAuthentication) {
			t.Fatalf("blocked login error = %v", err)
		}
	}
}

func TestLoginRotatesOpaqueTokenAndPersistsOnlyHash(t *testing.T) {
	operatorID := uuid.New()
	credentials := &fakeCredentials{credential: Credential{OperatorID: operatorID, Username: "admin", PasswordHash: "argon2", Enabled: true}}
	sessions := &fakeSessions{}
	verifier := &countingVerifier{valid: true}
	service := NewServiceWithVerifier(credentials, sessions, verifier)
	first, err := service.Login(context.Background(), "admin", "correct horse battery staple")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}
	second, err := service.Login(context.Background(), "admin", "correct horse battery staple")
	if err != nil {
		t.Fatalf("second login: %v", err)
	}
	if first.Token == second.Token || len(first.Token) != 43 || len(sessions.createdHash) != 32 {
		t.Fatalf("token rotation/hash lengths: first=%q second=%q hash=%d", first.Token, second.Token, len(sessions.createdHash))
	}
	if string(sessions.createdHash) == first.Token || string(sessions.createdHash) == second.Token {
		t.Fatal("raw token was passed to the session repository")
	}
	if got, ok := TokenHash(second.Token); !ok || string(got) != string(sessions.createdHash) {
		t.Fatal("session repository did not receive the SHA-256 token hash")
	}
	if sessions.createdUsername != "admin" || sessions.createdPasswordHash != "argon2" {
		t.Fatalf("verified credential identity/hash was not carried into session creation: username=%q hash=%q", sessions.createdUsername, sessions.createdPasswordHash)
	}
}

func TestValidateAndRevokeHashOpaqueToken(t *testing.T) {
	sessions := &fakeSessions{stored: StoredSession{ID: uuid.New(), OperatorID: uuid.New()}}
	service := NewServiceWithVerifier(&fakeCredentials{}, sessions, &countingVerifier{})
	token := strings.Repeat("a", 43)
	if _, err := service.Validate(context.Background(), token); err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if len(sessions.findHash) != 32 || string(sessions.findHash) == token {
		t.Fatal("validation did not hash the opaque token")
	}
	if err := service.Revoke(context.Background(), token); err != nil {
		t.Fatalf("revoke token: %v", err)
	}
	if string(sessions.revokeHash) != string(sessions.findHash) {
		t.Fatal("revoke used a different token representation")
	}
	if _, err := service.Validate(context.Background(), "not-a-token"); !errors.Is(err, ErrSessionInvalid) {
		t.Fatalf("malformed token error = %v", err)
	}
}
