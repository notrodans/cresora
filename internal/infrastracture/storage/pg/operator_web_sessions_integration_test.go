package pg

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	operatorsessions "github.com/notrodans/nebula-go/internal/application/operatorsessions"
)

func TestOperatorWebSessionStorePostgresSecurityBoundaries(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, _, failure := newIsolatedTelegramPeerLookupDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}

	operatorA, operatorB := uuid.New(), uuid.New()
	for _, operatorID := range []uuid.UUID{operatorA, operatorB} {
		if _, failure = database.Exec(context, `
			INSERT INTO operators (id, username, password_hash, password_changed_at, tokens_invalid_before, enabled)
			VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp() - interval '1 hour', TRUE)`,
			operatorID, operatorID.String(), canonicalOperatorCredentialPHC); failure != nil {
			t.Fatalf("insert provisioned operator: %v", failure)
		}
	}

	store := NewOperatorWebSessionStore(database)
	hashes := make([][32]byte, 0, operatorWebSessionLimit+1)
	for index := 0; index < operatorWebSessionLimit; index++ {
		hash := sha256.Sum256([]byte("operator-a-session-" + string(rune('a'+index))))
		if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, hash[:]); failure != nil {
			t.Fatalf("create session %d: %v", index, failure)
		}
		hashes = append(hashes, hash)
	}
	if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, []byte("short")); failure == nil {
		t.Fatal("accepted non-SHA-256 token hash")
	}

	newest := sha256.Sum256([]byte("operator-a-session-new"))
	if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, newest[:]); failure != nil {
		t.Fatalf("create session over limit: %v", failure)
	}
	if _, failure = store.FindValidSession(context, hashes[0][:]); failure == nil {
		t.Fatal("oldest session remained live past the five-session limit")
	}
	if _, failure = store.FindValidSession(context, newest[:]); failure != nil {
		t.Fatalf("newest session is not valid: %v", failure)
	}

	if failure = store.RevokeSession(context, newest[:]); failure != nil {
		t.Fatalf("revoke session: %v", failure)
	}
	if _, failure = store.FindValidSession(context, newest[:]); failure == nil {
		t.Fatal("revoked session remained valid")
	}

	foreign := sha256.Sum256([]byte("operator-b-session"))
	if _, failure = store.CreateSession(context, operatorB, operatorB.String(), canonicalOperatorCredentialPHC, foreign[:]); failure != nil {
		t.Fatalf("create foreign operator session: %v", failure)
	}
	if _, failure = store.FindValidSession(context, foreign[:]); failure != nil {
		t.Fatalf("foreign operator's own session was rejected: %v", failure)
	}

	if _, failure = database.Exec(context, `UPDATE operators SET enabled = FALSE WHERE id = $1`, operatorB); failure != nil {
		t.Fatalf("disable operator: %v", failure)
	}
	if _, failure = store.FindValidSession(context, foreign[:]); failure == nil {
		t.Fatal("disabled operator session remained valid")
	}

	// Both lifetime bounds are evaluated by PostgreSQL rather than by a
	// timestamp supplied by the caller.
	idleHash := sha256.Sum256([]byte("operator-a-idle-session"))
	if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, idleHash[:]); failure != nil {
		t.Fatalf("create idle session: %v", failure)
	}
	if _, failure = database.Exec(context, `UPDATE operator_web_sessions SET created_at = clock_timestamp() - interval '3 seconds', last_seen_at = clock_timestamp() - interval '2 seconds', idle_expires_at = clock_timestamp() - interval '1 second' WHERE token_hash = $1`, idleHash[:]); failure != nil {
		t.Fatalf("expire idle session: %v", failure)
	}
	if _, failure = store.FindValidSession(context, idleHash[:]); failure == nil {
		t.Fatal("idle-expired session remained valid")
	}
	absoluteHash := sha256.Sum256([]byte("operator-a-absolute-session"))
	if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, absoluteHash[:]); failure != nil {
		t.Fatalf("create absolute session: %v", failure)
	}
	if _, failure = database.Exec(context, `UPDATE operator_web_sessions SET created_at = clock_timestamp() - interval '3 seconds', last_seen_at = clock_timestamp() - interval '2 seconds', idle_expires_at = clock_timestamp() - interval '1 second', absolute_expires_at = clock_timestamp() - interval '1 second' WHERE token_hash = $1`, absoluteHash[:]); failure != nil {
		t.Fatalf("expire absolute session: %v", failure)
	}
	if _, failure = store.FindValidSession(context, absoluteHash[:]); failure == nil {
		t.Fatal("absolute-expired session remained valid")
	}
	if failure = store.RevokeOperatorSessions(context, operatorA); failure != nil {
		t.Fatalf("revoke operator sessions: %v", failure)
	}
	boundaryHash := sha256.Sum256([]byte("operator-a-boundary-session"))
	if _, failure = store.CreateSession(context, operatorA, operatorA.String(), canonicalOperatorCredentialPHC, boundaryHash[:]); failure != nil {
		t.Fatalf("create boundary session: %v", failure)
	}

	// The reset boundary is database-owned and invalidates a session even when
	// the row has not yet been physically marked revoked.
	if _, failure = database.Exec(context, `UPDATE operators SET enabled = TRUE, tokens_invalid_before = clock_timestamp() + interval '1 hour' WHERE id = $1`, operatorA); failure != nil {
		t.Fatalf("advance password-reset boundary: %v", failure)
	}
	if _, failure = store.FindValidSession(context, boundaryHash[:]); failure == nil {
		t.Fatal("session created before tokens_invalid_before remained valid")
	}
}

type blockingLoginVerifier struct {
	ready   chan<- struct{}
	release <-chan struct{}
}

func (verifier blockingLoginVerifier) Verify(string, string) (bool, error) {
	verifier.ready <- struct{}{}
	<-verifier.release
	return true, nil
}

func TestOperatorWebSessionStoreRejectsResetBetweenVerificationAndCreation(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, _, failure := newIsolatedTelegramPeerLookupDatabase(context, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}

	operatorID := uuid.New()
	username := "reset-race-admin"
	resetHash := "$argon2id$v=19$m=8192,t=1,p=1$QkJCQkJCQkJCQkJCQkJCQg$v4jpKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg"
	if _, failure = database.Exec(context, `
		INSERT INTO operators (id, username, password_hash, password_changed_at, tokens_invalid_before, enabled)
		VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp() - interval '1 hour', TRUE)`, operatorID, username, canonicalOperatorCredentialPHC); failure != nil {
		t.Fatalf("insert reset-race operator: %v", failure)
	}

	credentialStore := NewOperatorCredentialStore(database)
	sessionStore := NewOperatorWebSessionStore(database)
	ready := make(chan struct{}, 1)
	release := make(chan struct{})
	service := operatorsessions.NewServiceWithVerifier(
		credentialStore,
		sessionStore,
		blockingLoginVerifier{ready: ready, release: release},
	)
	loginResult := make(chan error, 1)
	go func() {
		_, loginFailure := service.Login(context, username, "correct horse battery staple")
		loginResult <- loginFailure
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("login verifier did not start")
	}

	resetResult := make(chan error, 1)
	go func() {
		_, resetFailure := credentialStore.BootstrapOrReset(context, username, resetHash)
		resetResult <- resetFailure
	}()
	select {
	case resetFailure := <-resetResult:
		if resetFailure != nil {
			t.Fatalf("reset during verification: %v", resetFailure)
		}
	case <-time.After(time.Second):
		t.Fatal("reset did not commit between verification and session creation")
	}
	close(release)
	if loginFailure := <-loginResult; !errors.Is(loginFailure, operatorsessions.ErrAuthentication) {
		t.Fatalf("login after reset returned %v, want generic authentication failure", loginFailure)
	}

	var sessions int
	if failure = database.QueryRow(context, `SELECT count(*) FROM operator_web_sessions WHERE operator_id = $1`, operatorID).Scan(&sessions); failure != nil {
		t.Fatalf("count reset-race sessions: %v", failure)
	}
	if sessions != 0 {
		t.Fatalf("reset-race login created %d session(s)", sessions)
	}
}
