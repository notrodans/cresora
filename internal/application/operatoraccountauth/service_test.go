package operatoraccountauth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	applicationroot "github.com/notrodans/cresora/internal/application"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
)

type servicePersistence struct {
	mu sync.Mutex

	account        Account
	beginOutcome   BeginOutcome
	beginCalls     int
	finalizeCalls  int
	beginAborts    int
	completeAborts int
	log            []string
	finalizeError  error
	beginAbortErr  error
	completeErr    error
	authExpiresAt  time.Time
}

func (p *servicePersistence) BeginOrResume(_ context.Context, _ applicationroot.Actor, phone string, expiresAt time.Time) (BeginResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.beginCalls++
	p.log = append(p.log, "begin")
	if !p.authExpiresAt.IsZero() {
		expiresAt = p.authExpiresAt
	}
	account := p.account
	account.Phone = phone
	return BeginResult{Account: account, Outcome: p.beginOutcome, AuthExpiresAt: expiresAt}, nil
}

func (p *servicePersistence) Finalize(_ context.Context, _ applicationroot.Actor, _ operatoraccount.ID, _ operatoraccount.Version, profile Profile) (Account, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.finalizeCalls++
	p.log = append(p.log, "finalize")
	if p.finalizeError != nil {
		return Account{}, p.finalizeError
	}
	account := p.account
	account.Profile = profile
	account.Status = operatoraccount.StatusActive
	return account, nil
}

func (p *servicePersistence) BeginAbort(_ context.Context, _ applicationroot.Actor, _ operatoraccount.ID, _ operatoraccount.Version) (operatoraccount.Version, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.beginAborts++
	p.log = append(p.log, "begin-abort")
	if p.beginAbortErr != nil {
		return 0, p.beginAbortErr
	}
	return p.account.Version + 1, nil
}

func (p *servicePersistence) CompleteAbort(_ context.Context, _ applicationroot.Actor, _ operatoraccount.ID, _ operatoraccount.Version) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completeAborts++
	p.log = append(p.log, "complete-abort")
	return p.completeErr
}

func (p *servicePersistence) ListAccounts(context.Context, applicationroot.Actor) ([]Account, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return []Account{p.account}, nil
}

func (p *servicePersistence) ListOrphanAuthenticationLifecycles(context.Context) ([]AuthTarget, error) {
	return nil, nil
}

type serviceProvider struct {
	mu sync.Mutex

	hash          PhoneCodeHash
	sendError     error
	signInError   error
	passwordErr   error
	passwordsSeen []string
	codeHashes    []PhoneCodeHash
	signIns       atomic.Int32
	passwords     atomic.Int32
	entered       chan struct{}
	release       chan struct{}
	callLog       []string
}

func (p *serviceProvider) SendCode(_ context.Context, _ AuthTarget, _ string) (SendCodeResult, error) {
	p.mu.Lock()
	p.callLog = append(p.callLog, "send")
	p.mu.Unlock()
	return SendCodeResult{PhoneCodeHash: p.hash, Delivery: "SMS"}, p.sendError
}

func (p *serviceProvider) SignIn(_ context.Context, _ AuthTarget, _ string, _ string, hash PhoneCodeHash) (Profile, error) {
	p.signIns.Add(1)
	p.mu.Lock()
	p.codeHashes = append(p.codeHashes, hash)
	p.mu.Unlock()
	if p.entered != nil {
		select {
		case <-p.entered:
		default:
			close(p.entered)
		}
		<-p.release
	}
	return Profile{UserID: 42, Username: "operator"}, p.signInError
}

func (p *serviceProvider) Password(_ context.Context, _ AuthTarget, password string) (Profile, error) {
	p.passwords.Add(1)
	p.mu.Lock()
	p.passwordsSeen = append(p.passwordsSeen, password)
	p.mu.Unlock()
	return Profile{UserID: 42, Username: "operator"}, p.passwordErr
}

type serviceStopper struct {
	mu       sync.Mutex
	err      error
	accounts []AuthTarget
}

func (s *serviceStopper) StopAccount(_ context.Context, target AuthTarget) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts = append(s.accounts, target)
	return s.err
}

func newServiceFixture(t *testing.T) (*Service, *servicePersistence, *serviceProvider, *serviceStopper, applicationroot.Actor) {
	t.Helper()
	accountID := uuid.New()
	actor := applicationroot.Actor{OperatorID: uuid.New()}
	persistence := &servicePersistence{
		account:      Account{ID: accountID, Phone: "+15551234567", Status: operatoraccount.StatusAuthenticating, Version: 1},
		beginOutcome: BeginStarted,
	}
	provider := &serviceProvider{hash: NewPhoneCodeHash("secret-hash")}
	stopper := &serviceStopper{}
	service := NewService(persistence, provider, stopper)
	return service, persistence, provider, stopper, actor
}

func TestServiceStartAdmitsBeforeSendCodeAndKeepsHashOutOfChallenge(t *testing.T) {
	service, persistence, provider, _, actor := newServiceFixture(t)
	result, err := service.Start(context.Background(), actor, "+1 (555) 123-4567")
	if err != nil {
		t.Fatal(err)
	}
	if result.Challenge == nil || result.Challenge.Stage != StageCode {
		t.Fatalf("start result = %+v, want code challenge", result)
	}
	if result.Challenge.Phone != "+15551234567" {
		t.Fatalf("normalized phone = %q", result.Challenge.Phone)
	}
	if persistence.log[0] != "begin" {
		t.Fatalf("persistence/provider order = %v, want begin first", persistence.log)
	}
	if provider.hash.Value() == "" {
		t.Fatal("test hash unexpectedly empty")
	}
	if fmt.Sprintf("%+v", *result.Challenge) == provider.hash.Value() {
		t.Fatal("challenge projection contains the phone-code hash")
	}
}

func TestServiceUsesStoredBeginExpiryAsChallengeUpperBound(t *testing.T) {
	service, persistence, _, _, actor := newServiceFixture(t)
	now := time.Unix(500, 0)
	persistence.authExpiresAt = now.Add(time.Minute)
	persistence.beginOutcome = BeginInProgress
	service = NewService(persistence, service.provider, service.stopper, WithClock(func() time.Time { return now }), WithChallengeTTL(time.Hour))
	result, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	if result.Challenge == nil || !result.Challenge.ExpiresAt.Equal(persistence.authExpiresAt) {
		t.Fatalf("challenge expiry = %v, want stored auth expiry %v", result.Challenge, persistence.authExpiresAt)
	}
}

func TestServicePasswordStageAndCompletionTombstone(t *testing.T) {
	service, persistence, provider, _, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrPasswordRequired
	passwordResult, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
	if !errors.Is(err, ErrPasswordRequired) {
		t.Fatalf("code error = %v, want password required", err)
	}
	if passwordResult.Challenge == nil || passwordResult.Challenge.Stage != StagePassword {
		t.Fatalf("password transition result = %+v", passwordResult)
	}
	provider.signInError = nil
	result, err := service.Password(context.Background(), actor, start.Challenge.RequestID, "correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if result.Account == nil || persistence.finalizeCalls != 1 {
		t.Fatalf("password result = %+v, finalize calls = %d", result, persistence.finalizeCalls)
	}
	duplicate, err := service.Password(context.Background(), actor, start.Challenge.RequestID, "correct horse")
	if err != nil || duplicate.Account == nil {
		t.Fatalf("completion tombstone result = %+v, error = %v", duplicate, err)
	}
	if provider.passwords.Load() != 1 {
		t.Fatalf("password provider calls = %d, want one", provider.passwords.Load())
	}
}

func TestServicePasswordPreservesRawBytes(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrPasswordRequired
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345"); !errors.Is(err, ErrPasswordRequired) {
		t.Fatal(err)
	}
	password := "  exact password\x00 bytes  "
	if _, err := service.Password(context.Background(), actor, start.Challenge.RequestID, password); err != nil {
		t.Fatal(err)
	}
	if len(provider.passwordsSeen) != 1 || provider.passwordsSeen[0] != password {
		t.Fatalf("password passed to provider = %q, want %q", provider.passwordsSeen, password)
	}
}

func TestServiceReservesFixedAttemptBudgetsBeforeProviderCalls(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrInvalidCode
	for attempt := 0; attempt < maxCodeAttempts; attempt++ {
		if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "wrong"); !errors.Is(err, ErrInvalidCode) {
			t.Fatalf("attempt %d error = %v", attempt+1, err)
		}
	}
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "wrong"); !errors.Is(err, ErrAttemptsExceeded) {
		t.Fatalf("exhausted code error = %v", err)
	}
	if provider.signIns.Load() != maxCodeAttempts {
		t.Fatalf("SignIn calls = %d, want %d", provider.signIns.Load(), maxCodeAttempts)
	}
}

func TestServiceUsesFixedGlobalChallengeCapacity(t *testing.T) {
	service, persistence, provider, _, actor := newServiceFixture(t)
	service.coordinator.capacity = 1
	if _, err := service.Start(context.Background(), actor, "+15551234567"); err != nil {
		t.Fatal(err)
	}
	secondActor := applicationroot.Actor{OperatorID: uuid.New()}
	if _, err := service.Start(context.Background(), secondActor, "+15557654321"); !errors.Is(err, ErrChallengeCapacity) {
		t.Fatalf("capacity error = %v, want bounded capacity failure", err)
	}
	if persistence.beginAborts != 1 {
		t.Fatalf("capacity conflict did not abort newly admitted account: begin aborts = %d", persistence.beginAborts)
	}
	if provider.signIns.Load() != 0 {
		t.Fatalf("capacity conflict unexpectedly invoked SignIn: %d", provider.signIns.Load())
	}
}

func TestServiceRetriesOnlyFinalizeAfterProviderAuthorization(t *testing.T) {
	service, persistence, provider, _, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	persistence.finalizeError = errors.New("finalize temporarily failed")
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345"); err == nil {
		t.Fatal("first finalization unexpectedly succeeded")
	}
	service.coordinator.mu.Lock()
	record := service.coordinator.challenges[start.Challenge.RequestID]
	service.coordinator.mu.Unlock()
	if record == nil {
		t.Fatal("pending finalization challenge was removed")
	}
	record.mu.Lock()
	hashCleared := record.hash.IsZero()
	pendingRetained := record.pendingProfile != nil
	record.mu.Unlock()
	if provider.signIns.Load() != 1 || len(provider.codeHashes) != 1 || !hashCleared || !pendingRetained {
		t.Fatalf("authorized state was not retained safely: sign-ins=%d hashes=%+v hash-cleared=%t pending=%t", provider.signIns.Load(), provider.codeHashes, hashCleared, pendingRetained)
	}
	persistence.finalizeError = nil
	result, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
	if err != nil || result.Account == nil {
		t.Fatalf("retry finalization result = %+v, error = %v", result, err)
	}
	if provider.signIns.Load() != 1 {
		t.Fatalf("retry reran SignIn: calls = %d", provider.signIns.Load())
	}
}

func TestServiceInvalidCodePreservesChallenge(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrInvalidCode
	result, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "wrong")
	if !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("code error = %v, want invalid code", err)
	}
	if result.Challenge == nil || result.Challenge.RequestID != start.Challenge.RequestID {
		t.Fatalf("invalid code removed challenge: %+v", result)
	}
}

func TestServiceCapsRetryAfterAtChallengeExpiry(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	service = NewService(service.persistence, provider, service.stopper, WithChallengeTTL(time.Minute))
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	providerFailure, constructorErr := NewRetryAfterError(2 * time.Hour)
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	provider.signInError = providerFailure
	result, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
	if result.Challenge == nil {
		t.Fatalf("retry result = %+v, want preserved challenge", result)
	}
	var retry *RetryAfterError
	if !errors.As(err, &retry) {
		t.Fatalf("retry error = %v, want RetryAfterError", err)
	}
	if retry.RetryAfter() > time.Minute {
		t.Fatalf("retry-after = %s, exceeds challenge TTL", retry.RetryAfter())
	}
}

func TestServiceSerializesCodeOperationsPerChallenge(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	provider.entered = make(chan struct{})
	provider.release = make(chan struct{})
	provider.signInError = ErrInvalidCode
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, operationErr := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
		first <- operationErr
	}()
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("first SignIn did not start")
	}
	second := make(chan error, 1)
	go func() {
		_, operationErr := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
		second <- operationErr
	}()
	select {
	case <-second:
		t.Fatal("second SignIn ran concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	close(provider.release)
	if err := <-first; !errors.Is(err, ErrInvalidCode) {
		t.Fatal(err)
	}
	if err := <-second; !errors.Is(err, ErrInvalidCode) {
		t.Fatal(err)
	}
	if provider.signIns.Load() != 2 {
		t.Fatalf("SignIn calls = %d, want two serialized calls", provider.signIns.Load())
	}
}

func TestServiceUnauthorizedUsesStrictAbortOrder(t *testing.T) {
	service, persistence, provider, stopper, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrUnauthorized
	_, err = service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
	if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, ErrAuthenticationAborted) {
		t.Fatalf("unauthorized error = %v", err)
	}
	if persistence.completeAborts != 1 || len(stopper.accounts) != 1 {
		t.Fatalf("abort calls: begin=%d stop=%d complete=%d", persistence.beginAborts, len(stopper.accounts), persistence.completeAborts)
	}
	if persistence.log[len(persistence.log)-2] != "begin-abort" || persistence.log[len(persistence.log)-1] != "complete-abort" {
		t.Fatalf("abort order log = %v", persistence.log)
	}
	if stopper.accounts[0].Version != 1 {
		t.Fatalf("StopAccount version = %d, want original authenticating version 1", stopper.accounts[0].Version)
	}
}

func TestServiceExpiredChallengeStrictlyAbortsBeforeRemoval(t *testing.T) {
	service, persistence, provider, stopper, actor := newServiceFixture(t)
	now := time.Unix(100, 0)
	service = NewService(persistence, provider, stopper, WithClock(func() time.Time { return now }), WithChallengeTTL(time.Minute))
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345"); !errors.Is(err, ErrChallengeExpired) {
		t.Fatalf("expired code error = %v", err)
	}
	if provider.signIns.Load() != 0 || persistence.beginAborts != 1 || persistence.completeAborts != 1 {
		t.Fatalf("expired challenge calls: sign-in=%d begin-abort=%d complete-abort=%d", provider.signIns.Load(), persistence.beginAborts, persistence.completeAborts)
	}
	if len(stopper.accounts) != 1 || stopper.accounts[0].Version != 1 {
		t.Fatalf("expired StopAccount calls = %+v, want original version", stopper.accounts)
	}
	status, err := service.Status(context.Background(), actor)
	if err != nil || status.Challenge != nil {
		t.Fatalf("expired challenge remained after strict abort: %+v, %v", status, err)
	}
}

func TestServiceShutdownClosesAdmissionAbortsAndClearsState(t *testing.T) {
	service, persistence, _, stopper, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if persistence.beginAborts != 1 || persistence.completeAborts != 1 || len(stopper.accounts) != 1 {
		t.Fatalf("shutdown abort calls: begin=%d stop=%d complete=%d", persistence.beginAborts, len(stopper.accounts), persistence.completeAborts)
	}
	if stopper.accounts[0].Version != 1 {
		t.Fatalf("shutdown StopAccount version = %d, want original version", stopper.accounts[0].Version)
	}
	if _, err := service.Start(context.Background(), actor, "+15551234567"); !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("start after shutdown error = %v", err)
	}
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345"); !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("code after shutdown error = %v", err)
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatalf("idempotent shutdown error = %v", err)
	}
}

func TestServiceShutdownHonorsContextWhileProviderIgnoresCancellation(t *testing.T) {
	service, _, provider, _, actor := newServiceFixture(t)
	provider.entered = make(chan struct{})
	provider.release = make(chan struct{})
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	codeDone := make(chan error, 1)
	go func() {
		_, codeErr := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
		codeDone <- codeErr
	}()
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("SignIn did not start")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.Shutdown(shutdownContext); err == nil {
		t.Fatal("shutdown unexpectedly ignored its deadline")
	}
	close(provider.release)
	select {
	case <-codeDone:
	case <-time.After(time.Second):
		t.Fatal("canceled provider operation did not finish")
	}
}

func TestServiceShutdownClearsFailedAbortSecretsForRetry(t *testing.T) {
	service, persistence, provider, stopper, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	persistence.finalizeError = errors.New("finalize temporarily failed")
	if _, err := service.Code(context.Background(), actor, start.Challenge.RequestID, "12345"); err == nil {
		t.Fatal("failed finalization unexpectedly succeeded")
	}
	if provider.signIns.Load() != 1 {
		t.Fatalf("SignIn calls = %d, want one", provider.signIns.Load())
	}
	stopper.err = errors.New("stop failed")
	if err := service.Shutdown(context.Background()); err == nil {
		t.Fatal("failed shutdown unexpectedly succeeded")
	}
	service.coordinator.mu.Lock()
	remaining := len(service.coordinator.challenges)
	service.coordinator.mu.Unlock()
	if remaining != 1 || persistence.completeAborts != 0 {
		t.Fatalf("failed abort was not retained: challenges=%d complete-aborts=%d", remaining, persistence.completeAborts)
	}
	service.coordinator.mu.Lock()
	record := service.coordinator.challenges[start.Challenge.RequestID]
	service.coordinator.mu.Unlock()
	record.mu.Lock()
	hashCleared := record.hash.IsZero()
	pendingCleared := record.pendingProfile == nil
	transientStateCleared := record.phone == "" && record.delivery == "" && record.stage == "" &&
		record.expires.Load().(time.Time).IsZero() && !record.ready && record.codeAttempts == 0 && record.passwordAttempts == 0
	targetRetained := record.target == start.Challenge.AuthTarget
	requestRetained := record.requestID == start.Challenge.RequestID
	record.mu.Unlock()
	if !hashCleared || !pendingCleared || !transientStateCleared || !targetRetained || !requestRetained {
		t.Fatalf("failed abort retained authentication state: hash-cleared=%t pending-cleared=%t transient-cleared=%t target-retained=%t request-retained=%t", hashCleared, pendingCleared, transientStateCleared, targetRetained, requestRetained)
	}
	stopper.err = nil
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	service.coordinator.mu.Lock()
	remaining = len(service.coordinator.challenges)
	service.coordinator.mu.Unlock()
	if remaining != 0 || persistence.completeAborts != 1 {
		t.Fatalf("retry did not finish abort: challenges=%d complete-aborts=%d", remaining, persistence.completeAborts)
	}
}

func TestServiceCancelDoesNotCrossActorScope(t *testing.T) {
	service, _, _, _, owner := newServiceFixture(t)
	start, err := service.Start(context.Background(), owner, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	foreign := applicationroot.Actor{OperatorID: uuid.New()}
	if err := service.Cancel(context.Background(), foreign, start.Challenge.RequestID); !errors.Is(err, ErrChallengeNotFound) {
		t.Fatalf("foreign cancel error = %v, want challenge not found", err)
	}
}

func TestServiceNeverCompletesAbortWhenRuntimeStopFails(t *testing.T) {
	service, persistence, provider, stopper, actor := newServiceFixture(t)
	start, err := service.Start(context.Background(), actor, "+15551234567")
	if err != nil {
		t.Fatal(err)
	}
	provider.signInError = ErrSessionUnavailable
	stopper.err = errors.New("runtime stop failed")
	_, err = service.Code(context.Background(), actor, start.Challenge.RequestID, "12345")
	if err == nil || persistence.completeAborts != 0 {
		t.Fatalf("stop failure error = %v, complete aborts = %d", err, persistence.completeAborts)
	}
}

func TestServiceStatusMergesAccountsAndChallenge(t *testing.T) {
	service, _, _, _, actor := newServiceFixture(t)
	if _, err := service.Start(context.Background(), actor, "+15551234567"); err != nil {
		t.Fatal(err)
	}
	status, err := service.Status(context.Background(), actor)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Accounts) != 1 || status.Challenge == nil {
		t.Fatalf("status = %+v, want account and challenge", status)
	}
}
