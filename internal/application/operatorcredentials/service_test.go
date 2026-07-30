package operatorcredentials

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

const canonicalServiceTestPHC = "$argon2id$v=19$m=8192,t=1,p=1$QkJCQkJCQkJCQkJCQkJCQg$v4joKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg"

type fakeHasher struct {
	hash     string
	password string
}

func (hasher *fakeHasher) Hash(password string) (string, error) {
	hasher.password = password
	return hasher.hash, nil
}

type fakeRepository struct {
	operator  Operator
	username  string
	hash      string
	callCount int
}

func (repository *fakeRepository) BootstrapOrReset(_ context.Context, username, hash string) (Operator, error) {
	repository.callCount++
	repository.username = username
	repository.hash = hash
	return repository.operator, nil
}

func TestServiceHashesBeforePersisting(t *testing.T) {
	repository := &fakeRepository{operator: Operator{ID: uuid.New(), Username: "admin"}}
	hasher := &fakeHasher{hash: canonicalServiceTestPHC}
	service := NewService(repository, hasher)

	operator, err := service.BootstrapOrReset(context.Background(), "admin", "exact password")
	if err != nil {
		t.Fatalf("bootstrap credential: %v", err)
	}
	if operator != repository.operator || repository.hash != hasher.hash || repository.username != "admin" {
		t.Fatalf("unexpected service wiring: operator=%+v hash-persisted=%t username=%q calls=%d", operator, repository.hash != "", repository.username, repository.callCount)
	}
	if repository.callCount != 1 {
		t.Fatalf("expected one atomic repository call, got %d", repository.callCount)
	}
}

func TestServiceRejectsInvalidUsernameWithoutHashing(t *testing.T) {
	for index, username := range []string{
		"",
		" admin",
		"admin ",
		"admin\n",
		"admin\x1b[31m",
		"admin\u0085",
		"admin\u202eoperator",
		"admin\u200b",
		"аdmin",
	} {
		repository := &fakeRepository{}
		hasher := &fakeHasher{hash: "hash"}
		service := NewService(repository, hasher)
		if _, err := service.BootstrapOrReset(context.Background(), username, "exact password"); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("username test case %d should be rejected: %v", index, err)
		}
		if hasher.password != "" || repository.callCount != 0 {
			t.Fatalf("invalid username reached credential flow: password-was-passed=%t calls=%d", hasher.password != "", repository.callCount)
		}
	}
}

func TestServiceNeverPersistsPlaintext(t *testing.T) {
	repository := &fakeRepository{operator: Operator{ID: uuid.New(), Username: "admin"}}
	hasher := &fakeHasher{hash: canonicalServiceTestPHC}
	service := NewService(repository, hasher)
	if _, err := service.BootstrapOrReset(context.Background(), "admin", "a secret password"); err != nil {
		t.Fatalf("bootstrap credential: %v", err)
	}
	if repository.hash == "a secret password" {
		t.Fatal("plaintext password was passed to repository")
	}
}
