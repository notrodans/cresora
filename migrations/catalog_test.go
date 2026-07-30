package migrations

import (
	_ "embed"
	"strings"
	"testing"
)

//go:embed 20260729000300_secure_operator_credentials.sql
var secureOperatorCredentialsMigration string

func TestSecureOperatorCredentialsMigrationIsDestructiveForwardOnly(t *testing.T) {
	migration := strings.ToLower(secureOperatorCredentialsMigration)
	if !strings.Contains(migration, "-- +goose up") {
		t.Fatal("secure credential migration has no Goose up section")
	}
	if strings.Contains(migration, "-- +goose down") {
		t.Fatal("secure credential migration must not recreate legacy password state on down")
	}
	if !strings.Contains(migration, "drop column password") || !strings.Contains(migration, "add column password_hash text") {
		t.Fatal("migration does not destroy legacy password and add the hash column")
	}
	if !strings.Contains(migration, "add column password_changed_at timestamptz") ||
		!strings.Contains(migration, "ck_operators_password_hash_state") ||
		!strings.Contains(migration, "ck_operators_password_hash_format") ||
		!strings.Contains(migration, "update operators") ||
		!strings.Contains(migration, "set tokens_invalid_before") ||
		!strings.Contains(migration, "clock_timestamp()") ||
		!strings.Contains(migration, "greatest(tokens_invalid_before, clock_timestamp())") {
		t.Fatal("migration is missing provisioned/unprovisioned credential state")
	}
	if strings.Contains(migration, "add column password varchar") || strings.Contains(migration, "add column password text") {
		t.Fatal("migration reintroduces a plaintext password column")
	}
	const anchoredPHCConstraint = `password_hash ~ $phc$^\$argon2id\$v=19\$m=[1-9][0-9]*,t=[1-9][0-9]*,p=[1-9][0-9]*\$[a-za-z0-9+/]{11,86}\$[a-za-z0-9+/]{22,86}$$phc$`
	if !strings.Contains(migration, anchoredPHCConstraint) {
		t.Fatal("password_hash format constraint is missing the tagged dollar quote or true regex end anchor")
	}
}
