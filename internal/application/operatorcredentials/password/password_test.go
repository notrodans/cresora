package password

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

var testParameters = Parameters{
	MemoryKiB:   8 * 1024,
	Iterations:  1,
	Parallelism: 1,
	SaltLength:  16,
	KeyLength:   32,
}

func TestHashVerifyRoundTripAndWrongPassword(t *testing.T) {
	encoded, err := hashWithReader("correct horse battery", testParameters, bytes.NewReader(bytes.Repeat([]byte{0x42}, 16)))
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=8192,t=1,p=1$") {
		t.Fatal("unexpected PHC prefix")
	}
	valid, err := Verify("correct horse battery", encoded)
	if err != nil || !valid {
		t.Fatalf("verify correct password: valid=%v err=%v", valid, err)
	}
	valid, err = Verify("incorrect horse battery", encoded)
	if err != nil || valid {
		t.Fatalf("verify wrong password: valid=%v err=%v", valid, err)
	}
	valid, err = VerifyHash(encoded, "correct horse battery")
	if err != nil || !valid {
		t.Fatalf("verify hash-first spelling: valid=%v err=%v", valid, err)
	}
}

func TestDefaultHashUsesSafePHCParameters(t *testing.T) {
	encoded, err := Hash("default password")
	if err != nil {
		t.Fatalf("hash with defaults: %v", err)
	}
	parsed, err := ParsePHC(encoded)
	if err != nil {
		t.Fatalf("parse default hash: %v", err)
	}
	defaults := DefaultParameters()
	if parsed.Version != Argon2Version || parsed.MemoryKiB != defaults.MemoryKiB ||
		parsed.Iterations != defaults.Iterations || parsed.Parallelism != defaults.Parallelism ||
		parsed.SaltLength() != int(defaults.SaltLength) || parsed.HashLength() != int(defaults.KeyLength) {
		t.Fatalf("unexpected defaults: version=%d memory=%d iterations=%d parallelism=%d salt-length=%d hash-length=%d", parsed.Version, parsed.MemoryKiB, parsed.Iterations, parsed.Parallelism, parsed.SaltLength(), parsed.HashLength())
	}
}

func TestDefaultParametersReturnsIndependentValues(t *testing.T) {
	first := DefaultParameters()
	first.MemoryKiB = 1
	second := DefaultParameters()
	if second.MemoryKiB != DefaultMemoryKiB {
		t.Fatal("password defaults were mutable through a returned value")
	}
}

func TestParsedHashFormattingRedactsDecodedCredentialBytes(t *testing.T) {
	parsed, err := ParsePHC(canonicalTestPHC)
	if err != nil {
		t.Fatalf("parse canonical fixture: %v", err)
	}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		formatted := fmt.Sprintf(verb, parsed)
		if strings.Contains(formatted, canonicalTestSalt) || strings.Contains(formatted, canonicalTestHash) {
			t.Fatalf("%s exposed credential bytes", verb)
		}
		if !strings.Contains(formatted, "SaltLength:16") || !strings.Contains(formatted, "HashLength:32") {
			t.Fatalf("%s omitted safe metadata", verb)
		}
	}
}

func TestCanonicalLowCostPHCVerifies(t *testing.T) {
	valid, err := Verify("fixture password", canonicalTestPHC)
	if err != nil || !valid {
		t.Fatalf("canonical fixture should verify: valid=%v err=%v", valid, err)
	}
	valid, err = Verify("different password", canonicalTestPHC)
	if err != nil || valid {
		t.Fatalf("canonical fixture should reject a wrong password: valid=%v err=%v", valid, err)
	}
}

func TestPHCMalformedUnsupportedAndMaliciousParametersFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		value string
		err   error
	}{
		{name: "wrong algorithm", value: "$bcrypt$v=19$m=8192,t=1,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrUnsupportedHash},
		{name: "wrong version", value: "$argon2id$v=18$m=8192,t=1,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrUnsupportedHash},
		{name: "memory allocation bomb", value: "$argon2id$v=19$m=4294967295,t=1,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrMalformedHash},
		{name: "parallelism allocation bomb", value: "$argon2id$v=19$m=65536,t=1,p=999999$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrMalformedHash},
		{name: "padded base64", value: "$argon2id$v=19$m=8192,t=1,p=1$AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrMalformedHash},
		{name: "duplicate parameter", value: "$argon2id$v=19$m=8192,m=8192,t=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", err: ErrMalformedHash},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParsePHC(test.value)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
			valid, verifyErr := Verify("password with twelve", test.value)
			if valid || !errors.Is(verifyErr, test.err) {
				t.Fatalf("verify should fail closed: valid=%v err=%v", valid, verifyErr)
			}
		})
	}
}

const (
	// This fixture is a real Argon2id PHC value generated with the same
	// low-cost parameters used by tests. It is intentionally not a fake PHC
	// prefix: ParsePHC and Verify must accept it as a complete credential.
	canonicalTestSalt = "QkJCQkJCQkJCQkJCQkJCQg"
	canonicalTestHash = "v4joKPPnZL3tVyLC0OzMuWdDDxie5pxlJcAYc3Uclgg"
	canonicalTestPHC  = "$argon2id$v=19$m=8192,t=1,p=1$" + canonicalTestSalt + "$" + canonicalTestHash
)

func TestNeedsRehash(t *testing.T) {
	encoded, err := hashWithReader("password for rehash", testParameters, bytes.NewReader(bytes.Repeat([]byte{0x77}, 16)))
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	needs, err := NeedsRehashWithParameters(encoded, testParameters)
	if err != nil || needs {
		t.Fatalf("matching parameters should not need rehash: needs=%v err=%v", needs, err)
	}
	needs, err = NeedsRehash(encoded)
	if err != nil || !needs {
		t.Fatalf("non-default parameters should need rehash: needs=%v err=%v", needs, err)
	}
	needs, err = NeedsRehash("not a PHC hash")
	if needs || !errors.Is(err, ErrMalformedHash) {
		t.Fatalf("malformed hash should fail closed: needs=%v err=%v", needs, err)
	}
}

func TestPasswordPolicyAndNoNormalization(t *testing.T) {
	for _, value := range []string{strings.Repeat("x", MinPasswordLength-1), strings.Repeat("x", MaxPasswordBytes+1)} {
		if _, err := hashWithReader(value, testParameters, bytes.NewReader(bytes.Repeat([]byte{0x11}, 16))); !errors.Is(err, ErrPasswordPolicy) {
			t.Fatalf("password length %d should fail policy: %v", len(value), err)
		}
	}
	if _, err := hashWithReader(strings.Repeat("x", MinPasswordLength), testParameters, bytes.NewReader(bytes.Repeat([]byte{0x11}, 16))); err != nil {
		t.Fatalf("minimum-length password should be accepted: %v", err)
	}

	original := "  exact password  "
	encoded, err := hashWithReader(original, testParameters, bytes.NewReader(bytes.Repeat([]byte{0x22}, 16)))
	if err != nil {
		t.Fatalf("hash unnormalized password: %v", err)
	}
	valid, err := Verify(original, encoded)
	if err != nil || !valid {
		t.Fatalf("verify exact password: valid=%v err=%v", valid, err)
	}
	valid, err = Verify(strings.TrimSpace(original), encoded)
	if err != nil || valid {
		t.Fatalf("trimmed password must not verify: valid=%v err=%v", valid, err)
	}
}

func TestParametersRejectUnsafeValuesBeforeKDF(t *testing.T) {
	unsafe := []Parameters{
		{MemoryKiB: 0, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{MemoryKiB: maxMemoryKiB + 1, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{MemoryKiB: 8192, Iterations: maxIterations + 1, Parallelism: 1, SaltLength: 16, KeyLength: 32},
		{MemoryKiB: 8192, Iterations: 1, Parallelism: maxParallelism + 1, SaltLength: 16, KeyLength: 32},
		{MemoryKiB: 8192, Iterations: 1, Parallelism: 1, SaltLength: maxSaltLength + 1, KeyLength: 32},
		{MemoryKiB: 8192, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: maxKeyLength + 1},
	}
	for _, parameters := range unsafe {
		if _, err := HashWithParameters("password with twelve", parameters); !errors.Is(err, ErrMalformedHash) {
			t.Fatalf("unsafe parameters should be rejected: %+v -> %v", parameters, err)
		}
	}
}
