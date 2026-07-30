// Package password provides the password credential primitive used by local
// operator administration. It intentionally has no transport or persistence
// responsibilities.
package password

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	// MinPasswordLength is measured in bytes. Password bytes are never trimmed,
	// normalized, or otherwise changed before hashing or verification.
	MinPasswordLength = 12
	// MaxPasswordBytes is checked before any KDF work is started.
	MaxPasswordBytes = 1 << 10

	Argon2Version = argon2.Version

	DefaultMemoryKiB  = 64 * 1024
	DefaultIterations = 3
	// A single lane is deliberately conservative for a command that may be
	// invoked on a small administrative host.
	DefaultParallelism = 1
	DefaultSaltLength  = 16
	DefaultKeyLength   = 32

	// These bounds apply to untrusted PHC strings before Argon2 allocates its
	// memory. They permit future parameter upgrades without accepting values
	// that can cause unbounded work or allocation.
	minMemoryKiB       = 8
	maxMemoryKiB       = 128 * 1024
	minIterations      = 1
	maxIterations      = 6
	minParallelism     = 1
	maxParallelism     = 4
	minSaltLength      = 8
	maxSaltLength      = 64
	minKeyLength       = 16
	maxKeyLength       = 64
	maxEncodedHashSize = 512
)

var (
	// ErrPasswordPolicy is returned when a plaintext password is outside the
	// accepted input bounds. It contains no password material.
	ErrPasswordPolicy = errors.New("password does not meet policy")
	// ErrMalformedHash identifies a syntactically invalid or unsafe PHC value.
	ErrMalformedHash = errors.New("password hash is malformed")
	// ErrUnsupportedHash identifies a well-formed PHC discriminator that this
	// package intentionally does not implement.
	ErrUnsupportedHash = errors.New("password hash is unsupported")
)

// Parameters are the Argon2id parameters encoded in a PHC string.
type Parameters struct {
	MemoryKiB   uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultParameters returns a fresh copy of the safe parameters used by Hash.
// It is a function rather than an exported variable so callers cannot mutate
// process-wide password defaults.
func DefaultParameters() Parameters {
	return Parameters{
		MemoryKiB:   DefaultMemoryKiB,
		Iterations:  DefaultIterations,
		Parallelism: DefaultParallelism,
		SaltLength:  DefaultSaltLength,
		KeyLength:   DefaultKeyLength,
	}
}

func defaultParameters() Parameters {
	return DefaultParameters()
}

// ParsedHash is a validated Argon2id PHC value. Its decoded salt and derived
// key are private so callers cannot accidentally inspect, copy, or log
// credential material. Only safe parameter metadata and byte lengths are
// exposed.
type ParsedHash struct {
	Version     uint32
	MemoryKiB   uint32
	Iterations  uint32
	Parallelism uint8
	salt        []byte
	hash        []byte
}

// SaltLength returns the decoded salt length without exposing the salt.
func (parsed ParsedHash) SaltLength() int { return len(parsed.salt) }

// HashLength returns the decoded derived-key length without exposing the key.
func (parsed ParsedHash) HashLength() int { return len(parsed.hash) }

// String deliberately contains metadata only. ParsedHash implements the
// formatter as well as String/GoString because %+v and %#v otherwise provide
// tempting credential disclosure paths during debugging.
func (parsed ParsedHash) String() string {
	return fmt.Sprintf(
		"ParsedHash{Version:%d, MemoryKiB:%d, Iterations:%d, Parallelism:%d, SaltLength:%d, HashLength:%d}",
		parsed.Version,
		parsed.MemoryKiB,
		parsed.Iterations,
		parsed.Parallelism,
		parsed.SaltLength(),
		parsed.HashLength(),
	)
}

// GoString keeps %#v redacted too.
func (parsed ParsedHash) GoString() string { return parsed.String() }

// Format handles every fmt verb without ever passing the private byte slices
// to fmt. The verb and flags are intentionally ignored: all representations
// of a parsed credential remain metadata-only.
func (parsed ParsedHash) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, parsed.String())
}

// Hash creates a versioned Argon2id PHC string using DefaultParameters.
func Hash(plaintext string) (string, error) {
	return HashWithParameters(plaintext, defaultParameters())
}

// HashWithParameters creates a versioned Argon2id PHC string. This is useful
// for controlled tests and explicit future parameter migrations; production
// callers should normally use Hash.
func HashWithParameters(plaintext string, parameters Parameters) (string, error) {
	return hashWithReader(plaintext, parameters, rand.Reader)
}

func hashWithReader(plaintext string, parameters Parameters, random io.Reader) (string, error) {
	if err := validatePassword(plaintext); err != nil {
		return "", err
	}
	if err := parameters.validate(); err != nil {
		return "", err
	}
	if random == nil {
		return "", errors.New("generate password salt")
	}

	salt := make([]byte, parameters.SaltLength)
	if _, err := io.ReadFull(random, salt); err != nil {
		clear(salt)
		return "", errors.New("generate password salt")
	}
	defer clear(salt)

	secret := []byte(plaintext)
	defer clear(secret)
	derived := argon2.IDKey(
		secret,
		salt,
		parameters.Iterations,
		parameters.MemoryKiB,
		parameters.Parallelism,
		parameters.KeyLength,
	)
	defer clear(derived)

	return encodePHC(parameters, salt, derived), nil
}

// Verify checks plaintext against a PHC string. A wrong password is reported
// as (false, nil). Malformed, unsupported, or unsafe hashes fail closed with a
// generic error before any large allocation or KDF work.
//
// The plaintext argument comes first intentionally, matching Hash and making
// accidental logging of the encoded hash less likely at call sites.
func Verify(plaintext, encoded string) (bool, error) {
	if err := validatePassword(plaintext); err != nil {
		return false, err
	}
	parsed, err := ParsePHC(encoded)
	if err != nil {
		return false, err
	}
	defer clear(parsed.salt)
	defer clear(parsed.hash)

	secret := []byte(plaintext)
	defer clear(secret)
	derived := argon2.IDKey(
		secret,
		parsed.salt,
		parsed.Iterations,
		parsed.MemoryKiB,
		parsed.Parallelism,
		uint32(len(parsed.hash)),
	)
	defer clear(derived)
	return subtle.ConstantTimeCompare(derived, parsed.hash) == 1, nil
}

// VerifyHash is the hash-first spelling for callers that use conventional
// compare-hash APIs. Verify remains the primary API.
func VerifyHash(encoded, plaintext string) (bool, error) {
	return Verify(plaintext, encoded)
}

// ParsePHC validates and decodes an Argon2id PHC string without running the
// KDF. Validation is strict so attacker-controlled parameters cannot trigger
// unbounded allocations or work.
func ParsePHC(encoded string) (ParsedHash, error) {
	if len(encoded) == 0 || len(encoded) > maxEncodedHashSize {
		return ParsedHash{}, ErrMalformedHash
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		if len(parts) > 1 && parts[1] != "argon2id" {
			return ParsedHash{}, ErrUnsupportedHash
		}
		return ParsedHash{}, ErrMalformedHash
	}
	if parts[2] != "v=19" {
		return ParsedHash{}, ErrUnsupportedHash
	}

	parameters, err := parseParameters(parts[3])
	if err != nil {
		return ParsedHash{}, err
	}
	salt, err := decodeComponent(parts[4], minSaltLength, maxSaltLength)
	if err != nil {
		return ParsedHash{}, ErrMalformedHash
	}
	derived, err := decodeComponent(parts[5], minKeyLength, maxKeyLength)
	if err != nil {
		clear(salt)
		return ParsedHash{}, ErrMalformedHash
	}
	return ParsedHash{
		Version:     argon2.Version,
		MemoryKiB:   parameters.MemoryKiB,
		Iterations:  parameters.Iterations,
		Parallelism: parameters.Parallelism,
		salt:        salt,
		hash:        derived,
	}, nil
}

// NeedsRehash reports whether a valid hash differs from the current defaults.
// Invalid and unsupported hashes return the corresponding safe parse error.
func NeedsRehash(encoded string) (bool, error) {
	return NeedsRehashWithParameters(encoded, defaultParameters())
}

// NeedsRehashWithParameters reports whether a valid hash differs from the
// supplied current parameters.
func NeedsRehashWithParameters(encoded string, current Parameters) (bool, error) {
	if err := current.validate(); err != nil {
		return false, err
	}
	parsed, err := ParsePHC(encoded)
	if err != nil {
		return false, err
	}
	defer clear(parsed.salt)
	defer clear(parsed.hash)
	return parsed.MemoryKiB != current.MemoryKiB ||
		parsed.Iterations != current.Iterations ||
		parsed.Parallelism != current.Parallelism ||
		uint32(len(parsed.salt)) != current.SaltLength ||
		uint32(len(parsed.hash)) != current.KeyLength, nil
}

func validatePassword(plaintext string) error {
	if len(plaintext) < MinPasswordLength || len(plaintext) > MaxPasswordBytes {
		return ErrPasswordPolicy
	}
	return nil
}

func (parameters Parameters) validate() error {
	if parameters.MemoryKiB < minMemoryKiB || parameters.MemoryKiB > maxMemoryKiB ||
		parameters.Iterations < minIterations || parameters.Iterations > maxIterations ||
		parameters.Parallelism < minParallelism || parameters.Parallelism > maxParallelism ||
		parameters.SaltLength < minSaltLength || parameters.SaltLength > maxSaltLength ||
		parameters.KeyLength < minKeyLength || parameters.KeyLength > maxKeyLength ||
		parameters.MemoryKiB < uint32(8)*uint32(parameters.Parallelism) {
		return ErrMalformedHash
	}
	return nil
}

func encodePHC(parameters Parameters, salt, derived []byte) string {
	encoding := base64.RawStdEncoding
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		parameters.MemoryKiB,
		parameters.Iterations,
		parameters.Parallelism,
		encoding.EncodeToString(salt),
		encoding.EncodeToString(derived),
	)
}

func parseParameters(encoded string) (Parameters, error) {
	parts := strings.Split(encoded, ",")
	if len(parts) != 3 {
		return Parameters{}, ErrMalformedHash
	}
	memory, err := parseParameter(parts[0], "m", maxMemoryKiB)
	if err != nil {
		return Parameters{}, err
	}
	iterations, err := parseParameter(parts[1], "t", maxIterations)
	if err != nil {
		return Parameters{}, err
	}
	parallelism, err := parseParameter(parts[2], "p", maxParallelism)
	if err != nil {
		return Parameters{}, err
	}
	parameters := Parameters{
		MemoryKiB:   uint32(memory),
		Iterations:  uint32(iterations),
		Parallelism: uint8(parallelism),
		SaltLength:  minSaltLength,
		KeyLength:   minKeyLength,
	}
	if parameters.MemoryKiB < minMemoryKiB || parameters.Iterations < minIterations ||
		parameters.Parallelism < minParallelism || parameters.MemoryKiB < uint32(8)*uint32(parameters.Parallelism) {
		return Parameters{}, ErrMalformedHash
	}
	return parameters, nil
}

func parseParameter(encoded, name string, maximum uint64) (uint64, error) {
	prefix := name + "="
	if !strings.HasPrefix(encoded, prefix) {
		return 0, ErrMalformedHash
	}
	digits := strings.TrimPrefix(encoded, prefix)
	if digits == "" || (len(digits) > 1 && digits[0] == '0') {
		return 0, ErrMalformedHash
	}
	value, err := strconv.ParseUint(digits, 10, 32)
	if err != nil || value > maximum {
		return 0, ErrMalformedHash
	}
	return value, nil
}

func decodeComponent(encoded string, minimum, maximum int) ([]byte, error) {
	if encoded == "" || strings.Contains(encoded, "=") {
		return nil, ErrMalformedHash
	}
	if len(encoded) > base64.RawStdEncoding.EncodedLen(maximum) {
		return nil, ErrMalformedHash
	}
	decodedLength := base64.RawStdEncoding.DecodedLen(len(encoded))
	if decodedLength < minimum || decodedLength > maximum {
		return nil, ErrMalformedHash
	}
	decoded, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) < minimum || len(decoded) > maximum ||
		base64.RawStdEncoding.EncodeToString(decoded) != encoded {
		clear(decoded)
		return nil, ErrMalformedHash
	}
	return decoded, nil
}
