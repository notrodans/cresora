// Package application contains values shared by application use cases.
package application

import "github.com/google/uuid"

// Actor is the authenticated application principal for one request.
//
// OperatorID is intentionally the only identity attribute in this value. It
// is passed explicitly to application commands and services; application code
// must not derive it from a context, form, cookie, or other transport value.
type Actor struct {
	OperatorID uuid.UUID
}

// Principal is the transport-neutral name for Actor used by authentication
// entrypoints. It is an alias so the application has one minimal principal
// representation rather than parallel identity types.
type Principal = Actor
