package mailing

import (
	"context"
	"errors"
)

var (
	// ErrNotFound indicates that a mailing does not exist in the selected scope.
	ErrNotFound = errors.New("mailing does not exist")
	// ErrInvalidState indicates that a mailing cannot perform its lifecycle transition.
	ErrInvalidState = errors.New("mailing has invalid state")
	// ErrNoEligibleRecipients indicates that a mailing has no recipients to deliver.
	ErrNoEligibleRecipients = errors.New("mailing has no eligible recipients")
)

// Mailing represents one persistent mailing.
type Mailing interface {
	Queue(context.Context) error
	Stop(context.Context) error
}
