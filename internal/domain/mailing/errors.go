package mailing

import "errors"

// ErrUnresolvedDeliveryOutcomes prevents a stopped mailing from being queued
// again while a prior logical delivery can still produce a late outcome.
var ErrUnresolvedDeliveryOutcomes = errors.New("mailing has unresolved delivery outcomes")
