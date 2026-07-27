// Package api contains the source OpenAPI contract and its generation directive.
package api

//go:generate go tool ogen --target ../internal/entrypoint/http/ogen --package ogen --clean openapi.yaml
