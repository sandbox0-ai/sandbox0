package context

import "errors"

var (
	// ErrContextNotFound is returned when a context is not found.
	ErrContextNotFound = errors.New("context not found")
)
