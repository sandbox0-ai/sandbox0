package context

import "errors"

var (
	// ErrContextNotFound is returned when a context is not found.
	ErrContextNotFound = errors.New("context not found")

	// ErrMaxContextsReached is returned when maximum contexts limit is reached.
	ErrMaxContextsReached = errors.New("maximum contexts reached")
)
