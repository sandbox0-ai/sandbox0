package process

import "errors"

var (
	// ErrProcessSessionNotFound is returned when a process session is missing.
	ErrProcessSessionNotFound = errors.New("process session not found")

	// ErrInvalidProcessSpec is returned for malformed process-session specs.
	ErrInvalidProcessSpec = errors.New("invalid process spec")

	// ErrInvalidProcessEvent is returned for malformed input events.
	ErrInvalidProcessEvent = errors.New("invalid process event")

	// ErrUnsupportedChannelKind is returned for unsupported channel kinds.
	ErrUnsupportedChannelKind = errors.New("unsupported channel kind")

	// ErrUnsupportedChannelEvent is returned when an event does not match a channel.
	ErrUnsupportedChannelEvent = errors.New("unsupported channel event")

	// ErrDuplicateEventID is returned when an input event id is reused with a different payload.
	ErrDuplicateEventID = errors.New("duplicate event id")
)
