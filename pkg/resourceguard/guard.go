// Package resourceguard provides reusable single-object size guards for
// control-plane inputs and persistence boundaries.
package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// TooLargeError reports that one bounded resource exceeded its byte or item
// limit. It intentionally contains no input values so secret-bearing callers
// can safely return or log it.
type TooLargeError struct {
	Resource string
	Limit    int64
	Actual   int64
	Unit     string
}

func (e *TooLargeError) Error() string {
	if e == nil {
		return "resource is too large"
	}
	unit := e.Unit
	if unit == "" {
		unit = "bytes"
	}
	if e.Actual > 0 {
		return fmt.Sprintf("%s is too large: %d %s exceeds limit %d", e.Resource, e.Actual, unit, e.Limit)
	}
	return fmt.Sprintf("%s is too large: limit is %d %s", e.Resource, e.Limit, unit)
}

// IsTooLarge reports whether err contains a resource size violation.
func IsTooLarge(err error) bool {
	var tooLarge *TooLargeError
	return errors.As(err, &tooLarge)
}

// LimitJSONBody eagerly reads and restores one JSON request body while
// enforcing an exact hard limit. Eager reading ensures a decoder cannot accept
// a small first JSON value while leaving an oversized trailing body unread.
func LimitJSONBody(request *http.Request, resource string, maxBytes int64) error {
	if request == nil {
		return fmt.Errorf("request is required")
	}
	if maxBytes < 0 {
		return fmt.Errorf("maximum JSON body bytes must not be negative")
	}
	if request.Body == nil {
		return nil
	}
	if request.ContentLength > maxBytes {
		return tooLarge(resource, maxBytes, request.ContentLength, "bytes")
	}

	originalBody := request.Body
	payload, readErr := io.ReadAll(io.LimitReader(originalBody, maxBytes+1))
	closeErr := originalBody.Close()
	if readErr != nil {
		return fmt.Errorf("read %s: %w", resource, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", resource, closeErr)
	}
	if int64(len(payload)) > maxBytes {
		return tooLarge(resource, maxBytes, int64(len(payload)), "bytes")
	}
	request.Body = io.NopCloser(bytes.NewReader(payload))
	request.ContentLength = int64(len(payload))
	return nil
}

// Bytes validates a byte slice against maxBytes.
func Bytes(resource string, value []byte, maxBytes int64) error {
	return Length(resource, int64(len(value)), maxBytes, "bytes")
}

// String validates the UTF-8 byte length of a string against maxBytes.
func String(resource, value string, maxBytes int64) error {
	return Length(resource, int64(len(value)), maxBytes, "bytes")
}

// Map validates a map item count against maxItems.
func Map(resource string, actualItems, maxItems int) error {
	return Length(resource, int64(actualItems), int64(maxItems), "items")
}

// Slice validates a slice item count against maxItems.
func Slice(resource string, actualItems, maxItems int) error {
	return Length(resource, int64(actualItems), int64(maxItems), "items")
}

// Length validates a measured resource against a non-negative limit.
func Length(resource string, actual, limit int64, unit string) error {
	if limit < 0 {
		return fmt.Errorf("%s limit must not be negative", resource)
	}
	if actual <= limit {
		return nil
	}
	return tooLarge(resource, limit, actual, unit)
}

// CanonicalJSON marshals a value through encoding/json and validates the
// resulting compact, deterministic representation before callers persist,
// encrypt, or transmit it.
func CanonicalJSON(resource string, value any, maxBytes int64) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", resource, err)
	}
	if err := Bytes(resource, payload, maxBytes); err != nil {
		return nil, err
	}
	return payload, nil
}

func tooLarge(resource string, limit, actual int64, unit string) error {
	if resource == "" {
		resource = "resource"
	}
	return &TooLargeError{
		Resource: resource,
		Limit:    limit,
		Actual:   actual,
		Unit:     unit,
	}
}
