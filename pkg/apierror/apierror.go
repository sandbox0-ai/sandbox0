// Package apierror defines transport-neutral error categories shared by HTTP
// handlers and runtime services.
package apierror

import (
	"errors"
	"fmt"
	"strings"
)

// Kind identifies the stable response category for an API failure.
type Kind string

const (
	KindNotFound  Kind = "not_found"
	KindConflict  Kind = "conflict"
	KindForbidden Kind = "forbidden"
)

// Error carries a transport-neutral category and optional resource identity.
type Error struct {
	Kind     Kind
	Resource string
	ID       string
	Cause    error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	identity := strings.TrimSpace(strings.Join([]string{e.Resource, e.ID}, " "))
	if identity == "" {
		identity = "resource"
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s %s: %v", identity, e.Kind, e.Cause)
	}
	return fmt.Sprintf("%s %s", identity, e.Kind)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewNotFound(resource, id string) error {
	return &Error{Kind: KindNotFound, Resource: resource, ID: id}
}

func NewConflict(resource, id string, cause error) error {
	return &Error{Kind: KindConflict, Resource: resource, ID: id, Cause: cause}
}

func NewForbidden(resource, id string, cause error) error {
	return &Error{Kind: KindForbidden, Resource: resource, ID: id, Cause: cause}
}

func IsNotFound(err error) bool  { return isKind(err, KindNotFound) }
func IsConflict(err error) bool  { return isKind(err, KindConflict) }
func IsForbidden(err error) bool { return isKind(err, KindForbidden) }

func isKind(err error, kind Kind) bool {
	var categorized *Error
	return errors.As(err, &categorized) && categorized.Kind == kind
}
