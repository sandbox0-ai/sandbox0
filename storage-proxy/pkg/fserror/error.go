package fserror

import (
	"errors"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type Code int32

const (
	OK Code = iota
	Canceled
	Unknown
	InvalidArgument
	DeadlineExceeded
	NotFound
	AlreadyExists
	PermissionDenied
	ResourceExhausted
	FailedPrecondition
	Aborted
	OutOfRange
	Unimplemented
	Internal
	Unavailable
	DataLoss
	Unauthenticated
)

type Error struct {
	code     Code
	message  string
	redirect *pb.PrimaryRedirect
}

func New(code Code, message string) error {
	return &Error{code: code, message: message}
}

func WithRedirect(err error, redirect *pb.PrimaryRedirect) error {
	if err == nil {
		return nil
	}
	var fsErr *Error
	if errors.As(err, &fsErr) {
		cp := *fsErr
		cp.redirect = redirect
		return &cp
	}
	return &Error{code: Internal, message: err.Error(), redirect: redirect}
}

func FromError(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	var fsErr *Error
	if errors.As(err, &fsErr) {
		return fsErr, true
	}
	return nil, false
}

func CodeOf(err error) Code {
	if err == nil {
		return OK
	}
	if fsErr, ok := FromError(err); ok {
		return fsErr.code
	}
	return Internal
}

func MessageOf(err error) string {
	if err == nil {
		return ""
	}
	if fsErr, ok := FromError(err); ok {
		return fsErr.message
	}
	return err.Error()
}

func RedirectOf(err error) *pb.PrimaryRedirect {
	if fsErr, ok := FromError(err); ok {
		return fsErr.redirect
	}
	return nil
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *Error) Code() Code {
	if e == nil {
		return OK
	}
	return e.code
}

func (e *Error) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *Error) Redirect() *pb.PrimaryRedirect {
	if e == nil {
		return nil
	}
	return e.redirect
}
