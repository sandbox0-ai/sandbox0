package fserror

import (
	"errors"
	"syscall"

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
	cause    error
}

func New(code Code, message string) error {
	return &Error{code: code, message: message}
}

// NewErrno creates a filesystem error that retains its POSIX errno while also
// exposing the closest high-level error code to non-FUSE callers.
func NewErrno(errno syscall.Errno, message string) error {
	return &Error{code: codeForErrno(errno), message: message, cause: errno}
}

// ErrnoOf returns the POSIX errno carried by err, including wrapped errors.
func ErrnoOf(err error) (syscall.Errno, bool) {
	if err == nil {
		return 0, false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno, true
	}
	return 0, false
}

func codeForErrno(errno syscall.Errno) Code {
	switch errno {
	case syscall.ENOENT:
		return NotFound
	case syscall.EEXIST:
		return AlreadyExists
	case syscall.EACCES, syscall.EPERM:
		return PermissionDenied
	case syscall.ENOSPC, syscall.EDQUOT:
		return ResourceExhausted
	case syscall.EINVAL, syscall.ENOTDIR:
		return InvalidArgument
	case syscall.ENOSYS, syscall.EOPNOTSUPP:
		return Unimplemented
	case syscall.ENOTEMPTY, syscall.EISDIR:
		return FailedPrecondition
	default:
		return Internal
	}
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

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
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
