package fserror

import (
	"errors"
	"syscall"
	"testing"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestNewErrnoPreservesPOSIXAndHighLevelCodes(t *testing.T) {
	tests := []struct {
		name  string
		errno syscall.Errno
		code  Code
	}{
		{name: "not found", errno: syscall.ENOENT, code: NotFound},
		{name: "already exists", errno: syscall.EEXIST, code: AlreadyExists},
		{name: "permission denied", errno: syscall.EPERM, code: PermissionDenied},
		{name: "no space", errno: syscall.ENOSPC, code: ResourceExhausted},
		{name: "not a directory", errno: syscall.ENOTDIR, code: InvalidArgument},
		{name: "directory not empty", errno: syscall.ENOTEMPTY, code: FailedPrecondition},
		{name: "is a directory", errno: syscall.EISDIR, code: FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewErrno(tt.errno, tt.name)
			if got := CodeOf(err); got != tt.code {
				t.Fatalf("CodeOf() = %v, want %v", got, tt.code)
			}
			if !errors.Is(err, tt.errno) {
				t.Fatalf("errors.Is(%v) = false", tt.errno)
			}
			if got, ok := ErrnoOf(err); !ok || got != tt.errno {
				t.Fatalf("ErrnoOf() = (%v, %v), want (%v, true)", got, ok, tt.errno)
			}
		})
	}
}

func TestWithRedirectPreservesErrno(t *testing.T) {
	redirect := &pb.PrimaryRedirect{VolumeId: "vol-1"}
	err := WithRedirect(NewErrno(syscall.ENOTEMPTY, "directory not empty"), redirect)

	if !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal("WithRedirect() discarded ENOTEMPTY")
	}
	if got := RedirectOf(err); got != redirect {
		t.Fatalf("RedirectOf() = %v, want %v", got, redirect)
	}
}

func TestErrnoOfWrappedRawErrno(t *testing.T) {
	err := errors.Join(errors.New("operation failed"), syscall.ENOTDIR)
	if got, ok := ErrnoOf(err); !ok || got != syscall.ENOTDIR {
		t.Fatalf("ErrnoOf() = (%v, %v), want (%v, true)", got, ok, syscall.ENOTDIR)
	}
}

func TestWrapPreservesTypedCauseAndCode(t *testing.T) {
	cause := errors.New("quota unavailable")
	err := Wrap(Unavailable, "storage quota unavailable", cause)
	if CodeOf(err) != Unavailable {
		t.Fatalf("CodeOf() = %v, want %v", CodeOf(err), Unavailable)
	}
	if !errors.Is(err, cause) {
		t.Fatal("Wrap() did not preserve typed cause")
	}
}
