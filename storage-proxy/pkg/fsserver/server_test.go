package fsserver

import (
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
)

func TestMapErrnoToCode(t *testing.T) {
	tests := []struct {
		name  string
		errno syscall.Errno
		want  fserror.Code
	}{
		{
			name:  "already exists",
			errno: syscall.EEXIST,
			want:  fserror.AlreadyExists,
		},
		{
			name:  "not found",
			errno: syscall.ENOENT,
			want:  fserror.NotFound,
		},
		{
			name:  "permission denied",
			errno: syscall.EACCES,
			want:  fserror.PermissionDenied,
		},
		{
			name:  "operation not permitted",
			errno: syscall.EPERM,
			want:  fserror.PermissionDenied,
		},
		{
			name:  "no space",
			errno: syscall.ENOSPC,
			want:  fserror.ResourceExhausted,
		},
		{
			name:  "invalid argument",
			errno: syscall.EINVAL,
			want:  fserror.InvalidArgument,
		},
		{
			name:  "not a directory",
			errno: syscall.ENOTDIR,
			want:  fserror.InvalidArgument,
		},
		{
			name:  "unknown errno",
			errno: syscall.EIO,
			want:  fserror.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapErrnoToCode(tt.errno); got != tt.want {
				t.Errorf("mapErrnoToCode(%v) = %v, want %v", tt.errno, got, tt.want)
			}
		})
	}
}
