package grpc

import (
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestMapErrnoToCode(t *testing.T) {
	tests := []struct {
		name  string
		errno syscall.Errno
		want  codes.Code
	}{
		{
			name:  "already exists",
			errno: syscall.EEXIST,
			want:  codes.AlreadyExists,
		},
		{
			name:  "not found",
			errno: syscall.ENOENT,
			want:  codes.NotFound,
		},
		{
			name:  "permission denied",
			errno: syscall.EACCES,
			want:  codes.PermissionDenied,
		},
		{
			name:  "operation not permitted",
			errno: syscall.EPERM,
			want:  codes.PermissionDenied,
		},
		{
			name:  "no space",
			errno: syscall.ENOSPC,
			want:  codes.ResourceExhausted,
		},
		{
			name:  "invalid argument",
			errno: syscall.EINVAL,
			want:  codes.InvalidArgument,
		},
		{
			name:  "not a directory",
			errno: syscall.ENOTDIR,
			want:  codes.InvalidArgument,
		},
		{
			name:  "unknown errno",
			errno: syscall.EIO,
			want:  codes.Internal,
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
