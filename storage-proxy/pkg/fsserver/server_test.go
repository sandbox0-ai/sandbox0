package fsserver

import (
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
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

func TestLinkRejectsUnsupportedBackendWithoutLegacyFallback(t *testing.T) {
	t.Parallel()

	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-legacy": {
				VolumeID: "vol-legacy",
				TeamID:   "team-a",
				Backend:  "legacy",
			},
		},
	}, nil, nil)

	_, err := server.Link(authContext("team-a", ""), &pb.LinkRequest{
		VolumeId:  "vol-legacy",
		Inode:     2,
		NewParent: 1,
		NewName:   "linked",
	})
	if fserror.CodeOf(err) != fserror.FailedPrecondition {
		t.Fatalf("Link() error code = %v, want FailedPrecondition (err=%v)", fserror.CodeOf(err), err)
	}
}
