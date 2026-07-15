package volumefuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type readIntoSession struct {
	Session
	calledReadInto bool
	calledRead     bool
}

func (s *readIntoSession) ReadInto(_ context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	s.calledReadInto = true
	if req.Size != 4 {
		return 0, false, errors.New("unexpected read size")
	}
	return copy(dest, []byte("data")), true, nil
}

func (s *readIntoSession) Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.calledRead = true
	return &pb.ReadResponse{Data: []byte("slow")}, nil
}

func TestReadUsesReadIntoSession(t *testing.T) {
	session := &readIntoSession{}
	fs := New("vol-1", time.Second, session)

	result, st := fs.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 42},
		Size:     4,
	}, make([]byte, 8))
	if st != fuse.OK {
		t.Fatalf("Read() status = %v, want OK", st)
	}
	data, st := result.Bytes(make([]byte, result.Size()))
	if st != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v, want OK", st)
	}
	if !bytes.Equal(data, []byte("data")) {
		t.Fatalf("Read() data = %q, want data", data)
	}
	if !session.calledReadInto {
		t.Fatal("ReadInto was not called")
	}
	if session.calledRead {
		t.Fatal("Read fallback was called")
	}
}

type openFlagsTestSession struct {
	Session
	flags uint32
}

func (s openFlagsTestSession) OpenFlags() uint32 {
	return s.flags
}

func (s openFlagsTestSession) Open(context.Context, *pb.OpenRequest) (*pb.OpenResponse, error) {
	return &pb.OpenResponse{HandleId: 7}, nil
}

func TestOpenUsesSessionOpenFlags(t *testing.T) {
	session := openFlagsTestSession{flags: fuse.FOPEN_DIRECT_IO}
	fs := New("vol-1", time.Second, session)

	var out fuse.OpenOut
	st := fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 42},
	}, &out)
	if st != fuse.OK {
		t.Fatalf("Open() status = %v, want OK", st)
	}
	if out.Fh != 7 {
		t.Fatalf("Open() handle = %d, want 7", out.Fh)
	}
	if out.OpenFlags != fuse.FOPEN_DIRECT_IO {
		t.Fatalf("Open() flags = %#x, want DIRECT_IO", out.OpenFlags)
	}
}

func TestStatusToFusePreservesPOSIXErrno(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want fuse.Status
	}{
		{
			name: "structured not empty",
			err:  fserror.NewErrno(syscall.ENOTEMPTY, "directory not empty"),
			want: fuse.Status(syscall.ENOTEMPTY),
		},
		{
			name: "structured is directory",
			err:  fserror.NewErrno(syscall.EISDIR, "is a directory"),
			want: fuse.Status(syscall.EISDIR),
		},
		{
			name: "wrapped raw not directory",
			err:  fmt.Errorf("readdir: %w", syscall.ENOTDIR),
			want: fuse.Status(syscall.ENOTDIR),
		},
		{
			name: "generic failed precondition",
			err:  fserror.New(fserror.FailedPrecondition, "portal is not bound"),
			want: fuse.EIO,
		},
		{
			name: "internal error",
			err:  fserror.New(fserror.Internal, "storage failure"),
			want: fuse.EIO,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := statusToFuse(tt.err); got != tt.want {
				t.Fatalf("statusToFuse() = %v, want %v", got, tt.want)
			}
		})
	}
}
