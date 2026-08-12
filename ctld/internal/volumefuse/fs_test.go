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
	"golang.org/x/sys/unix"
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

type lseekIoctlTestSession struct {
	Session
	ioctlRequest *pb.IoctlRequest
}

func (s *lseekIoctlTestSession) GetAttr(context.Context, *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	return &pb.GetAttrResponse{Size: 10}, nil
}

func (s *lseekIoctlTestSession) Ioctl(_ context.Context, req *pb.IoctlRequest) (*pb.IoctlResponse, error) {
	s.ioctlRequest = req
	return &pb.IoctlResponse{DataOut: []byte{4, 3, 2, 1}}, nil
}

func TestLseekReportsDenseDataExtent(t *testing.T) {
	session := &lseekIoctlTestSession{}
	fs := New("vol-1", time.Second, session)

	for _, tt := range []struct {
		name    string
		offset  uint64
		whence  uint32
		want    uint64
		wantErr fuse.Status
	}{
		{name: "data", offset: 2, whence: uint32(unix.SEEK_DATA), want: 2},
		{name: "hole", offset: 2, whence: uint32(unix.SEEK_HOLE), want: 10},
		{name: "hole at eof", offset: 10, whence: uint32(unix.SEEK_HOLE), want: 10},
		{name: "data at eof", offset: 10, whence: uint32(unix.SEEK_DATA), wantErr: fuse.Status(syscall.ENXIO)},
		{name: "beyond eof", offset: 11, whence: uint32(unix.SEEK_HOLE), wantErr: fuse.Status(syscall.ENXIO)},
		{name: "invalid whence", offset: 0, whence: uint32(unix.SEEK_SET), wantErr: fuse.EINVAL},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out fuse.LseekOut
			status := fs.Lseek(nil, &fuse.LseekIn{
				InHeader: fuse.InHeader{NodeId: 42},
				Offset:   tt.offset,
				Whence:   tt.whence,
			}, &out)
			if status != tt.wantErr {
				t.Fatalf("Lseek() status = %v, want %v", status, tt.wantErr)
			}
			if status == fuse.OK && out.Offset != tt.want {
				t.Fatalf("Lseek() offset = %d, want %d", out.Offset, tt.want)
			}
		})
	}
}

func TestIoctlForwardsBuffers(t *testing.T) {
	session := &lseekIoctlTestSession{}
	fs := New("vol-1", time.Second, session)
	input := &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: 42},
		Cmd:      0x80086601,
		Arg:      99,
	}
	bufOut := make([]byte, 8)
	var out fuse.IoctlOut
	if status := fs.Ioctl(nil, input, []byte{1, 2, 3}, &out, bufOut); status != fuse.OK {
		t.Fatalf("Ioctl() status = %v, want OK", status)
	}
	if session.ioctlRequest == nil {
		t.Fatal("Ioctl() did not forward the request")
	}
	if session.ioctlRequest.VolumeId != "vol-1" || session.ioctlRequest.Inode != 42 || session.ioctlRequest.Cmd != input.Cmd || session.ioctlRequest.Arg != input.Arg {
		t.Fatalf("Ioctl() request = %+v", session.ioctlRequest)
	}
	if !bytes.Equal(session.ioctlRequest.DataIn, []byte{1, 2, 3}) || session.ioctlRequest.DataOutSize != uint32(len(bufOut)) {
		t.Fatalf("Ioctl() buffers = %+v", session.ioctlRequest)
	}
	if !bytes.Equal(bufOut[:4], []byte{4, 3, 2, 1}) {
		t.Fatalf("Ioctl() output = %v", bufOut)
	}
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

type fsyncDirTestSession struct {
	Session
	inode uint64
	err   error
}

func (s *fsyncDirTestSession) FsyncDir(_ context.Context, inode uint64) error {
	s.inode = inode
	return s.err
}

func TestFsyncDirUsesOptInSession(t *testing.T) {
	session := &fsyncDirTestSession{}
	fs := New("vol-1", time.Second, session)

	st := fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 42}})
	if st != fuse.OK {
		t.Fatalf("FsyncDir() status = %v, want OK", st)
	}
	if session.inode != 42 {
		t.Fatalf("FsyncDir() inode = %d, want 42", session.inode)
	}
}

func TestFsyncDirWithoutOptInReturnsNotImplemented(t *testing.T) {
	fs := New("vol-1", time.Second, &readIntoSession{})

	if st := fs.FsyncDir(nil, &fuse.FsyncIn{}); st != fuse.ENOSYS {
		t.Fatalf("FsyncDir() status = %v, want ENOSYS", st)
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
