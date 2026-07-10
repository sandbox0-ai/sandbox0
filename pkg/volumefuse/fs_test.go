package volumefuse

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
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

type openFlagsForHandleTestSession struct {
	openFlagsTestSession
	handleID uint64
}

func (s openFlagsForHandleTestSession) OpenFlagsForHandle(handleID uint64) (uint32, bool) {
	if handleID != s.handleID {
		return 0, false
	}
	return fuse.FOPEN_NONSEEKABLE, true
}

func TestOpenUsesHandleSpecificSessionOpenFlags(t *testing.T) {
	session := openFlagsForHandleTestSession{
		openFlagsTestSession: openFlagsTestSession{flags: fuse.FOPEN_DIRECT_IO},
		handleID:             7,
	}
	fs := New("vol-1", time.Second, session)

	var out fuse.OpenOut
	st := fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}}, &out)
	if st != fuse.OK {
		t.Fatalf("Open() status = %v, want OK", st)
	}
	if out.OpenFlags != fuse.FOPEN_NONSEEKABLE {
		t.Fatalf("Open() flags = %#x, want NONSEEKABLE", out.OpenFlags)
	}
}
