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
