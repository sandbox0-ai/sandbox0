package volume

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type staticTokenProvider struct{}

func (staticTokenProvider) GetInternalToken() string { return "token" }

type captureVolumeSession struct {
	volumeSession
	lookupReq *pb.LookupRequest
	writeReq  *pb.WriteRequest
}

func (c *captureVolumeSession) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	c.lookupReq = req
	return &pb.NodeResponse{Inode: 2, Attr: &pb.GetAttrResponse{Mode: syscall.S_IFREG | 0o644}}, nil
}

func (c *captureVolumeSession) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	c.writeReq = req
	return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
}

func TestVolumeFSLookupForwardsCallerActor(t *testing.T) {
	session := &captureVolumeSession{}
	fs := newVolumeFS("vol-1", 0)
	fs.setSession(session)
	out := &fuse.EntryOut{}
	header := &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}, Pid: 789}}

	if st := fs.Lookup(nil, header, "hello.txt", out); st != fuse.OK {
		t.Fatalf("Lookup() status = %v, want OK", st)
	}
	if session.lookupReq == nil || session.lookupReq.Actor == nil {
		t.Fatalf("Lookup() actor was not forwarded")
	}
	if session.lookupReq.Actor.Pid != 789 || session.lookupReq.Actor.Uid != 123 {
		t.Fatalf("Lookup() actor = %+v, want pid=789 uid=123", session.lookupReq.Actor)
	}
	if len(session.lookupReq.Actor.Gids) != 1 || session.lookupReq.Actor.Gids[0] != 456 {
		t.Fatalf("Lookup() gids = %v, want [456]", session.lookupReq.Actor.Gids)
	}
}

func TestVolumeFSWriteForwardsCallerActor(t *testing.T) {
	session := &captureVolumeSession{}
	fs := newVolumeFS("vol-1", 0)
	fs.setSession(session)
	input := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 5, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1001, Gid: 1002}, Pid: 1003}}, Offset: 7, Fh: 11}

	written, st := fs.Write(nil, input, []byte("hello"))
	if st != fuse.OK {
		t.Fatalf("Write() status = %v, want OK", st)
	}
	if written != 5 {
		t.Fatalf("Write() written = %d, want 5", written)
	}
	if session.writeReq == nil || session.writeReq.Actor == nil {
		t.Fatalf("Write() actor was not forwarded")
	}
	if session.writeReq.Actor.Pid != 1003 || session.writeReq.Actor.Uid != 1001 {
		t.Fatalf("Write() actor = %+v, want pid=1003 uid=1001", session.writeReq.Actor)
	}
	if len(session.writeReq.Actor.Gids) != 1 || session.writeReq.Actor.Gids[0] != 1002 {
		t.Fatalf("Write() gids = %v, want [1002]", session.writeReq.Actor.Gids)
	}
}

func TestVolumeFSMissingSessionFailsClosed(t *testing.T) {
	fs := newVolumeFS("vol-1", 0)
	out := &fuse.EntryOut{}
	header := &fuse.InHeader{NodeId: 1}

	if st := fs.Lookup(nil, header, "hello.txt", out); st != fuse.EIO {
		t.Fatalf("Lookup() status = %v, want EIO", st)
	}
}
