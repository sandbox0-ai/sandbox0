package volume

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type staticTokenProvider struct{}

func (staticTokenProvider) GetInternalToken() string { return "token" }

type captureFileSystemClient struct {
	pb.FileSystemClient
	lookupReq *pb.LookupRequest
	writeReq  *pb.WriteRequest
}

func (c *captureFileSystemClient) Lookup(_ context.Context, req *pb.LookupRequest, _ ...grpc.CallOption) (*pb.NodeResponse, error) {
	c.lookupReq = req
	return &pb.NodeResponse{Inode: 2, Attr: &pb.GetAttrResponse{Mode: syscall.S_IFREG | 0o644}}, nil
}

func (c *captureFileSystemClient) Write(_ context.Context, req *pb.WriteRequest, _ ...grpc.CallOption) (*pb.WriteResponse, error) {
	c.writeReq = req
	return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
}

func TestGrpcFSLookupForwardsCallerActor(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", client, staticTokenProvider{}, 0, zap.NewNop())
	out := &fuse.EntryOut{}
	header := &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}, Pid: 789}}

	if st := fs.Lookup(nil, header, "hello.txt", out); st != fuse.OK {
		t.Fatalf("Lookup() status = %v, want OK", st)
	}
	if client.lookupReq == nil || client.lookupReq.Actor == nil {
		t.Fatalf("Lookup() actor was not forwarded")
	}
	if client.lookupReq.Actor.Pid != 789 || client.lookupReq.Actor.Uid != 123 {
		t.Fatalf("Lookup() actor = %+v, want pid=789 uid=123", client.lookupReq.Actor)
	}
	if len(client.lookupReq.Actor.Gids) != 1 || client.lookupReq.Actor.Gids[0] != 456 {
		t.Fatalf("Lookup() gids = %v, want [456]", client.lookupReq.Actor.Gids)
	}
}

func TestGrpcFSWriteForwardsCallerActor(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", client, staticTokenProvider{}, 0, zap.NewNop())
	input := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 5, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1001, Gid: 1002}, Pid: 1003}}, Offset: 7, Fh: 11}

	written, st := fs.Write(nil, input, []byte("hello"))
	if st != fuse.OK {
		t.Fatalf("Write() status = %v, want OK", st)
	}
	if written != 5 {
		t.Fatalf("Write() written = %d, want 5", written)
	}
	if client.writeReq == nil || client.writeReq.Actor == nil {
		t.Fatalf("Write() actor was not forwarded")
	}
	if client.writeReq.Actor.Pid != 1003 || client.writeReq.Actor.Uid != 1001 {
		t.Fatalf("Write() actor = %+v, want pid=1003 uid=1001", client.writeReq.Actor)
	}
	if len(client.writeReq.Actor.Gids) != 1 || client.writeReq.Actor.Gids[0] != 1002 {
		t.Fatalf("Write() gids = %v, want [1002]", client.writeReq.Actor.Gids)
	}
}
