package volume

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type staticTokenProvider struct{}

func (staticTokenProvider) GetInternalToken() string { return "token" }

type captureFileSystemClient struct {
	pb.FileSystemClient
	lookupReq  *pb.LookupRequest
	accessReq  *pb.AccessRequest
	writeReq   *pb.WriteRequest
	flushReq   *pb.FlushRequest
	releaseReq *pb.ReleaseRequest
	lookupMD   metadata.MD
	flushes    int
	releases   int
	releaseCh  chan struct{}
}

func (c *captureFileSystemClient) Lookup(ctx context.Context, req *pb.LookupRequest, _ ...grpc.CallOption) (*pb.NodeResponse, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		c.lookupMD = md
	}
	c.lookupReq = req
	return &pb.NodeResponse{Inode: 2, Attr: &pb.GetAttrResponse{Mode: syscall.S_IFREG | 0o644}}, nil
}

func (c *captureFileSystemClient) Write(_ context.Context, req *pb.WriteRequest, _ ...grpc.CallOption) (*pb.WriteResponse, error) {
	c.writeReq = req
	return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
}

func (c *captureFileSystemClient) Access(_ context.Context, req *pb.AccessRequest, _ ...grpc.CallOption) (*pb.Empty, error) {
	c.accessReq = req
	return &pb.Empty{}, nil
}

func (c *captureFileSystemClient) Flush(_ context.Context, req *pb.FlushRequest, _ ...grpc.CallOption) (*pb.Empty, error) {
	c.flushReq = req
	c.flushes++
	return &pb.Empty{}, nil
}

func (c *captureFileSystemClient) Release(_ context.Context, req *pb.ReleaseRequest, _ ...grpc.CallOption) (*pb.Empty, error) {
	c.releaseReq = req
	c.releases++
	if c.releaseCh != nil {
		close(c.releaseCh)
	}
	return &pb.Empty{}, nil
}

func TestGrpcFSLookupForwardsCallerActor(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop())
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
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop())
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

func TestGrpcFSLookupUsesSessionCredentialWhenPresent(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "session-1", "secret-1", client, staticTokenProvider{}, 0, zap.NewNop())
	out := &fuse.EntryOut{}
	header := &fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}, Pid: 789}}

	if st := fs.Lookup(nil, header, "hello.txt", out); st != fuse.OK {
		t.Fatalf("Lookup() status = %v, want OK", st)
	}
	if client.lookupMD == nil {
		t.Fatal("Lookup() should carry outgoing metadata")
	}
	if got := client.lookupMD["x-volume-session-id"]; len(got) != 1 || got[0] != "session-1" {
		t.Fatalf("x-volume-session-id = %v, want [session-1]", got)
	}
	if got := client.lookupMD["x-volume-session-secret"]; len(got) != 1 || got[0] != "secret-1" {
		t.Fatalf("x-volume-session-secret = %v, want [secret-1]", got)
	}
}

func TestGrpcFSFlushCanDeferToRelease(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop(), withDeferredFlushToRelease(true))
	input := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 5}, Fh: 11}

	if st := fs.Flush(nil, input); st != fuse.OK {
		t.Fatalf("Flush() status = %v, want OK", st)
	}
	if client.flushes != 0 {
		t.Fatalf("Flush() called storage-proxy %d times, want 0", client.flushes)
	}
}

func TestGrpcFSFlushCallsStorageProxyByDefault(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop())
	input := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 5}, Fh: 11}

	if st := fs.Flush(nil, input); st != fuse.OK {
		t.Fatalf("Flush() status = %v, want OK", st)
	}
	if client.flushes != 1 {
		t.Fatalf("Flush() called storage-proxy %d times, want 1", client.flushes)
	}
	if client.flushReq == nil || client.flushReq.HandleId != 11 {
		t.Fatalf("Flush() request = %+v, want handle 11", client.flushReq)
	}
}

func TestGrpcFSSkipAccessReturnsWithoutStorageProxy(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop(), withSkipAccess(true))
	input := &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: 5}, Mask: 4}

	if st := fs.Access(nil, input); st != fuse.OK {
		t.Fatalf("Access() status = %v, want OK", st)
	}
	if client.accessReq != nil {
		t.Fatalf("Access() called storage-proxy with %+v, want no call", client.accessReq)
	}
}

func TestGrpcFSAccessCallsStorageProxyByDefault(t *testing.T) {
	client := &captureFileSystemClient{}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop())
	input := &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: 5}, Mask: 4}

	if st := fs.Access(nil, input); st != fuse.OK {
		t.Fatalf("Access() status = %v, want OK", st)
	}
	if client.accessReq == nil || client.accessReq.Inode != 5 || client.accessReq.Mask != 4 {
		t.Fatalf("Access() request = %+v, want inode 5 mask 4", client.accessReq)
	}
}

func TestGrpcFSAsyncReleaseCopiesRequestBeforeReturning(t *testing.T) {
	client := &captureFileSystemClient{releaseCh: make(chan struct{})}
	fs := newGrpcFS("vol-1", "", "", client, staticTokenProvider{}, 0, zap.NewNop(), withAsyncRelease(true))
	input := &fuse.ReleaseIn{
		InHeader: fuse.InHeader{
			NodeId: 5,
			Caller: fuse.Caller{
				Owner: fuse.Owner{Uid: 1001, Gid: 1002},
				Pid:   1003,
			},
		},
		Fh: 11,
	}

	fs.Release(nil, input)
	select {
	case <-client.releaseCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async release")
	}
	if client.releases != 1 {
		t.Fatalf("Release() calls = %d, want 1", client.releases)
	}
	if client.releaseReq == nil || client.releaseReq.Inode != 5 || client.releaseReq.HandleId != 11 {
		t.Fatalf("Release() request = %+v, want inode 5 handle 11", client.releaseReq)
	}
	if client.releaseReq.Actor == nil || client.releaseReq.Actor.Pid != 1003 || client.releaseReq.Actor.Uid != 1001 {
		t.Fatalf("Release() actor = %+v, want pid=1003 uid=1001", client.releaseReq.Actor)
	}
}
